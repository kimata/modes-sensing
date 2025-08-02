#!/usr/bin/env python3
"""
ModeS のメッセージを解析し，上空の温度と風速を算出して出力します．

Usage:
  receiver.py [-c CONFIG] [-d]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
  -d                : デバッグモードで動作します．
"""
# 参考: https://www.ishikawa-lab.com/RasPi_ModeS.html

import logging
import math
import queue
import socket
import threading
from collections import deque

import numpy as np
import pyModeS
from sklearn.ensemble import IsolationForest

FRAGMENT_BUF_SIZE = 100

fragment_list = []

is_running = False

# Isolation Forest用のデータ蓄積
meteorological_history = deque(maxlen=500)  # 最大500件のデータを保持
OUTLIER_DETECTION_MIN_SAMPLES = 100  # 外れ値検出を開始する最小サンプル数


def receive_lines(sock):
    buffer = b""

    while True:
        data = sock.recv(1024)

        if data is None:
            return

        buffer += data
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            yield line.decode()


def calc_temperature(trueair, mach):
    k = 1.403  # 比熱比(空気)
    M = 28.966e-3  # 分子量(空気) [kg/mol]
    R = 8.314472  # 気体定数

    K = M / k / R

    return (trueair / mach) * (trueair / mach) * K - 273.15


def calc_magnetic_declination(latitude, longitude):
    # NOTE:
    # 地磁気値(2020.0年値)を求める
    # https://vldb.gsi.go.jp/sokuchi/geomag/menu_04/
    delta_latitude = latitude - 37
    delta_longitude = longitude - 138

    return (
        (8 + 15.822 / 60)
        + (18.462 / 60) * delta_latitude
        - (7.726 / 60) * delta_longitude
        + (0.007 / 60) * delta_latitude * delta_latitude
        + (0.007 / 60) * delta_latitude * delta_longitude
        - (0.655 / 60) * delta_longitude * delta_longitude
    )


def calc_wind(latitude, longitude, trackangle, groundspeed, heading, trueair):  # noqa: PLR0913
    magnetic_declination = calc_magnetic_declination(latitude, longitude)

    ground_dir = math.pi / 2 - math.radians(trackangle)
    ground_x = groundspeed * math.cos(ground_dir)
    ground_y = groundspeed * math.sin(ground_dir)
    air_dir = math.pi / 2 - math.radians(heading) + math.radians(magnetic_declination)
    air_x = trueair * math.cos(air_dir)
    air_y = trueair * math.sin(air_dir)

    wind_x = ground_x - air_x
    wind_y = ground_y - air_y

    return {
        "x": wind_x,
        "y": wind_y,
        # NOTE: 北を 0 として，風が来る方の角度
        "angle": math.degrees(
            (math.pi / 2 - math.atan2(wind_y, wind_x) + 2 * math.pi + math.pi) % (2 * math.pi)
        ),
        "speed": math.sqrt(wind_x * wind_x + wind_y * wind_y),
    }


def calc_meteorological_data(  # noqa: PLR0913
    callsign,
    altitude,
    latitude,
    longitude,
    trackangle,
    groundspeed,
    trueair,
    heading,
    indicatedair,  # noqa: ARG001
    mach,
):
    altitude *= 0.3048  # 単位換算: feet →  mete
    groundspeed *= 0.514  # 単位換算: knot → m/s
    trueair *= 0.514

    temperature = calc_temperature(trueair, mach)
    wind = calc_wind(latitude, longitude, trackangle, groundspeed, heading, trueair)

    if temperature < -100:
        logging.warning(
            "温度が異常なので捨てます．(callsign: %s, temperature: %.1f, "
            "altitude: %s, trueair: %s, mach: %s)",
            callsign,
            temperature,
            altitude,
            trueair,
            mach,
        )
    return {
        "callsign": callsign,
        "altitude": altitude,
        "latitude": latitude,
        "longitude": longitude,
        "temperature": temperature,
        "wind": wind,
    }


def is_outlier_data(temperature, altitude):
    """
    Isolation Forestを使用してaltitudeとtemperatureのペアが外れ値かどうかを判定

    Args:
        temperature (float): 気温
        altitude (float): 高度

    Returns:
        bool: 外れ値の場合True、正常値の場合False

    """
    global meteorological_history

    # データが十分蓄積されていない場合は外れ値として扱わない
    if len(meteorological_history) < OUTLIER_DETECTION_MIN_SAMPLES:
        return False

    try:
        # 履歴データから特徴量を抽出（altitude, temperature）
        features = np.array(
            [
                [data["altitude"], data["temperature"]]
                for data in meteorological_history
                if data["altitude"] is not None and data["temperature"] is not None
            ]
        )

        # Isolation Forestモデルを構築
        # contamination=0.003は約3σに相当（99.7%の信頼区間外を外れ値とする）
        isolation_forest = IsolationForest(contamination=0.003, random_state=42)
        isolation_forest.fit(features)

        # 新しいデータポイントを検査
        new_data = np.array([[altitude, temperature]])
        prediction = isolation_forest.predict(new_data)

        # -1が外れ値、1が正常値
        return prediction[0] == -1

    except Exception as e:
        logging.warning("外れ値検出でエラーが発生しました: %s", e)
        return False


def calc_distance(lat1, lon1, lat2, lon2):
    R = 6371.0

    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # NOTE: ハバースインの公式
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return R * c


def round_floats(obj, ndigits=1):
    if isinstance(obj, float):
        return round(obj, ndigits)
    elif isinstance(obj, dict):
        return {k: round_floats(v, ndigits) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [round_floats(elem, ndigits) for elem in obj]
    elif isinstance(obj, tuple):
        return tuple(round_floats(elem, ndigits) for elem in obj)
    else:
        return obj


def message_pairing(icao, packet_type, data, queue, area_info):
    global fragment_list, meteorological_history

    if not all(value is not None for value in data):
        logging.warning("データに欠損があるので捨てます．(type: %s, data: %s)", packet_type, data)
        return

    fragment = next((fragment for fragment in fragment_list if fragment["icao"] == icao), None)

    if fragment is None:
        fragment_list.append({"icao": icao, packet_type: data})
        if len(fragment_list) == FRAGMENT_BUF_SIZE:
            fragment_list.pop(0)

    else:
        fragment[packet_type] = data

        if all(packet_type in fragment for packet_type in ["adsb_pos", "adsb_sign", "bsd50", "bsd60"]):
            meteorological_data = calc_meteorological_data(
                *fragment["adsb_sign"],
                *fragment["adsb_pos"],
                *fragment["bsd50"],
                *fragment["bsd60"],
            )
            distance = calc_distance(
                area_info["lat"]["ref"],
                area_info["lon"]["ref"],
                fragment["adsb_pos"][1],
                fragment["adsb_pos"][2],
            )
            if distance < area_info["distance"]:
                # 温度が異常値でない場合のみ外れ値検出を実行
                if meteorological_data["temperature"] >= -100:
                    # 外れ値検出
                    is_outlier = is_outlier_data(
                        meteorological_data["temperature"], meteorological_data["altitude"]
                    )

                    if not is_outlier:
                        # 正常値の場合、queueに送信し履歴に追加
                        logging.info(round_floats(meteorological_data))

                        queue.put(meteorological_data)

                        meteorological_history.append(
                            {
                                "altitude": meteorological_data["altitude"],
                                "temperature": meteorological_data["temperature"],
                            }
                        )
                    else:
                        logging.warning(
                            "外れ値として除外されました (callsign: %s, altitude: %.1fm, temperature: %.1f°C)",
                            fragment["adsb_sign"][0],
                            meteorological_data["altitude"],
                            meteorological_data["temperature"],
                        )
                        # 外れ値でも履歴には追加しない（統計モデルを汚染しないため）
                else:
                    # 温度異常値は外れ値検出の対象外（従来通りの処理）
                    logging.debug("温度異常値のため外れ値検出をスキップ")
            else:
                logging.info(
                    "範囲外なので無視されます (callsign: %s, latitude: %.2f, longitude: %.2f, distance: %s)",
                    fragment["adsb_sign"][0],
                    fragment["adsb_pos"][1],
                    fragment["adsb_pos"][2],
                    distance,
                )

            fragment_list.remove(fragment)


def process_message(message, queue, area_info):  # noqa: C901
    logging.debug("receive: %s", message)

    if len(message) < 2:
        return

    # NOTE: 先頭と末尾の文字を除去
    message = message[1:-1]

    if len(message) < 22:
        return

    icao = str(pyModeS.icao(message))
    dformat = pyModeS.df(message)
    if dformat == 17:
        logging.debug("receive ADSB")
        code = pyModeS.typecode(message)

        if code is not None:
            if (5 <= code <= 18) or (20 <= code <= 22):
                altitude = pyModeS.adsb.altitude(message)
                if altitude != 0:
                    latitude, longitude = pyModeS.adsb.position_with_ref(
                        message, area_info["lat"]["ref"], area_info["lon"]["ref"]
                    )

                    message_pairing(
                        icao,
                        "adsb_pos",
                        (altitude, latitude, longitude),
                        queue,
                        area_info,
                    )
            elif 1 <= code <= 4:
                callsign = pyModeS.adsb.callsign(message).rstrip("_")
                message_pairing(icao, "adsb_sign", (callsign,), queue, area_info)

    elif dformat in (20, 21):
        if pyModeS.bds.bds50.is50(message):
            logging.debug("receive BDS50")

            trackangle = pyModeS.commb.trk50(message)
            groundspeed = pyModeS.commb.gs50(message)
            trueair = pyModeS.commb.tas50(message)

            message_pairing(icao, "bsd50", (trackangle, groundspeed, trueair), queue, area_info)

        elif pyModeS.bds.bds60.is60(message):
            logging.debug("receive BDS60")

            heading = pyModeS.commb.hdg60(message)
            indicatedair = pyModeS.commb.ias60(message)
            mach = pyModeS.commb.mach60(message)

            message_pairing(icao, "bsd60", (heading, indicatedair, mach), queue, area_info)


def watch_message(host, port, queue, area_info):
    global is_running  # noqa: PLW0603
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            is_running = True

            for line in receive_lines(sock):
                try:
                    process_message(line, queue, area_info)
                except Exception:  # noqa: PERF203
                    logging.exception("Failed to process message")
    except Exception:
        logging.exception("メッセージ受信でエラーが発生しました．")
        is_running = False


def start(host, port, queue, area_info):
    thread = threading.Thread(target=watch_message, args=(host, port, queue, area_info))
    thread.start()

    return thread


if __name__ == "__main__":
    import docopt
    import my_lib.config
    import my_lib.logger

    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    debug_mode = args["-d"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = my_lib.config.load(config_file)

    measurement_queue = queue.Queue()

    start(
        config["modes"]["decoder"]["host"],
        config["modes"]["decoder"]["port"],
        measurement_queue,
        config["filter"]["area"],
    )

    while True:
        logging.info(measurement_queue.get())

        if not is_running:
            break
