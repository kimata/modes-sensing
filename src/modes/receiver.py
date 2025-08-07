#!/usr/bin/env python3
"""
ModeS のメッセージを解析し，上空の温度と風速を算出して出力します．

Usage:
  receiver.py [-c CONFIG] [-D]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
  -D                : デバッグモードで動作します．
"""
# 参考: https://www.ishikawa-lab.com/RasPi_ModeS.html

import collections
import logging
import math
import queue
import socket
import threading

import numpy as np
import pyModeS
import sklearn.ensemble
import sklearn.linear_model

FRAGMENT_BUF_SIZE = 100

fragment_list = []

should_terminate = threading.Event()

HISTRY_SAMPLES = 10000
meteorological_history = collections.deque(maxlen=HISTRY_SAMPLES)
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


def is_physically_reasonable(altitude, temperature, regression_model, tolerance_factor=4):
    """
    高度-温度の物理的相関が妥当かどうかを判定

    Args:
        altitude (float): 高度
        temperature (float): 気温
        regression_model: 学習済み線形回帰モデル
        tolerance_factor (float): 許容範囲の倍率

    Returns:
        bool: 物理的に妥当な場合True

    """
    try:
        # 予測温度を計算
        predicted_temp = regression_model.predict([[altitude]])[0]

        # 高度-温度の一般的な関係：高度が1000m上がると約6.5°C下がる
        # 標準大気での温度減率を考慮した許容範囲を設定
        standard_lapse_rate = 0.0065  # °C/m
        altitude_diff_threshold = 200  # m（許容する高度差）
        temp_tolerance = standard_lapse_rate * altitude_diff_threshold * tolerance_factor

        # 予測値との差が許容範囲内かチェック
        residual = abs(temperature - predicted_temp)

        judge = residual <= temp_tolerance

        if judge:
            logging.info(
                "物理的に妥当な高度-温度相関のため正常値として扱います "
                "(altitude: %.1fm, temperature: %.1f°C, predicted_temp=%.1f°C, residual=%.1f°C)",
                altitude,
                temperature,
                predicted_temp,
                residual,
            )

        return judge

    except Exception:
        return True  # エラー時は保守的に妥当とみなす


def detect_outlier_by_altitude_neighbors(  # noqa: PLR0913
    altitude, temperature, altitudes, temperatures, callsign=None, n_neighbors=400, sigma_threshold=4
):
    """
    高度近傍ベースの異常検知を実行

    高度が近いデータポイントの局所的な分布を使用して異常値を検出します。
    低高度では温度のばらつきが大きいという特性に対応できます。

    Args:
        altitude (float): 検査対象の高度
        temperature (float): 検査対象の気温
        altitudes (np.array): 履歴データの高度配列
        temperatures (np.array): 履歴データの温度配列
        callsign (str, optional): 航空機のコールサイン（ログ用）
        n_neighbors (int): 使用する近傍データ数（デフォルト: 200）
        sigma_threshold (float): 異常値判定のシグマ閾値（デフォルト: 2.5）

    Returns:
        bool: 外れ値の場合True、正常値の場合False

    """
    # 高度差を計算
    altitude_diffs = np.abs(altitudes.flatten() - altitude)

    # 最も近い高度のインデックスを取得（最大n_neighbors個）
    n_actual = min(n_neighbors, len(altitude_diffs))
    nearest_indices = np.argpartition(altitude_diffs, n_actual - 1)[:n_actual]

    # 近傍データの温度を取得
    neighbor_temps = temperatures.flatten()[nearest_indices]
    neighbor_alts = altitudes.flatten()[nearest_indices]

    # 近傍データの平均高度と温度統計を計算
    mean_neighbor_alt = np.mean(neighbor_alts)
    mean_temp = np.mean(neighbor_temps)
    std_temp = np.std(neighbor_temps)

    # 温度の偏差を計算
    temp_deviation = abs(temperature - mean_temp)
    z_score = temp_deviation / std_temp if std_temp > 0 else 0

    # 異常値判定
    is_outlier = z_score > sigma_threshold

    # 判定結果をログ出力
    if is_outlier:
        logging.warning(
            "%s: callsign=%s, altitude=%.1fm, temperature=%.1f°C, "
            "neighbor_mean_alt=%.1fm, neighbor_mean_temp=%.1f°C, neighbor_std=%.1f°C, "
            "deviation=%.1f°C, z_score=%.2f (threshold=%.1f)",
            "外れ値検出（高度近傍）",
            callsign or "Unknown",
            altitude,
            temperature,
            mean_neighbor_alt,
            mean_temp,
            std_temp,
            temp_deviation,
            z_score,
            sigma_threshold,
        )
    else:
        logging.info(
            "%s: callsign=%s, altitude=%.1fm, temperature=%.1f°C, "
            "neighbor_mean_alt=%.1fm, neighbor_mean_temp=%.1f°C, neighbor_std=%.1f°C, "
            "deviation=%.1f°C, z_score=%.2f (threshold=%.1f)",
            "正常値判定（高度近傍）",
            callsign or "Unknown",
            altitude,
            temperature,
            mean_neighbor_alt,
            mean_temp,
            std_temp,
            temp_deviation,
            z_score,
            sigma_threshold,
        )

    return is_outlier


def is_outlier_data(temperature, altitude, callsign=None):
    """
    高度-温度相関を考慮してaltitudeとtemperatureのペアが外れ値かどうかを判定

    二段階アプローチ：
    1. 物理的相関チェック（高度が低い→温度が高い関係を保護）
    2. 残差ベースの異常検知

    Args:
        temperature (float): 気温
        altitude (float): 高度
        callsign (str, optional): 航空機のコールサイン（ログ用）

    Returns:
        bool: 外れ値の場合True、正常値の場合False

    """
    global meteorological_history

    # データが十分蓄積されていない場合は外れ値として扱わない
    if len(meteorological_history) < OUTLIER_DETECTION_MIN_SAMPLES:
        return False

    try:
        # 履歴データから特徴量を抽出
        valid_data = [
            data
            for data in meteorological_history
            if data["altitude"] is not None and data["temperature"] is not None
        ]

        if len(valid_data) < OUTLIER_DETECTION_MIN_SAMPLES:
            return False

        altitudes = np.array([[data["altitude"]] for data in valid_data])
        temperatures = np.array([data["temperature"] for data in valid_data])

        # 第一段階：線形回帰で高度-温度関係を学習
        regression_model = sklearn.linear_model.LinearRegression()
        regression_model.fit(altitudes, temperatures)

        # 物理的相関チェック（より寛容に）
        if is_physically_reasonable(altitude, temperature, regression_model, tolerance_factor=2.5):
            return False  # 物理的に妥当なので外れ値ではない

        # 第二段階：高度近傍ベースの異常検知（低高度でのばらつき対応）
        return detect_outlier_by_altitude_neighbors(altitude, temperature, altitudes, temperatures, callsign)

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
            distance = calc_distance(
                area_info["lat"]["ref"],
                area_info["lon"]["ref"],
                fragment["adsb_pos"][1],
                fragment["adsb_pos"][2],
            )
            meteorological_data = calc_meteorological_data(
                *fragment["adsb_sign"],
                *fragment["adsb_pos"],
                *fragment["bsd50"],
                *fragment["bsd60"],
            )
            # distanceをmeteorological_dataに追加
            meteorological_data["distance"] = distance

            # 温度が異常値でない場合のみ外れ値検出を実行
            if meteorological_data["temperature"] >= -100:
                # 外れ値検出
                is_outlier = is_outlier_data(
                    meteorological_data["temperature"],
                    meteorological_data["altitude"],
                    meteorological_data["callsign"],
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
                # 温度異常値は外れ値検出の対象外（従来通りの処理）
                logging.debug("温度異常値のため外れ値検出をスキップ")

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


def worker(host, port, queue, area_info):
    logging.info("Start receive worker")

    should_terminate.clear()

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            for line in receive_lines(sock):
                if should_terminate.is_set():
                    break

                try:
                    process_message(line, queue, area_info)
                except Exception:
                    logging.exception("Failed to process message")
    except Exception:
        logging.exception("メッセージ受信でエラーが発生しました．")

    logging.warning("Stop receive worker")


def init(data):
    meteorological_history.extend(data)


def start(host, port, queue, area_info):
    thread = threading.Thread(target=worker, args=(host, port, queue, area_info))
    thread.start()

    return thread


def term():
    should_terminate.set()


if __name__ == "__main__":
    import docopt
    import my_lib.config
    import my_lib.logger

    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    debug_mode = args["-D"]

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

        if should_terminate.is_set():
            break
