#!/usr/bin/env python3
"""
Mode S のメッセージを解析し，上空の温度と風速を算出して出力します．

Usage:
  receiver.py [-c CONFIG]

Options:
  -c CONFIG     : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
"""
# 参考: https://www.ishikawa-lab.com/RasPi_ModeS.html

import socket
import pyModeS
import logging
import math
import traceback
import threading
import queue

FRAGMENT_BUF_SIZE = 100

fragment_list = []


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


def cacl_temperature(trueair, mach):
    k = 1.403  # 比熱比(空気)
    M = 28.966e-3  # 分子量(空気) [kg/mol]
    R = 8.314472  # 気体定数

    K = M / k / R

    return (trueair / mach) * (trueair / mach) * K - 273.15


def calc_magnetic_declination(latitude, longitude):
    # NOTE:
    # 地磁気値(2020.0年値）を求める
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


def calc_wind(latitude, longitude, trackangle, groundspeed, heading, trueair):
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


def calc_meteorological_data(
    altitude, latitude, longitude, trackangle, groundspeed, trueair, heading, indicatedair, mach
):
    altitude *= 0.3048  # 単位換算: feet →  mete
    groundspeed *= 0.514  # 単位換算: knot → m/s
    trueair *= 0.514

    temperature = cacl_temperature(trueair, mach)
    wind = calc_wind(latitude, longitude, trackangle, groundspeed, heading, trueair)

    if temperature < -200:
        logging.warning(
            (
                "温度が異常なので捨てます．"
                + "(temperature: {temperature:.1f}, altitude: {altitude}, trueair: {trueair}, mach: {mach})"
            ).format(
                temperature=temperature,
                altitude=altitude,
                trueair=trueair,
                mach=mach,
            )
        )
    return {
        "altitude": altitude,
        "latitude": latitude,
        "longitude": longitude,
        "temperature": temperature,
        "wind": wind,
    }


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

    distance = R * c

    return distance


def message_pairing(icao, packet_type, data, queue, area_info):
    global fragment_list

    if not all(value is not None for value in data):
        logging.warning(
            "データに欠損があるので捨てます．(type: {type}, data: {data})".format(type=packet_type, data=data)
        )
        return

    fragment = next((fragment for fragment in fragment_list if fragment["icao"] == icao), None)

    if fragment is None:
        fragment_list.append({"icao": icao, packet_type: data})
        if len(fragment_list) == FRAGMENT_BUF_SIZE:
            fragment_list.pop(0)

    else:
        fragment[packet_type] = data

        if ("adsb" in fragment) and ("bsd50" in fragment) and ("bsd60" in fragment):
            meteorological_data = calc_meteorological_data(
                *fragment["adsb"], *fragment["bsd50"], *fragment["bsd60"]
            )
            distance = calc_distance(
                area_info["lat"]["ref"], area_info["lon"]["ref"], fragment["adsb"][1], fragment["adsb"][2]
            )
            if distance < area_info["distance"]:
                queue.put(meteorological_data)
            else:
                logging.debug(
                    "範囲外なので無視されます (latitude: {latitude:.2f}, longitude: {longitude:.2f}, distance: {distance})".format(
                        latitude=fragment["adsb"][1], longitude=fragment["adsb"][2], distance=distance
                    )
                )

            fragment_list.remove(fragment)


def process_message(message, queue, area_info):
    logging.debug("receive: {message}".format(message=message))

    if len(message) < 2:
        return

    # NOTE: 先頭と末尾の文字を除去
    message = message[1:-1]

    if len(message) < 22:
        return

    icao = str(pyModeS.icao(message))
    df = pyModeS.df(message)
    if df == 17:
        logging.debug("receive ADSB")
        code = pyModeS.typecode(message)

        if code is not None and ((code >= 5 and code <= 18) or (code >= 20 and code <= 22)):
            altitude = pyModeS.adsb.altitude(message)
            if altitude != 0:
                latitude, longitude = pyModeS.adsb.position_with_ref(
                    message, area_info["lat"]["ref"], area_info["lon"]["ref"]
                )

                message_pairing(icao, "adsb", (altitude, latitude, longitude), queue, area_info)

    elif (df == 20) or (df == 21):
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
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))

            for line in receive_lines(sock):
                process_message(line, queue, area_info)
    except Exception:
        logging.error(traceback.format_exc())


def start(host, port, queue, area_info):
    thread = threading.Thread(target=watch_message, args=(host, port, queue, area_info))
    thread.start()

    return thread


if __name__ == "__main__":
    from docopt import docopt

    import local_lib.config
    import local_lib.logger

    args = docopt(__doc__)

    local_lib.logger.init("ModeS sensing", level=logging.INFO)

    config_file = args["-c"]
    config = local_lib.config.load(args["-c"])

    measurement_queue = queue.Queue()

    start(
        config["modes"]["decoder"]["host"],
        config["modes"]["decoder"]["port"],
        measurement_queue,
        config["filter"]["area"],
    )

    while True:
        logging.info(measurement_queue.get())
