#!/usr/bin/env python3
# 参考: https://www.ishikawa-lab.com/RasPi_ModeS.html

import socket
import pyModeS
import logging
import traceback

FRAGMENT_BUF_SIZE = 100

lat_ref = 35.0
lon_ref = 139.0

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


def reconstruct_data(icao, packet_type, data):
    global fragment_list

    fragment = next((fragment for fragment in fragment_list if fragment["icao"] == icao), None)

    if fragment is None:
        fragment_list.append({"icao": icao, packet_type: data})
        if len(fragment_list) == FRAGMENT_BUF_SIZE:
            fragment_list.pop(0)

    else:
        fragment[packet_type] = data

        if ("adsb" in fragment) and ("bsd50" in fragment) and ("bsd60" in fragment):
            logging.info(fragment)
            fragment_list.remove(fragment)


def process_message(message):
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
        logging.info("receive ADSB")
        code = pyModeS.typecode(message)

        if code is not None and ((code >= 5 and code <= 18) or (code >= 20 and code <= 22)):
            altitude = pyModeS.adsb.altitude(message)
            if altitude != 0:
                latitude = pyModeS.adsb.position_with_ref(message, lat_ref, lon_ref)[0]
                longitude = pyModeS.adsb.position_with_ref(message, lat_ref, lon_ref)[1]

                reconstruct_data(icao, "adsb", (altitude, latitude, longitude))
    elif df == 21:
        if pyModeS.bds.bds50.is50(message):
            logging.info("receive BDS50")

            trackangle = pyModeS.commb.trk50(message)
            roundspeed = pyModeS.commb.gs50(message)
            trueair = pyModeS.commb.tas50(message)

            reconstruct_data(icao, "bsd50", (trackangle, roundspeed, trueair))

        elif pyModeS.bds.bds60.is60(message):
            logging.info("receive BDS60")

            heading = pyModeS.commb.hdg60(message)
            indicatedair = pyModeS.commb.ias60(message)
            mach = pyModeS.commb.mach60(message)

            reconstruct_data(icao, "bsd60", (heading, indicatedair, mach))


def process_modes(host, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))

            for line in receive_lines(sock):
                process_message(line)
    except Exception:
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    import local_lib.logger

    host = "192.168.2.45"
    port = 30002

    local_lib.logger.init("ModeS sensing", level=logging.INFO)

    process_modes(host, port)
