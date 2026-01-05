"""VDL2 (dumpvdl2) から気象データを受信するモジュール

ZMQ SUB ソケットを使用して dumpvdl2 から JSON メッセージを受信し、
ACARS メッセージから気象データを抽出します。
"""

from __future__ import annotations

import logging
import queue
import threading
from typing import TYPE_CHECKING

import zmq

import vdl2.parser

if TYPE_CHECKING:
    import modes.database_postgresql

_should_terminate = threading.Event()


def _worker(
    host: str,
    port: int,
    data_queue: queue.Queue[modes.database_postgresql.MeasurementData],
    ref_lat: float,
    ref_lon: float,
) -> None:
    """ZMQ 受信ワーカー

    dumpvdl2 から JSON メッセージを受信し、ACARS 気象データを抽出して
    キューに追加します。

    Args:
        host: dumpvdl2 のホスト
        port: dumpvdl2 の ZMQ ポート
        data_queue: 抽出したデータを追加するキュー
        ref_lat: 基準点の緯度
        ref_lon: 基準点の経度
    """
    ctx = zmq.Context()
    socket = ctx.socket(zmq.SUB)
    socket.connect(f"tcp://{host}:{port}")
    socket.setsockopt(zmq.SUBSCRIBE, b"")
    socket.setsockopt(zmq.RCVTIMEO, 5000)  # 5秒タイムアウト

    logging.info("VDL2 receiver started: %s:%d", host, port)

    total_count = 0
    weather_count = 0

    while not _should_terminate.is_set():
        try:
            msg = socket.recv()
            total_count += 1

            acars_data = vdl2.parser.parse_acars_weather(msg)
            if acars_data:
                measurement = vdl2.parser.convert_to_measurement_data(acars_data, ref_lat, ref_lon)
                if measurement:
                    weather_count += 1
                    data_queue.put(measurement)
                    logging.debug(
                        "VDL2 weather data: %s alt=%d temp=%.1f",
                        measurement.callsign,
                        measurement.altitude,
                        measurement.temperature,
                    )

            # 進捗ログ（1000メッセージごと）
            if total_count % 1000 == 0:
                logging.info(
                    "VDL2 received %d messages, %d weather data",
                    total_count,
                    weather_count,
                )

        except zmq.Again:
            # タイムアウト、ループを継続
            continue
        except Exception:
            logging.exception("VDL2 receive error")

    socket.close()
    ctx.term()
    logging.info("VDL2 receiver stopped: %d messages, %d weather data", total_count, weather_count)


def start(
    host: str,
    port: int,
    data_queue: queue.Queue[modes.database_postgresql.MeasurementData],
    ref_lat: float,
    ref_lon: float,
) -> threading.Thread:
    """VDL2 受信ワーカーを開始する

    Args:
        host: dumpvdl2 のホスト
        port: dumpvdl2 の ZMQ ポート
        data_queue: 抽出したデータを追加するキュー
        ref_lat: 基準点の緯度
        ref_lon: 基準点の経度

    Returns:
        開始したワーカースレッド
    """
    _should_terminate.clear()
    thread = threading.Thread(
        target=_worker,
        args=(host, port, data_queue, ref_lat, ref_lon),
        daemon=True,
    )
    thread.start()
    return thread


def term() -> None:
    """受信を終了する"""
    _should_terminate.set()
