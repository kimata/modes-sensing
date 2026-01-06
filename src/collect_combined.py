#!/usr/bin/env python3
"""Mode-S と VDL2 の気象データを統合して PostgreSQL に保存します

両方のデータソースからリアルタイムでデータを受信し、
VDL2 の高度補完を ADS-B データで行います。

Usage:
  collect_combined.py [-c CONFIG] [-n COUNT] [-D]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
  -n COUNT          : n 回計測データを受信したら終了します。0 は制限なし。 [default: 0]
  -D                : デバッグモードで動作します．
"""

from __future__ import annotations

import logging
import multiprocessing
import pathlib
import queue
import signal
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import FrameType

import my_lib.footprint

import amdar.database.postgresql as database_postgresql
import amdar.sources.modes.receiver as modes_receiver
import amdar.sources.outlier
import amdar.sources.vdl2.receiver as vdl2_receiver
from amdar.config import Config
from amdar.database.postgresql import DBConfig, MeasurementData
from amdar.sources.aggregator import IntegratedBuffer

_SCHEMA_CONFIG = "config.schema"


def _sig_handler(num: int, _: FrameType | None) -> None:
    logging.warning("receive signal %d", num)

    if num in (signal.SIGTERM, signal.SIGINT):
        database_postgresql.store_term()
        modes_receiver.term()
        vdl2_receiver.term()


def _vdl2_to_combined_queue(
    vdl2_queue: queue.Queue[MeasurementData],
    combined_queue: multiprocessing.Queue[MeasurementData],
    stop_event: threading.Event,
) -> None:
    """VDL2 キューからデータを読み取り、統合キューに転送する

    Args:
        vdl2_queue: VDL2 レシーバーからのキュー
        combined_queue: 統合キュー（DB 保存用）
        stop_event: 停止イベント
    """
    while not stop_event.is_set():
        try:
            data = vdl2_queue.get(timeout=1.0)
            combined_queue.put(data)
            logging.debug("VDL2 data forwarded to combined queue: %s", data.callsign)
        except queue.Empty:
            continue


def execute(
    config: Config,
    liveness_file: pathlib.Path,
    count: int = 0,
) -> None:
    signal.signal(signal.SIGTERM, _sig_handler)

    # 統合バッファ（Mode-S → VDL2 の高度補完用）
    shared_buffer = IntegratedBuffer(window_seconds=60.0)

    # 統合キュー（両ソースのデータを DB に保存）
    measurement_queue: multiprocessing.Queue[MeasurementData] = multiprocessing.Queue()

    # VDL2 用のローカルキュー
    vdl2_queue: queue.Queue[MeasurementData] = queue.Queue()

    conn = database_postgresql.open(
        config.database.host,
        config.database.port,
        config.database.name,
        config.database.user,
        config.database.password,
    )

    # 履歴データを取得してreceiver.pyの外れ値検出機能に初期データを提供
    try:
        logging.info("データベースから履歴データを取得中...")

        # 外れ値検出に必要な最新の履歴データを取得（高度と温度のペア）
        historical_records = database_postgresql.fetch_latest(
            conn,
            amdar.sources.outlier.DEFAULT_HISTORY_SIZE,
            distance=config.filter.area.distance,
            columns=["altitude", "temperature"],
        )

        if historical_records:
            # (altitude, temperature) のタプルリストに変換
            historical_data = [(record["altitude"], record["temperature"]) for record in historical_records]

            # receiver.pyの履歴データを初期化
            modes_receiver.init(historical_data)
            logging.info("履歴データを初期化しました: %d件", len(historical_data))
        else:
            logging.warning("履歴データが見つかりませんでした")

    except Exception as e:
        logging.warning("履歴データの取得に失敗しました: %s", e)
        # エラーが発生しても処理を継続

    # Mode-S レシーバーを開始（共有バッファにフィード）
    logging.info("Mode-S レシーバーを開始します...")
    modes_receiver.start(config, measurement_queue, buffer=shared_buffer)

    # VDL2 設定の取得
    if config.decoder.vdl2 is not None:
        vdl2_host = config.decoder.vdl2.host
        vdl2_port = config.decoder.vdl2.port
        vdl2_liveness = config.liveness.file.receiver.vdl2
    else:
        logging.warning("VDL2 設定がありません。VDL2 レシーバーはスキップされます。")
        vdl2_host = None
        vdl2_port = None
        vdl2_liveness = None

    # 基準点
    ref_lat = config.filter.area.lat.ref
    ref_lon = config.filter.area.lon.ref

    # VDL2 レシーバーを開始（設定がある場合のみ）
    stop_event = threading.Event()
    if vdl2_host is not None and vdl2_port is not None:
        logging.info("VDL2 レシーバーを開始します (host=%s, port=%d)...", vdl2_host, vdl2_port)
        vdl2_receiver.start(
            vdl2_host,
            vdl2_port,
            vdl2_queue,
            ref_lat,
            ref_lon,
            buffer=shared_buffer,
            liveness_file=vdl2_liveness,
        )

        # VDL2 キュー転送スレッドを開始
        transfer_thread = threading.Thread(
            target=_vdl2_to_combined_queue,
            args=(vdl2_queue, measurement_queue, stop_event),
            daemon=True,
        )
        transfer_thread.start()

    db_config = DBConfig(
        host=config.database.host,
        port=config.database.port,
        name=config.database.name,
        user=config.database.user,
        password=config.database.password,
    )

    try:
        # 定期的にバッファ統計をログ出力
        def _log_buffer_stats() -> None:
            while not stop_event.is_set():
                time.sleep(60)
                stats = shared_buffer.get_stats()
                logging.info(
                    "Buffer stats: aircraft=%d, entries=%d, callsign_mappings=%d",
                    stats["aircraft_count"],
                    stats["total_entries"],
                    stats["callsign_mappings"],
                )

        stats_thread = threading.Thread(target=_log_buffer_stats, daemon=True)
        stats_thread.start()

        # DB 保存ループを開始
        database_postgresql.store_queue(
            conn, measurement_queue, liveness_file, db_config, config.slack, count
        )
    except Exception:
        logging.exception("Failed to store data")
    finally:
        stop_event.set()

    modes_receiver.term()
    vdl2_receiver.term()


######################################################################
if __name__ == "__main__":
    import docopt
    import my_lib.config
    import my_lib.logger

    from amdar.config import load_from_dict

    assert __doc__ is not None  # noqa: S101
    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    count = int(args["-n"])
    debug_mode = args["-D"]

    my_lib.logger.init("modes-combined", level=logging.DEBUG if debug_mode else logging.INFO)

    config_dict = my_lib.config.load(config_file, pathlib.Path(_SCHEMA_CONFIG))
    config = load_from_dict(config_dict, pathlib.Path.cwd())

    execute(config, config.liveness.file.collector, count)
