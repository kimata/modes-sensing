#!/usr/bin/env python3
"""
ModeS のメッセージを PostgreSQL に保存します

Usage:
  collect.py [-c CONFIG] [-n COUNT] [-D]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
  -n COUNT          : n 回計測データを受信したら終了します。0 は制限なし。 [default: 0]
  -D                : デバッグモードで動作します．
"""

from __future__ import annotations

import logging
import multiprocessing
import pathlib
import signal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import FrameType

import my_lib.footprint

import modes.database_postgresql
import modes.receiver
from modes.config import Config
from modes.database_postgresql import DBConfig, MeasurementData

SCHEMA_CONFIG = "config.schema"


def sig_handler(num: int, _: FrameType | None) -> None:
    logging.warning("receive signal %d", num)

    if num in (signal.SIGTERM, signal.SIGINT):
        modes.database_postgresql.store_term()
        modes.receiver.term()


def execute(
    config: Config,
    liveness_file: pathlib.Path,
    count: int = 0,
) -> None:
    signal.signal(signal.SIGTERM, sig_handler)

    measurement_queue: multiprocessing.Queue[MeasurementData] = multiprocessing.Queue()

    conn = modes.database_postgresql.open(
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
        historical_records = modes.database_postgresql.fetch_latest(
            conn,
            modes.receiver.HISTRY_SAMPLES,
            distance=config.filter.area.distance,
            columns=["altitude", "temperature"],
        )

        if historical_records:
            # receiver.pyの履歴データ形式に変換
            historical_data = [
                modes.receiver.HistoryData(
                    altitude=record["altitude"],
                    temperature=record["temperature"],
                )
                for record in historical_records
            ]

            # receiver.pyの履歴データを初期化
            modes.receiver.init(historical_data)
            logging.info("履歴データを初期化しました: %d件", len(historical_data))
        else:
            logging.warning("履歴データが見つかりませんでした")

    except Exception as e:
        logging.warning("履歴データの取得に失敗しました: %s", e)
        # エラーが発生しても処理を継続

    modes.receiver.start(config, measurement_queue)

    db_config = DBConfig(
        host=config.database.host,
        port=config.database.port,
        name=config.database.name,
        user=config.database.user,
        password=config.database.password,
    )

    try:
        modes.database_postgresql.store_queue(
            conn, measurement_queue, liveness_file, db_config, config.slack, count
        )
    except Exception:
        logging.exception("Failed to store data")

    modes.receiver.term()


######################################################################
if __name__ == "__main__":
    import docopt
    import my_lib.config
    import my_lib.logger

    from modes.config import load_from_dict

    assert __doc__ is not None  # noqa: S101
    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    count = int(args["-n"])
    debug_mode = args["-D"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config_dict = my_lib.config.load(config_file, pathlib.Path(SCHEMA_CONFIG))
    config = load_from_dict(config_dict, pathlib.Path.cwd())

    execute(config, config.liveness.file.collector, count)
