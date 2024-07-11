#!/usr/bin/env python3
"""
Mode S のメッセージを保管し，条件にマッチしたものを出力します．

Usage:
  database.py [-c CONFIG]

Options:
  -c CONFIG     : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
"""

import sqlite3
import logging
import queue
import traceback

import modes.receiver


def open(log_db_path):
    sqlite = sqlite3.connect(log_db_path)
    sqlite.execute(
        "CREATE TABLE IF NOT EXISTS meteorological_data("
        + "id INTEGER primary key autoincrement, time TEXT NOT NULL, "
        + "callsign TEXT NOT NULL, altitude REAL, latitude REAL, longitude REAL, "
        + "temperature REAL, wind_x REAL, wind_y REAL, "
        + "wind_angle REAL, wind_speed REAL"
        + ")"
    )
    sqlite.commit()
    sqlite.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))

    return sqlite


def insert(sqlite, data):
    sqlite.execute(
        'INSERT INTO meteorological_data VALUES (NULL, DATETIME("now"), ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [
            data["callsign"],
            data["altitude"],
            data["latitude"],
            data["longitude"],
            data["temperature"],
            data["wind"]["x"],
            data["wind"]["y"],
            data["wind"]["angle"],
            data["wind"]["speed"],
        ],
    )


def store(queue):
    log_db_path = "meteorological_data.db"
    sqlite = open(log_db_path)

    try:
        while True:
            data = queue.get()
            logging.info(data)
            insert(sqlite, data)
            sqlite.commit()
    except Exception:
        sqlite.close()
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    from docopt import docopt

    import local_lib.config
    import local_lib.logger

    args = docopt(__doc__)

    local_lib.logger.init("ModeS sensing", level=logging.INFO)

    config_file = args["-c"]
    config = local_lib.config.load(args["-c"])

    measurement_queue = queue.Queue()

    modes.receiver.start(
        config["modes"]["decoder"]["host"],
        config["modes"]["decoder"]["port"],
        measurement_queue,
        config["filter"]["area"],
    )

    store(measurement_queue)
