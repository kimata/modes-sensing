#!/usr/bin/env python3
"""
Mode S のメッセージを保管し，条件にマッチしたものを出力します．

Usage:
  database.py [-c CONFIG]

Options:
  -c CONFIG     : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
"""

import datetime
import logging
import queue
import sqlite3
import traceback


def open(log_db_path):
    sqlite = sqlite3.connect(log_db_path)
    sqlite.execute(
        "CREATE TABLE IF NOT EXISTS meteorological_data ("
        + "id INTEGER primary key autoincrement, time INTEGER NOT NULL, "
        + "callsign TEXT NOT NULL, altitude REAL, latitude REAL, longitude REAL, "
        + "temperature REAL, wind_x REAL, wind_y REAL, "
        + "wind_angle REAL, wind_speed REAL"
        + ");"
    )
    sqlite.execute("CREATE INDEX IF NOT EXISTS idx_tim ON meteorological_data (time);")
    sqlite.commit()
    sqlite.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))

    return sqlite


def insert(sqlite, data):
    sqlite.execute(
        'INSERT INTO meteorological_data VALUES (NULL, strftime("%s", "now"), ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (
            data["callsign"],
            data["altitude"],
            data["latitude"],
            data["longitude"],
            data["temperature"],
            data["wind"]["x"],
            data["wind"]["y"],
            data["wind"]["angle"],
            data["wind"]["speed"],
        ),
    )
    sqlite.commit()


def store_queue(sqlite, queue):
    try:
        while True:
            data = queue.get()
            logging.info(data)
            insert(sqlite, data)
    except Exception:
        sqlite.close()
        logging.error(traceback.format_exc())


def fetch_by_time(sqlite, time_start, time_end):
    cur = sqlite.cursor()

    cur.execute(
        "SELECT * FROM meteorological_data WHERE time BETWEEN ? AND ?",
        (
            time_start.astimezone(datetime.timezone.utc),
            time_end.astimezone(datetime.timezone.utc),
        ),
    )

    data_list = [
        {
            **data,
            "time": (
                datetime.datetime.strptime(data["time"], "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=9)
            ),
        }
        for data in cur.fetchall()
    ]

    return data_list


if __name__ == "__main__":
    import modes.receiver
    import my_lib.config
    import my_lib.logger
    from docopt import docopt

    args = docopt(__doc__)

    my_lib.logger.init("ModeS sensing", level=logging.INFO)

    config_file = args["-c"]
    config = my_lib.config.load(args["-c"])

    measurement_queue = queue.Queue()

    modes.receiver.start(
        config["modes"]["decoder"]["host"],
        config["modes"]["decoder"]["port"],
        measurement_queue,
        config["filter"]["area"],
    )

    sqlite = open(config["database"]["path"])

    store_queue(sqlite, measurement_queue)
