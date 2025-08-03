#!/usr/bin/env python3
"""
Mode S のメッセージを保管し，条件にマッチしたものを出力します．

Usage:
  database_sqlite.py [-c CONFIG]

Options:
  -c CONFIG     : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
"""

import datetime
import logging
import queue
import sqlite3

import my_lib.footprint


def open(log_db_path):  # noqa: A001
    sqlite = sqlite3.connect(log_db_path)
    sqlite.execute(
        "CREATE TABLE IF NOT EXISTS meteorological_data ("
        "id INTEGER primary key autoincrement, time INTEGER NOT NULL, "
        "callsign TEXT NOT NULL, distance REAL, altitude REAL, latitude REAL, longitude REAL, "
        "temperature REAL, wind_x REAL, wind_y REAL, "
        "wind_angle REAL, wind_speed REAL);"
    )
    sqlite.execute("CREATE INDEX IF NOT EXISTS idx_time ON meteorological_data (time);")
    sqlite.execute("CREATE INDEX IF NOT EXISTS idx_distance ON meteorological_data (distance);")
    sqlite.execute("CREATE INDEX IF NOT EXISTS idx_time_distance ON meteorological_data (time, distance);")
    sqlite.commit()
    sqlite.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r, strict=False))

    return sqlite


def insert(sqlite, data):
    sqlite.execute(
        'INSERT INTO meteorological_data VALUES (NULL, strftime("%s", "now"), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (
            data["callsign"],
            data["distance"],
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


def store_queue(sqlite, queue, liveness_file, count):
    i = 0
    try:
        while True:
            data = queue.get()
            insert(sqlite, data)
            my_lib.footprint.update(liveness_file)

            i += 1
            if (count != 0) and (i == count):
                break
    except Exception:
        sqlite.close()
        logging.exception("Database error occurred")


def fetch_by_time(sqlite, time_start, time_end, distance, columns=None):
    """
    指定された時間範囲と距離でデータを取得する

    Args:
        sqlite: SQLite接続
        time_start: 開始時刻
        time_end: 終了時刻
        distance: 距離フィルタ
        columns: 取得するカラムのリスト。Noneの場合はデフォルト['time', 'altitude', 'temperature', 'distance']

    Returns:
        取得されたデータのリスト

    """
    if columns is None:
        columns = ["time", "altitude", "temperature", "distance"]

    # カラム名をサニタイズ（SQLインジェクション対策）
    valid_columns = [
        "time",
        "callsign",
        "distance",
        "altitude",
        "latitude",
        "longitude",
        "temperature",
        "wind_x",
        "wind_y",
        "wind_angle",
        "wind_speed",
    ]
    sanitized_columns = [col for col in columns if col in valid_columns]

    if not sanitized_columns:
        msg = "No valid columns specified"
        raise ValueError(msg)

    columns_str = ", ".join(sanitized_columns)

    cur = sqlite.cursor()

    query = (
        f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
        f"WHERE time BETWEEN ? AND ? AND distance <= ? ORDER BY time"
    )
    cur.execute(
        query,
        (
            time_start.astimezone(datetime.timezone.utc),
            time_end.astimezone(datetime.timezone.utc),
            distance,
        ),
    )

    return [
        {
            **data,
            "time": (
                datetime.datetime.strptime(data["time"], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=datetime.timezone.utc
                )
                + datetime.timedelta(hours=9)
            )
            if "time" in data
            else None,
        }
        for data in cur.fetchall()
    ]


if __name__ == "__main__":
    import docopt
    import my_lib.config
    import my_lib.logger

    import modes.receiver

    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    debug_mode = args["-d"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = my_lib.config.load(config_file)

    measurement_queue = queue.Queue()

    modes.receiver.start(
        config["modes"]["decoder"]["host"],
        config["modes"]["decoder"]["port"],
        measurement_queue,
        config["filter"]["area"],
    )

    sqlite = open(config["database"]["path"])

    store_queue(sqlite, measurement_queue, config["liveness"]["file"]["collector"])
