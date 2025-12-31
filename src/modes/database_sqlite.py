#!/usr/bin/env python3
"""
Mode S のメッセージを保管し，条件にマッチしたものを出力します．

Usage:
  database_sqlite.py [-c CONFIG] [-D]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
  -D                : デバッグモードで動作します．
"""

from __future__ import annotations

import datetime
import logging
import queue
import time
from typing import TYPE_CHECKING, Any

import my_lib.footprint
import my_lib.sqlite_util

from modes.database_postgresql import DataRangeResult, MeasurementData

if TYPE_CHECKING:
    import pathlib
    import sqlite3


def open(log_db_path: pathlib.Path) -> sqlite3.Connection:  # noqa: A001
    with my_lib.sqlite_util.connect(log_db_path) as sqlite:
        sqlite.execute(
            "CREATE TABLE IF NOT EXISTS meteorological_data ("
            "id INTEGER primary key autoincrement, time INTEGER NOT NULL, "
            "callsign TEXT NOT NULL, distance REAL, altitude REAL, latitude REAL, longitude REAL, "
            "temperature REAL, wind_x REAL, wind_y REAL, "
            "wind_angle REAL, wind_speed REAL, method TEXT);"
        )
        sqlite.execute("CREATE INDEX IF NOT EXISTS idx_time ON meteorological_data (time);")
        sqlite.execute("CREATE INDEX IF NOT EXISTS idx_distance ON meteorological_data (distance);")
        sqlite.execute(
            "CREATE INDEX IF NOT EXISTS idx_time_distance ON meteorological_data (time, distance);"
        )
        sqlite.commit()
        sqlite.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r, strict=False))

        return sqlite


def insert(sqlite: sqlite3.Connection, data: MeasurementData) -> None:
    sqlite.execute(
        "INSERT INTO meteorological_data VALUES "
        '(NULL, strftime("%s", "now"), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (
            data.callsign,
            data.distance,
            data.altitude,
            data.latitude,
            data.longitude,
            data.temperature,
            data.wind.x,
            data.wind.y,
            data.wind.angle,
            data.wind.speed,
            data.method,
        ),
    )
    sqlite.commit()


def store_queue(
    sqlite: sqlite3.Connection,
    queue: queue.Queue[MeasurementData],
    liveness_file: pathlib.Path,
    count: int = 0,
) -> None:
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


def fetch_by_time(
    sqlite: sqlite3.Connection,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    distance: float,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
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
        "method",
    ]
    sanitized_columns = [col for col in columns if col in valid_columns]

    if not sanitized_columns:
        msg = "No valid columns specified"
        raise ValueError(msg)

    columns_str = ", ".join(sanitized_columns)

    start = time.perf_counter()

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

    data = [
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
    logging.info(
        "Elapsed time: %.2f sec (selected %d columns, %s rows)",
        time.perf_counter() - start,
        len(sanitized_columns),
        f"{len(data):,}",
    )

    return data


def fetch_latest(
    conn: sqlite3.Connection,
    limit: int,
    distance: float | None = None,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    最新のデータを指定された件数取得する

    Args:
        conn: SQLite接続
        limit: 取得する最大件数
        distance: 距離フィルタ（Noneの場合はフィルタなし）
        columns: 取得するカラムのリスト。Noneの場合はデフォルト['time', 'altitude', 'temperature', 'distance']

    Returns:
        取得されたデータのリスト（時間の降順でソート）

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
        "method",
    ]
    sanitized_columns = [col for col in columns if col in valid_columns]

    if not sanitized_columns:
        msg = "No valid columns specified"
        raise ValueError(msg)

    columns_str = ", ".join(sanitized_columns)

    start = time.perf_counter()
    cur = conn.cursor()

    # 距離フィルタの有無で条件分岐
    if distance is not None:
        query = (
            f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
            f"WHERE altitude IS NOT NULL AND temperature IS NOT NULL "
            f"AND temperature > -100 AND distance <= ? "
            f"ORDER BY time DESC LIMIT ?"
        )
        cur.execute(query, (distance, limit))
    else:
        query = (
            f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
            f"WHERE altitude IS NOT NULL AND temperature IS NOT NULL "
            f"AND temperature > -100 "
            f"ORDER BY time DESC LIMIT ?"
        )
        cur.execute(query, (limit,))

    # SQLiteの時間データをdatetime型に変換
    data = []
    for row in cur.fetchall():
        row_data = dict(row)
        if row_data.get("time"):
            # SQLiteのtimeカラムはUNIX timestampとして格納されている
            if isinstance(row_data["time"], int):
                row_data["time"] = datetime.datetime.fromtimestamp(
                    row_data["time"], tz=datetime.timezone.utc
                ) + datetime.timedelta(hours=9)
            elif isinstance(row_data["time"], str):
                row_data["time"] = datetime.datetime.strptime(row_data["time"], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=datetime.timezone.utc
                ) + datetime.timedelta(hours=9)
        data.append(row_data)

    logging.info(
        "Elapsed time: %.2f sec (selected %d columns, %s rows)",
        time.perf_counter() - start,
        len(sanitized_columns),
        f"{len(data):,}",
    )

    return data


def fetch_data_range(conn: sqlite3.Connection) -> DataRangeResult:
    """
    データベースの最古・最新データの日時とレコード数を取得する

    Args:
        conn: SQLite接続

    Returns:
        DataRangeResult: earliest, latest, countを含むデータクラス

    """
    query = """
    SELECT
        MIN(time) as earliest,
        MAX(time) as latest,
        COUNT(*) as count
    FROM meteorological_data
    """

    start = time.perf_counter()
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()

    logging.info(
        "Elapsed time: %.2f sec (data range query)",
        time.perf_counter() - start,
    )

    if result and result["earliest"] and result["latest"]:
        # SQLiteの時間データをdatetime型に変換
        earliest = result["earliest"]
        latest = result["latest"]

        if isinstance(earliest, int):
            earliest = datetime.datetime.fromtimestamp(
                earliest, tz=datetime.timezone.utc
            ) + datetime.timedelta(hours=9)
        elif isinstance(earliest, str):
            earliest = datetime.datetime.strptime(earliest, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=datetime.timezone.utc
            ) + datetime.timedelta(hours=9)

        if isinstance(latest, int):
            latest = datetime.datetime.fromtimestamp(latest, tz=datetime.timezone.utc) + datetime.timedelta(
                hours=9
            )
        elif isinstance(latest, str):
            latest = datetime.datetime.strptime(latest, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=datetime.timezone.utc
            ) + datetime.timedelta(hours=9)

        return DataRangeResult(
            earliest=earliest,
            latest=latest,
            count=result["count"],
        )
    else:
        # データがない場合
        return DataRangeResult(earliest=None, latest=None, count=0)


if __name__ == "__main__":
    import docopt
    import my_lib.config
    import my_lib.logger

    import modes.receiver

    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    debug_mode = args["-D"]

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
