#!/usr/bin/env python3
"""SQLite データベースアクセス（開発・テスト用）

DEPRECATED: 本番環境では postgresql.py を使用してください。
このファイルは開発・テスト用途のみでサポートされます。

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
import pathlib
import queue
import time
from typing import TYPE_CHECKING, Any

import my_lib.footprint
import my_lib.notify.slack
import my_lib.sqlite_util
import my_lib.time

from amdar.constants import GRAPH_TEMPERATURE_THRESHOLD, get_db_schema_path, sanitize_columns
from amdar.database.postgresql import (
    VALID_METEOROLOGICAL_COLUMNS,
    DataRangeResult,
    MeasurementData,
)

# スキーマファイルのパス
_SCHEMA_FILE = get_db_schema_path("sqlite.schema")


def _parse_sqlite_timestamp(value: int | str | None) -> datetime.datetime | None:
    """SQLite のタイムスタンプを datetime に変換する

    SQLite では時刻が UNIX timestamp (int) または ISO 形式文字列 (str) で
    格納されている。これを JST の datetime に変換する。

    Args:
        value: UNIX timestamp (int)、ISO 形式文字列 (str)、または None

    Returns:
        JST タイムゾーン付き datetime、または None
    """
    if value is None:
        return None

    jst = my_lib.time.get_zoneinfo()

    if isinstance(value, int):
        return datetime.datetime.fromtimestamp(value, tz=jst)
    if isinstance(value, str):
        dt = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=jst)
    return None


if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Sequence


def open(log_db_path: pathlib.Path) -> sqlite3.Connection:
    with my_lib.sqlite_util.connect(log_db_path) as sqlite:
        # 外部スキーマファイルからスキーマを読み込んで実行
        _execute_schema(sqlite)
        sqlite.commit()
        sqlite.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r, strict=False))

        return sqlite


def _execute_schema(sqlite: sqlite3.Connection) -> None:
    """外部スキーマファイルを読み込んで実行"""
    schema_sql = _SCHEMA_FILE.read_text(encoding="utf-8")

    # スキーマファイル内の各ステートメントを実行
    for raw_statement in schema_sql.split(";"):
        statement = raw_statement.strip()
        if statement and not statement.startswith("--"):
            sqlite.execute(statement)


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
    data_queue: queue.Queue[MeasurementData],
    liveness_file: pathlib.Path,
    slack_config: my_lib.notify.slack.SlackErrorOnlyConfig | my_lib.notify.slack.SlackEmptyConfig,
    count: int = 0,
) -> None:
    """データベースへのデータ格納を行うワーカー関数

    Args:
        sqlite: SQLite接続
        data_queue: 測定データのキュー
        liveness_file: ヘルスチェック用ファイルパス
        slack_config: Slack通知設定
        count: 処理するデータ数（0の場合は無制限）

    """
    i = 0
    try:
        while True:
            data = data_queue.get()
            insert(sqlite, data)
            my_lib.footprint.update(liveness_file)

            i += 1
            if (count != 0) and (i == count):
                break
    except Exception:
        sqlite.close()
        logging.exception("Database error occurred")
        my_lib.notify.slack.error(
            slack_config,
            "データベースエラー",
            "SQLiteデータベースへの保存中にエラーが発生しました",
        )


def fetch_by_time(
    sqlite: sqlite3.Connection,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    distance: float,
    columns: list[str] | None = None,
) -> Sequence[dict[str, Any]]:
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
    columns_str = sanitize_columns(columns, VALID_METEOROLOGICAL_COLUMNS)

    start = time.perf_counter()

    cur = sqlite.cursor()

    query = (
        f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
        f"WHERE time BETWEEN ? AND ? AND distance <= ? ORDER BY time"
    )
    cur.execute(
        query,
        (
            time_start.astimezone(datetime.UTC),
            time_end.astimezone(datetime.UTC),
            distance,
        ),
    )

    data = [
        {
            **row,
            "time": _parse_sqlite_timestamp(row.get("time")) if "time" in row else None,
        }
        for row in cur.fetchall()
    ]
    logging.info(
        "Elapsed time: %.2f sec (selected %d columns, %s rows)",
        time.perf_counter() - start,
        columns_str.count(",") + 1,
        f"{len(data):,}",
    )

    return data


def fetch_latest(
    conn: sqlite3.Connection,
    limit: int,
    distance: float | None = None,
    columns: list[str] | None = None,
) -> Sequence[dict[str, Any]]:
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
    columns_str = sanitize_columns(columns, VALID_METEOROLOGICAL_COLUMNS)

    start = time.perf_counter()
    cur = conn.cursor()

    # 距離フィルタの有無で条件分岐
    if distance is not None:
        query = (
            f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
            f"WHERE altitude IS NOT NULL AND temperature IS NOT NULL "
            f"AND temperature > {GRAPH_TEMPERATURE_THRESHOLD} AND distance <= ? "
            f"ORDER BY time DESC LIMIT ?"
        )
        cur.execute(query, (distance, limit))
    else:
        query = (
            f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
            f"WHERE altitude IS NOT NULL AND temperature IS NOT NULL "
            f"AND temperature > {GRAPH_TEMPERATURE_THRESHOLD} "
            f"ORDER BY time DESC LIMIT ?"
        )
        cur.execute(query, (limit,))

    # SQLiteの時間データをdatetime型に変換
    data = []
    for row in cur.fetchall():
        row_data = dict(row)
        if row_data.get("time"):
            row_data["time"] = _parse_sqlite_timestamp(row_data["time"])
        data.append(row_data)

    logging.info(
        "Elapsed time: %.2f sec (selected %d columns, %s rows)",
        time.perf_counter() - start,
        columns_str.count(",") + 1,
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
        earliest = _parse_sqlite_timestamp(result["earliest"])
        latest = _parse_sqlite_timestamp(result["latest"])

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

    import amdar.config
    import amdar.constants
    import amdar.sources.modes.receiver

    assert __doc__ is not None  # noqa: S101
    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    debug_mode = args["-D"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config_dict = my_lib.config.load(config_file, amdar.constants.get_schema_path())
    config = amdar.config.load_from_dict(config_dict, pathlib.Path.cwd())

    measurement_queue: queue.Queue[MeasurementData] = queue.Queue()

    amdar.sources.modes.receiver.start(config, measurement_queue)

    sqlite = open(pathlib.Path(config_dict["database"]["path"]))

    store_queue(
        sqlite, measurement_queue, pathlib.Path(config_dict["liveness"]["file"]["collector"]), config.slack
    )
