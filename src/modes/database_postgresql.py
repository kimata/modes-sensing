#!/usr/bin/env python3
"""
Mode S のメッセージを保管し，条件にマッチしたものを出力します．

Usage:
  database_postgres.py [-c CONFIG] [-D]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
  -D                : デバッグモードで動作します．
"""

from __future__ import annotations

import contextlib
import datetime
import logging
import queue
import threading
import time
from typing import TYPE_CHECKING, Any, TypedDict

import my_lib.footprint
import psycopg2
import psycopg2.extras

if TYPE_CHECKING:
    import multiprocessing
    import pathlib

    from psycopg2.extensions import connection as PgConnection  # noqa: N812


class WindData(TypedDict):
    """風向・風速データ"""

    x: float
    y: float
    angle: float
    speed: float


class MeasurementData(TypedDict, total=False):
    """測定データ（receiver.py から受け取る形式）"""

    callsign: str
    distance: float
    altitude: float
    latitude: float
    longitude: float
    temperature: float
    wind: WindData
    method: str | None


class DataRangeResult(TypedDict):
    """データ範囲クエリの結果"""

    earliest: datetime.datetime | None
    latest: datetime.datetime | None
    count: int


class AggregationLevel(TypedDict):
    """集約レベルの設定"""

    table: str
    time_interval: str
    altitude_bin: int
    max_days: int


# 期間に応じた集約レベルの定義
AGGREGATION_LEVELS: list[AggregationLevel] = [
    # 7日以内は生データ
    {"table": "meteorological_data", "time_interval": "raw", "altitude_bin": 0, "max_days": 7},
    # 7-30日は1時間×500m集約
    {"table": "hourly_altitude_grid", "time_interval": "1 hour", "altitude_bin": 500, "max_days": 30},
    # 30日以上は6時間×500m集約
    {"table": "sixhour_altitude_grid", "time_interval": "6 hours", "altitude_bin": 500, "max_days": 9999},
]


should_terminate = threading.Event()


def open(host: str, port: int, database: str, user: str, password: str) -> PgConnection:  # noqa: A001
    connection_params: dict[str, Any] = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }

    try:
        conn = psycopg2.connect(**connection_params)
    except psycopg2.OperationalError as e:
        if "does not exist" in str(e):
            # データベースが存在しない場合、postgresデータベースに接続して作成
            admin_params = connection_params.copy()
            admin_params["database"] = "postgres"

            admin_conn = psycopg2.connect(**admin_params)
            admin_conn.autocommit = True

            with admin_conn.cursor() as cur:
                # データベース名をエスケープしてSQLインジェクションを防ぐ
                cur.execute(f"CREATE DATABASE {psycopg2.extensions.quote_ident(database, admin_conn)}")

            admin_conn.close()

            # 新しく作成したデータベースに接続
            conn = psycopg2.connect(**connection_params)
        else:
            raise

    conn.autocommit = True

    with conn.cursor() as cur:
        # テーブルを再作成する場合は削除
        # cur.execute("DROP TABLE IF EXISTS meteorological_data CASCADE;")

        cur.execute(
            "CREATE TABLE IF NOT EXISTS meteorological_data ("
            "id SERIAL PRIMARY KEY, "
            "time TIMESTAMP NOT NULL, "
            "callsign TEXT NOT NULL, "
            "distance REAL, "
            "altitude REAL, "
            "latitude REAL, "
            "longitude REAL, "
            "temperature REAL, "
            "wind_x REAL, "
            "wind_y REAL, "
            "wind_angle REAL, "
            "wind_speed REAL, "
            "method TEXT"
            ");"
        )

        # インデックス設計方針:
        # - グラフ描画クエリの最適化を重視（distance <= 100, temperature > -100の条件が多い）
        # - 部分インデックス（WHERE句付き）により、メンテナンスオーバーヘッドを削減
        # - 使用頻度の低い位置情報系インデックス（latitude, longitude）は削除

        # 基本インデックス（単一カラムでの範囲検索用）
        cur.execute("CREATE INDEX IF NOT EXISTS idx_time ON meteorological_data (time);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_distance ON meteorological_data (distance);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_altitude ON meteorological_data (altitude);")

        # 複合インデックス（よく使われる組み合わせ）
        # 時刻と距離の組み合わせ（メインクエリ用）
        cur.execute("CREATE INDEX IF NOT EXISTS idx_time_distance ON meteorological_data (time, distance);")
        # 時刻と高度の組み合わせ（グラフ表示用）
        cur.execute("CREATE INDEX IF NOT EXISTS idx_time_alt ON meteorological_data (time, altitude);")

        # 高効率部分インデックス（グラフ描画用 - 条件付きインデックスで効率化）
        # fetch_by_time関数で最も頻繁に使用される条件に特化
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_optimized_fetch
            ON meteorological_data (time, distance)
            WHERE distance <= 100 AND temperature > -100 AND altitude IS NOT NULL;
        """)

        # 風向データ専用インデックス（風向グラフ用）
        # 風向グラフ生成時の高速化（wind_speed > 0.1で無風データを除外）
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_wind_data
            ON meteorological_data (time, altitude, wind_x, wind_y, wind_speed, wind_angle)
            WHERE distance <= 100 AND wind_speed > 0.1;
        """)

        # 温度・高度データ用複合インデックス（グラフ生成の高速化）
        # 等高線、ヒートマップ、散布図の生成で使用
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_time_distance_temp_alt
            ON meteorological_data (time, distance, temperature, altitude)
            WHERE distance <= 100 AND temperature > -100;
        """)

        # BRIN インデックス（時系列データに効果的、メモリ効率良い）
        # 大量の時系列データでの範囲検索で効果的、メモリ使用量が少ない
        cur.execute("CREATE INDEX IF NOT EXISTS idx_time_brin ON meteorological_data USING BRIN (time);")

        # マテリアライズドビュー: 1時間×500m高度帯の集約データ
        # 7日〜30日の期間表示に使用
        cur.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_altitude_grid AS
            SELECT
                date_trunc('hour', time) AS time_bucket,
                (floor(altitude / 500) * 500)::int AS altitude_bin,
                AVG(temperature) AS temperature,
                AVG(wind_x) AS wind_x,
                AVG(wind_y) AS wind_y,
                AVG(wind_speed) AS wind_speed,
                AVG(wind_angle) AS wind_angle,
                COUNT(*) AS sample_count
            FROM meteorological_data
            WHERE distance <= 100
              AND temperature > -100
              AND altitude IS NOT NULL
              AND altitude >= 0
              AND altitude <= 13000
            GROUP BY date_trunc('hour', time), (floor(altitude / 500) * 500)::int;
        """)

        # 1時間集約ビューのインデックス
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_hourly_grid_time
            ON hourly_altitude_grid (time_bucket);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_hourly_grid_time_alt
            ON hourly_altitude_grid (time_bucket, altitude_bin);
        """)

        # マテリアライズドビュー: 6時間×500m高度帯の集約データ
        # 30日以上の長期間表示に使用
        cur.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS sixhour_altitude_grid AS
            SELECT
                date_trunc('hour', time)
                    - (EXTRACT(hour FROM time)::int % 6) * interval '1 hour' AS time_bucket,
                (floor(altitude / 500) * 500)::int AS altitude_bin,
                AVG(temperature) AS temperature,
                AVG(wind_x) AS wind_x,
                AVG(wind_y) AS wind_y,
                AVG(wind_speed) AS wind_speed,
                AVG(wind_angle) AS wind_angle,
                COUNT(*) AS sample_count
            FROM meteorological_data
            WHERE distance <= 100
              AND temperature > -100
              AND altitude IS NOT NULL
              AND altitude >= 0
              AND altitude <= 13000
            GROUP BY
                date_trunc('hour', time) - (EXTRACT(hour FROM time)::int % 6) * interval '1 hour',
                (floor(altitude / 500) * 500)::int;
        """)

        # 6時間集約ビューのインデックス
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_sixhour_grid_time
            ON sixhour_altitude_grid (time_bucket);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_sixhour_grid_time_alt
            ON sixhour_altitude_grid (time_bucket, altitude_bin);
        """)

    return conn


def insert(conn: PgConnection, data: MeasurementData) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO meteorological_data (time, callsign, distance, altitude, latitude, longitude, "
            "temperature, wind_x, wind_y, wind_angle, wind_speed, method) "
            "VALUES (CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
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
                data.get("method"),  # methodがない場合はNoneを挿入
            ),
        )


def store_queue(  # noqa: C901
    conn: PgConnection,
    measurement_queue: multiprocessing.Queue[MeasurementData],
    liveness_file: pathlib.Path,
    count: int = 0,
    db_config: dict[str, Any] | None = None,
) -> None:
    """
    データベースへのデータ格納を行うワーカー関数

    Args:
        conn: データベース接続
        measurement_queue: 測定データのキュー
        liveness_file: ヘルスチェック用ファイルパス
        count: 処理するデータ数（0の場合は無制限）
        db_config: 再接続用のデータベース設定（config["database"]形式）

    """
    logging.info("Start store worker")

    i = 0
    consecutive_errors = 0
    max_consecutive_errors = 3

    while True:
        try:
            data = measurement_queue.get(timeout=1)
            insert(conn, data)
            my_lib.footprint.update(liveness_file)

            # 成功したらエラーカウンタをリセット
            consecutive_errors = 0

            i += 1
            if (count != 0) and (i == count):
                break

        except queue.Empty:
            # タイムアウトはエラーとしてカウントしない
            pass

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            consecutive_errors += 1
            logging.error(  # noqa: TRY400
                "Database connection error (%d/%d consecutive): %s",
                consecutive_errors,
                max_consecutive_errors,
                str(e),
            )

            if consecutive_errors >= max_consecutive_errors:
                if db_config:
                    logging.error("Maximum consecutive errors reached. Attempting to reconnect...")  # noqa: TRY400

                    with contextlib.suppress(Exception):
                        # 既存の接続をクローズ
                        conn.close()

                    try:
                        # 再接続を試みる
                        conn = open(
                            db_config["host"],
                            db_config["port"],
                            db_config["name"],
                            db_config["user"],
                            db_config["pass"],
                        )
                        logging.info("Successfully reconnected to database")
                        consecutive_errors = 0  # 再接続成功したらカウンタをリセット
                        continue
                    except Exception:
                        logging.exception("Reconnection failed")
                        break
                else:
                    logging.error(  # noqa: TRY400
                        "Failed to reconnect after %d consecutive errors. Terminating.",
                        max_consecutive_errors,
                    )
                    break

        except Exception:
            consecutive_errors += 1
            logging.exception(
                "Unexpected error in store_queue (%d/%d consecutive)",
                consecutive_errors,
                max_consecutive_errors,
            )

            if consecutive_errors >= max_consecutive_errors:
                logging.error("Maximum consecutive errors reached. Terminating.")  # noqa: TRY400
                break

        if should_terminate.is_set():
            break

    with contextlib.suppress(Exception):
        conn.close()

    logging.warning("Stop store worker")


def store_term() -> None:
    should_terminate.set()


def fetch_by_time(  # noqa: PLR0913
    conn: PgConnection,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    distance: float,
    columns: list[str] | None = None,
    max_altitude: float | None = None,
) -> list[dict[str, Any]]:
    """
    指定された時間範囲と距離でデータを取得する

    Args:
        conn: データベース接続
        time_start: 開始時刻
        time_end: 終了時刻
        distance: 距離フィルタ
        columns: 取得するカラムのリスト。Noneの場合はデフォルト['time', 'altitude', 'temperature', 'distance']
        max_altitude: 最大高度フィルタ（Noneの場合はフィルタなし）

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
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # クエリを最適化：インデックスを効率的に使用し、不要なデータを事前フィルタ
        if max_altitude is not None:
            query = (
                f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
                f"WHERE time >= %s AND time <= %s AND distance <= %s "
                f"AND altitude IS NOT NULL AND altitude <= %s "
                f"ORDER BY time"
            )
            cur.execute(
                query,
                (
                    time_start.astimezone(datetime.timezone.utc),
                    time_end.astimezone(datetime.timezone.utc),
                    distance,
                    max_altitude,
                ),
            )
        else:
            query = (
                f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
                f"WHERE time >= %s AND time <= %s AND distance <= %s "
                f"AND altitude IS NOT NULL "
                f"ORDER BY time"
            )
            cur.execute(
                query,
                (
                    time_start.astimezone(datetime.timezone.utc),
                    time_end.astimezone(datetime.timezone.utc),
                    distance,
                ),
            )
        # fetchallではなく大きなデータセット向けにitersize指定でメモリ効率化
        cur.itersize = 10000  # 大量データ取得時のメモリ効率化
        data = cur.fetchall()

        logging.info(
            "Elapsed time: %.2f sec (selected %d columns, %s rows)",
            time.perf_counter() - start,
            len(sanitized_columns),
            f"{len(data):,}",
        )

        return data


def fetch_latest(
    conn: PgConnection,
    limit: int,
    distance: float | None = None,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    最新のデータを指定された件数取得する

    Args:
        conn: データベース接続
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
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # 距離フィルタの有無で条件分岐
        if distance is not None:
            query = (
                f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
                f"WHERE altitude IS NOT NULL AND temperature IS NOT NULL "
                f"AND temperature > -100 AND distance <= %s "
                f"ORDER BY time DESC LIMIT %s"
            )
            cur.execute(query, (distance, limit))
        else:
            query = (
                f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
                f"WHERE altitude IS NOT NULL AND temperature IS NOT NULL "
                f"AND temperature > -100 "
                f"ORDER BY time DESC LIMIT %s"
            )
            cur.execute(query, (limit,))

        data = cur.fetchall()

        logging.info(
            "Elapsed time: %.2f sec (selected %d columns, %s rows)",
            time.perf_counter() - start,
            len(sanitized_columns),
            f"{len(data):,}",
        )

        return data


def fetch_data_range(conn: PgConnection) -> DataRangeResult:
    """
    データベースの最古・最新データの日時とレコード数を取得する

    Args:
        conn: データベース接続

    Returns:
        dict: earliest, latest, countを含む辞書

    """
    query = """
    SELECT
        MIN(time) as earliest,
        MAX(time) as latest,
        COUNT(*) as count
    FROM meteorological_data
    """

    start = time.perf_counter()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        result = cur.fetchone()

    logging.info(
        "Elapsed time: %.2f sec (data range query)",
        time.perf_counter() - start,
    )

    if result and result["earliest"] and result["latest"]:
        return {
            "earliest": result["earliest"],
            "latest": result["latest"],
            "count": result["count"],
        }
    else:
        # データがない場合
        return {"earliest": None, "latest": None, "count": 0}


def get_aggregation_level(days: float) -> AggregationLevel:
    """
    期間に応じた適切な集約レベルを取得する

    Args:
        days: クエリ対象の期間（日数）

    Returns:
        適切な集約レベルの設定

    """
    for level in AGGREGATION_LEVELS:
        if days <= level["max_days"]:
            return level
    return AGGREGATION_LEVELS[-1]


def fetch_aggregated_by_time(
    conn: PgConnection,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    max_altitude: float | None = None,
) -> list[dict[str, Any]]:
    """
    期間に応じて適切な集約レベルのデータを取得する

    Args:
        conn: データベース接続
        time_start: 開始時刻
        time_end: 終了時刻
        max_altitude: 最大高度フィルタ（Noneの場合はフィルタなし）

    Returns:
        取得されたデータのリスト（生データ形式に変換済み）

    """
    days = (time_end - time_start).total_seconds() / 86400
    level = get_aggregation_level(days)

    logging.info(
        "Using aggregation level: %s (period: %.1f days, interval: %s, altitude_bin: %dm)",
        level["table"],
        days,
        level["time_interval"],
        level["altitude_bin"],
    )

    # 生データの場合は既存の関数を使用
    if level["table"] == "meteorological_data":
        return fetch_by_time(
            conn,
            time_start,
            time_end,
            distance=100,  # 集約ビューは既にdistance<=100でフィルタ済み
            columns=["time", "altitude", "temperature", "wind_x", "wind_y", "wind_speed", "wind_angle"],
            max_altitude=max_altitude,
        )

    # 集約データを取得
    start = time.perf_counter()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if max_altitude is not None:
            query = f"""
                SELECT
                    time_bucket AS time,
                    altitude_bin AS altitude,
                    temperature,
                    wind_x,
                    wind_y,
                    wind_speed,
                    wind_angle,
                    sample_count
                FROM {level["table"]}
                WHERE time_bucket >= %s
                  AND time_bucket <= %s
                  AND altitude_bin <= %s
                ORDER BY time_bucket, altitude_bin
            """  # noqa: S608
            cur.execute(
                query,
                (
                    time_start.astimezone(datetime.timezone.utc),
                    time_end.astimezone(datetime.timezone.utc),
                    max_altitude,
                ),
            )
        else:
            query = f"""
                SELECT
                    time_bucket AS time,
                    altitude_bin AS altitude,
                    temperature,
                    wind_x,
                    wind_y,
                    wind_speed,
                    wind_angle,
                    sample_count
                FROM {level["table"]}
                WHERE time_bucket >= %s
                  AND time_bucket <= %s
                ORDER BY time_bucket, altitude_bin
            """  # noqa: S608
            cur.execute(
                query,
                (
                    time_start.astimezone(datetime.timezone.utc),
                    time_end.astimezone(datetime.timezone.utc),
                ),
            )

        cur.itersize = 10000
        data = cur.fetchall()

        logging.info(
            "Elapsed time: %.2f sec (aggregated data from %s, %s rows)",
            time.perf_counter() - start,
            level["table"],
            f"{len(data):,}",
        )

        return data


def refresh_materialized_views(conn: PgConnection) -> dict[str, float]:
    """
    全てのマテリアライズドビューを更新する

    Args:
        conn: データベース接続

    Returns:
        各ビューの更新にかかった時間（秒）

    """
    views = ["hourly_altitude_grid", "sixhour_altitude_grid"]
    timings: dict[str, float] = {}

    for view in views:
        start = time.perf_counter()
        try:
            with conn.cursor() as cur:
                # CONCURRENTLYを使用すると、更新中もビューを読み取り可能
                # ただし、初回はインデックスが必要
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
            elapsed = time.perf_counter() - start
            timings[view] = elapsed
            logging.info("Refreshed %s in %.2f sec", view, elapsed)
        except psycopg2.errors.ObjectNotInPrerequisiteState:
            # CONCURRENTLY が使えない場合（ユニークインデックスがない）は通常のREFRESH
            with conn.cursor() as cur:
                cur.execute(f"REFRESH MATERIALIZED VIEW {view}")
            elapsed = time.perf_counter() - start
            timings[view] = elapsed
            logging.info("Refreshed %s (non-concurrent) in %.2f sec", view, elapsed)
        except Exception:
            logging.exception("Failed to refresh %s", view)
            timings[view] = -1

    return timings


def check_materialized_views_exist(conn: PgConnection) -> dict[str, bool]:
    """
    マテリアライズドビューの存在を確認する

    Args:
        conn: データベース接続

    Returns:
        各ビューの存在フラグ

    """
    views = ["hourly_altitude_grid", "sixhour_altitude_grid"]
    result: dict[str, bool] = {}

    with conn.cursor() as cur:
        for view in views:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = %s)",
                (view,),
            )
            exists = cur.fetchone()[0]
            result[view] = exists

    return result


def get_materialized_view_stats(conn: PgConnection) -> dict[str, dict[str, Any]]:
    """
    マテリアライズドビューの統計情報を取得する

    Args:
        conn: データベース接続

    Returns:
        各ビューの統計情報

    """
    views = ["hourly_altitude_grid", "sixhour_altitude_grid"]
    stats: dict[str, dict[str, Any]] = {}

    for view in views:
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT
                        COUNT(*) as row_count,
                        MIN(time_bucket) as earliest,
                        MAX(time_bucket) as latest
                    FROM {view}
                    """  # noqa: S608
                )
                result = cur.fetchone()
                stats[view] = dict(result) if result else {"row_count": 0, "earliest": None, "latest": None}
        except Exception:  # noqa: PERF203
            logging.exception("Failed to get stats for %s", view)
            stats[view] = {"error": True}

    return stats


if __name__ == "__main__":
    import multiprocessing

    import docopt
    import my_lib.config
    import my_lib.logger

    import modes.receiver

    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    debug_mode = args["-D"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = my_lib.config.load(config_file)

    measurement_queue = multiprocessing.Queue()

    modes.receiver.start(
        config["modes"]["decoder"]["host"],
        config["modes"]["decoder"]["port"],
        measurement_queue,
        config["filter"]["area"],
    )

    conn = open(
        config["database"]["host"],
        config["database"]["port"],
        config["database"]["name"],
        config["database"]["user"],
        config["database"]["pass"],
    )

    store_queue(
        conn, measurement_queue, config["liveness"]["file"]["collector"], db_config=config["database"]
    )
