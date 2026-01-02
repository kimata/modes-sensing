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
import logging
import pathlib
import queue
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import my_lib.footprint
import my_lib.notify.slack
import my_lib.time
import psycopg2
import psycopg2.extras

if TYPE_CHECKING:
    import datetime
    import multiprocessing
    from collections.abc import Sequence

    from psycopg2.extensions import connection as PgConnection  # noqa: N812


class ReconnectError(Exception):
    """データベース再接続に失敗した場合の例外"""


class TerminationRequestedError(Exception):
    """終了が要求された場合の例外"""


@dataclass(frozen=True)
class DBConfig:
    """データベース接続設定"""

    host: str
    port: int
    name: str
    user: str
    password: str


@dataclass
class WindData:
    """風向・風速データ"""

    x: float
    y: float
    angle: float
    speed: float


@dataclass
class MeasurementData:
    """測定データ（receiver.py から受け取る形式）"""

    callsign: str
    altitude: float
    latitude: float
    longitude: float
    temperature: float
    wind: WindData
    distance: float
    method: str = "mode-s"


@dataclass(frozen=True)
class DataRangeResult:
    """データ範囲クエリの結果"""

    earliest: datetime.datetime | None
    latest: datetime.datetime | None
    count: int


@dataclass(frozen=True)
class AggregationLevel:
    """集約レベルの設定"""

    table: str
    time_interval: str
    altitude_bin: int
    max_days: int


# 期間に応じたサンプリングレベルの定義
# 長期間では時間×高度帯から代表点を1つ選ぶことでデータ量を削減しつつ品質を維持
AGGREGATION_LEVELS: list[AggregationLevel] = [
    # 7日以内は生データ
    AggregationLevel(table="meteorological_data", time_interval="raw", altitude_bin=0, max_days=7),
    # 7-30日は1時間×500m帯から代表点をサンプリング
    AggregationLevel(table="hourly_altitude_grid", time_interval="1 hour", altitude_bin=500, max_days=30),
    # 30日以上は6時間×500m帯から代表点をサンプリング
    AggregationLevel(table="sixhour_altitude_grid", time_interval="6 hours", altitude_bin=500, max_days=9999),
]


# 再接続設定
MAX_RECONNECT_RETRIES: int = 5
RECONNECT_DELAY: float = 5.0

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

        # マテリアライズドビュー: 1時間×500m高度帯からの代表点サンプリング
        # 7日〜30日の期間表示に使用
        # 平均ではなく実際のデータ点を保持することで描画品質を維持
        cur.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_altitude_grid AS
            SELECT DISTINCT ON (time_bucket, altitude_bin)
                date_trunc('hour', time) AS time_bucket,
                (floor(altitude / 500) * 500)::int AS altitude_bin,
                time,
                altitude,
                temperature,
                wind_x,
                wind_y,
                wind_speed,
                wind_angle
            FROM meteorological_data
            WHERE distance <= 100
              AND temperature > -100
              AND altitude IS NOT NULL
              AND altitude >= 0
              AND altitude <= 13000
            ORDER BY time_bucket, altitude_bin, time DESC;
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

        # マテリアライズドビュー: 6時間×500m高度帯からの代表点サンプリング
        # 30日以上の長期間表示に使用
        # 平均ではなく実際のデータ点を保持することで描画品質を維持
        cur.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS sixhour_altitude_grid AS
            SELECT DISTINCT ON (time_bucket, altitude_bin)
                date_trunc('hour', time)
                    - (EXTRACT(hour FROM time)::int % 6) * interval '1 hour' AS time_bucket,
                (floor(altitude / 500) * 500)::int AS altitude_bin,
                time,
                altitude,
                temperature,
                wind_x,
                wind_y,
                wind_speed,
                wind_angle
            FROM meteorological_data
            WHERE distance <= 100
              AND temperature > -100
              AND altitude IS NOT NULL
              AND altitude >= 0
              AND altitude <= 13000
            ORDER BY
                time_bucket, altitude_bin, time DESC;
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
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                my_lib.time.now(),
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


def _attempt_reconnect(db_config: DBConfig) -> PgConnection:
    """データベースへの再接続を試行する

    Args:
        db_config: データベース接続設定

    Returns:
        再接続成功時は新しい接続

    Raises:
        TerminationRequestedError: 終了が要求された場合
        ReconnectError: すべての再接続試行に失敗した場合

    """
    for attempt in range(1, MAX_RECONNECT_RETRIES + 1):
        if should_terminate.is_set():
            raise TerminationRequestedError("終了が要求されました")

        logging.warning(
            "再接続を試行します（%d/%d回目、%.1f秒待機）...",
            attempt,
            MAX_RECONNECT_RETRIES,
            RECONNECT_DELAY,
        )
        time.sleep(RECONNECT_DELAY)

        try:
            new_conn = open(
                db_config.host,
                db_config.port,
                db_config.name,
                db_config.user,
                db_config.password,
            )
            logging.info("データベースへの再接続に成功しました（%d回目）", attempt)
            return new_conn
        except Exception:
            logging.exception("再接続に失敗しました（%d回目）", attempt)

    error_message = f"すべての再接続試行（{MAX_RECONNECT_RETRIES}回）に失敗しました"
    logging.error(error_message)
    raise ReconnectError(error_message)


class _StoreState:
    """store_queueの内部状態を管理するクラス"""

    def __init__(self, conn: PgConnection, max_consecutive_errors: int = 3) -> None:
        self.conn = conn
        self.consecutive_errors = 0
        self.max_consecutive_errors = max_consecutive_errors
        self.processed_count = 0
        self.should_stop = False

    def reset_errors(self) -> None:
        self.consecutive_errors = 0

    def increment_errors(self) -> bool:
        """エラーカウントを増加し、上限に達したかを返す"""
        self.consecutive_errors += 1
        return self.consecutive_errors >= self.max_consecutive_errors


def store_queue(  # noqa: PLR0913
    conn: PgConnection,
    measurement_queue: multiprocessing.Queue[MeasurementData],
    liveness_file: pathlib.Path,
    db_config: DBConfig,
    slack_config: my_lib.notify.slack.SlackErrorOnlyConfig | my_lib.notify.slack.SlackEmptyConfig,
    count: int = 0,
) -> None:
    """データベースへのデータ格納を行うワーカー関数

    Args:
        conn: データベース接続
        measurement_queue: 測定データのキュー
        liveness_file: ヘルスチェック用ファイルパス
        db_config: 再接続用のデータベース設定
        slack_config: Slack通知設定
        count: 処理するデータ数（0の場合は無制限）

    """
    logging.info("データ保存ワーカーを開始します")
    state = _StoreState(conn)

    while not state.should_stop and not should_terminate.is_set():
        try:
            _process_one_item(state, measurement_queue, liveness_file)

            if count != 0 and state.processed_count >= count:
                break

        except queue.Empty:
            continue

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            _handle_db_error(state, e, db_config, slack_config)

        except Exception:
            _handle_unexpected_error(state, slack_config)

    with contextlib.suppress(Exception):
        state.conn.close()

    logging.warning("データ保存ワーカーを停止します")


def _process_one_item(
    state: _StoreState,
    measurement_queue: multiprocessing.Queue[MeasurementData],
    liveness_file: pathlib.Path,
) -> None:
    """キューから1件取得してDBに保存する"""
    data = measurement_queue.get(timeout=1)
    insert(state.conn, data)
    my_lib.footprint.update(liveness_file)
    state.reset_errors()
    state.processed_count += 1


def _handle_db_error(
    state: _StoreState,
    error: Exception,
    db_config: DBConfig,
    slack_config: my_lib.notify.slack.SlackErrorOnlyConfig | my_lib.notify.slack.SlackEmptyConfig,
) -> None:
    """データベース接続エラーを処理する"""
    logging.error(
        "データベース接続エラー（連続%d/%d回目）: %s",
        state.consecutive_errors + 1,
        state.max_consecutive_errors,
        str(error),
    )

    if not state.increment_errors():
        return

    # 最大エラー数に達した場合、再接続を試行
    logging.warning("最大連続エラー数に達しました。再接続を開始します...")

    with contextlib.suppress(Exception):
        state.conn.close()

    try:
        state.conn = _attempt_reconnect(db_config)
        state.reset_errors()
    except (ReconnectError, TerminationRequestedError) as e:
        state.should_stop = True
        my_lib.notify.slack.error(
            slack_config,
            "データベース接続エラー",
            f"データベースへの再接続に失敗しました。処理を終了します。\nエラー: {e}",
        )


def _handle_unexpected_error(
    state: _StoreState,
    slack_config: my_lib.notify.slack.SlackErrorOnlyConfig | my_lib.notify.slack.SlackEmptyConfig,
) -> None:
    """予期しないエラーを処理する"""
    logging.exception(
        "データ保存ワーカーで予期しないエラーが発生しました（連続%d/%d回目）",
        state.consecutive_errors + 1,
        state.max_consecutive_errors,
    )

    if state.increment_errors():
        error_message = "最大連続エラー数に達しました。処理を終了します"
        logging.error(error_message)
        state.should_stop = True
        my_lib.notify.slack.error(
            slack_config,
            "データ保存エラー",
            f"{error_message}\n連続エラー回数: {state.consecutive_errors}",
        )


def store_term() -> None:
    should_terminate.set()


def fetch_by_time(  # noqa: PLR0913
    conn: PgConnection,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    distance: float,
    columns: list[str] | None = None,
    max_altitude: float | None = None,
) -> Sequence[dict[str, Any]]:
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
            # データベースはnaive datetime（サーバローカルタイム=JST）で保存されているため、
            # クエリ時もタイムゾーン情報を除去してnaive datetimeとして比較する
            cur.execute(
                query,
                (
                    time_start.replace(tzinfo=None),
                    time_end.replace(tzinfo=None),
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
            # データベースはnaive datetime（サーバローカルタイム=JST）で保存されているため、
            # クエリ時もタイムゾーン情報を除去してnaive datetimeとして比較する
            cur.execute(
                query,
                (
                    time_start.replace(tzinfo=None),
                    time_end.replace(tzinfo=None),
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


def fetch_by_time_numpy(  # noqa: PLR0913
    conn: PgConnection,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    distance: float,
    max_altitude: float | None = None,
    include_wind: bool = False,
) -> dict[str, Any]:
    """
    指定された時間範囲と距離でデータをNumPy配列として取得する（高速版）

    RealDictCursor を使わず、タプル形式で取得してNumPy配列に直接変換することで
    大量データの取得を高速化する。ORDER BY も省略してパフォーマンスを向上。

    Args:
        conn: データベース接続
        time_start: 開始時刻
        time_end: 終了時刻
        distance: 距離フィルタ
        max_altitude: 最大高度フィルタ（Noneの場合はフィルタなし）
        include_wind: 風データを含めるか

    Returns:
        NumPy配列を含む辞書:
        {
            "time": numpy.ndarray,
            "altitude": numpy.ndarray,
            "temperature": numpy.ndarray,
            "wind_x": numpy.ndarray (include_wind=True時のみ),
            "wind_y": numpy.ndarray (include_wind=True時のみ),
            "wind_speed": numpy.ndarray (include_wind=True時のみ),
            "wind_angle": numpy.ndarray (include_wind=True時のみ),
            "count": int,
        }

    """
    import numpy  # noqa: ICN001

    # カラム選択
    if include_wind:
        columns = "time, altitude, temperature, wind_x, wind_y, wind_speed, wind_angle"
        col_count = 7
    else:
        columns = "time, altitude, temperature"
        col_count = 3

    start = time.perf_counter()
    with conn.cursor() as cur:
        # ORDER BY を省略（グラフ描画には時間順序が不要）
        if max_altitude is not None:
            query = (
                f"SELECT {columns} FROM meteorological_data "  # noqa: S608
                f"WHERE time >= %s AND time <= %s AND distance <= %s "
                f"AND altitude IS NOT NULL AND altitude <= %s"
            )
            cur.execute(
                query,
                (
                    time_start.replace(tzinfo=None),
                    time_end.replace(tzinfo=None),
                    distance,
                    max_altitude,
                ),
            )
        else:
            query = (
                f"SELECT {columns} FROM meteorological_data "  # noqa: S608
                f"WHERE time >= %s AND time <= %s AND distance <= %s "
                f"AND altitude IS NOT NULL"
            )
            cur.execute(
                query,
                (
                    time_start.replace(tzinfo=None),
                    time_end.replace(tzinfo=None),
                    distance,
                ),
            )

        # タプル形式で全データ取得
        rows = cur.fetchall()
        row_count = len(rows)

        if row_count == 0:
            result: dict[str, Any] = {
                "time": numpy.array([], dtype="datetime64[us]"),
                "altitude": numpy.array([], dtype=numpy.float64),
                "temperature": numpy.array([], dtype=numpy.float64),
                "count": 0,
            }
            if include_wind:
                result["wind_x"] = numpy.array([], dtype=numpy.float64)
                result["wind_y"] = numpy.array([], dtype=numpy.float64)
                result["wind_speed"] = numpy.array([], dtype=numpy.float64)
                result["wind_angle"] = numpy.array([], dtype=numpy.float64)
            return result

        # タプルのリストからNumPy配列に一括変換
        # 時間、高度、温度を事前確保した配列に直接書き込み
        times = numpy.empty(row_count, dtype="datetime64[us]")
        altitudes = numpy.empty(row_count, dtype=numpy.float64)
        temperatures = numpy.empty(row_count, dtype=numpy.float64)

        if include_wind:
            wind_x = numpy.empty(row_count, dtype=numpy.float64)
            wind_y = numpy.empty(row_count, dtype=numpy.float64)
            wind_speed = numpy.empty(row_count, dtype=numpy.float64)
            wind_angle = numpy.empty(row_count, dtype=numpy.float64)

            for i, row in enumerate(rows):
                times[i] = row[0]
                altitudes[i] = row[1] if row[1] is not None else numpy.nan
                temperatures[i] = row[2] if row[2] is not None else numpy.nan
                wind_x[i] = row[3] if row[3] is not None else numpy.nan
                wind_y[i] = row[4] if row[4] is not None else numpy.nan
                wind_speed[i] = row[5] if row[5] is not None else numpy.nan
                wind_angle[i] = row[6] if row[6] is not None else numpy.nan

            logging.info(
                "Elapsed time: %.2f sec (numpy fetch, %d columns, %s rows)",
                time.perf_counter() - start,
                col_count,
                f"{row_count:,}",
            )
            return {
                "time": times,
                "altitude": altitudes,
                "temperature": temperatures,
                "count": row_count,
                "wind_x": wind_x,
                "wind_y": wind_y,
                "wind_speed": wind_speed,
                "wind_angle": wind_angle,
            }

        for i, row in enumerate(rows):
            times[i] = row[0]
            altitudes[i] = row[1] if row[1] is not None else numpy.nan
            temperatures[i] = row[2] if row[2] is not None else numpy.nan

        logging.info(
            "Elapsed time: %.2f sec (numpy fetch, %d columns, %s rows)",
            time.perf_counter() - start,
            col_count,
            f"{row_count:,}",
        )
        return {
            "time": times,
            "altitude": altitudes,
            "temperature": temperatures,
            "count": row_count,
        }


def fetch_aggregated_numpy(
    conn: PgConnection,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    max_altitude: float | None = None,
    include_wind: bool = False,
) -> dict[str, Any]:
    """
    期間に応じて適切な集約レベルのデータをNumPy配列として取得する（高速版）

    Args:
        conn: データベース接続
        time_start: 開始時刻
        time_end: 終了時刻
        max_altitude: 最大高度フィルタ（Noneの場合はフィルタなし）
        include_wind: 風データを含めるか

    Returns:
        NumPy配列を含む辞書（fetch_by_time_numpy と同じ形式）

    """
    import numpy  # noqa: ICN001

    days = (time_end - time_start).total_seconds() / 86400
    level = get_aggregation_level(days)

    logging.info(
        "Using aggregation level: %s (period: %.1f days, interval: %s, altitude_bin: %dm)",
        level.table,
        days,
        level.time_interval,
        level.altitude_bin,
    )

    # 生データの場合は既存の関数を使用
    if level.table == "meteorological_data":
        return fetch_by_time_numpy(
            conn,
            time_start,
            time_end,
            distance=100,
            max_altitude=max_altitude,
            include_wind=include_wind,
        )

    # マテリアライズドビューが存在するか確認
    view_exists = check_materialized_views_exist(conn)
    if not view_exists.get(level.table, False):
        logging.warning(
            "Materialized view %s does not exist, falling back to raw data",
            level.table,
        )
        return fetch_by_time_numpy(
            conn,
            time_start,
            time_end,
            distance=100,
            max_altitude=max_altitude,
            include_wind=include_wind,
        )

    # カラム選択（time_bucket を time として取得）
    if include_wind:
        columns = "time_bucket AS time, altitude, temperature, wind_x, wind_y, wind_speed, wind_angle"
        col_count = 7
    else:
        columns = "time_bucket AS time, altitude, temperature"
        col_count = 3

    start = time.perf_counter()
    try:
        with conn.cursor() as cur:
            if max_altitude is not None:
                query = f"""
                    SELECT {columns}
                    FROM {level.table}
                    WHERE time_bucket >= %s
                      AND time_bucket <= %s
                      AND altitude <= %s
                """  # noqa: S608
                cur.execute(
                    query,
                    (
                        time_start.replace(tzinfo=None),
                        time_end.replace(tzinfo=None),
                        max_altitude,
                    ),
                )
            else:
                query = f"""
                    SELECT {columns}
                    FROM {level.table}
                    WHERE time_bucket >= %s
                      AND time_bucket <= %s
                """  # noqa: S608
                cur.execute(
                    query,
                    (
                        time_start.replace(tzinfo=None),
                        time_end.replace(tzinfo=None),
                    ),
                )

            rows = cur.fetchall()
            row_count = len(rows)

            if row_count == 0:
                logging.warning(
                    "No data in materialized view %s, falling back to raw data",
                    level.table,
                )
                return fetch_by_time_numpy(
                    conn,
                    time_start,
                    time_end,
                    distance=100,
                    max_altitude=max_altitude,
                    include_wind=include_wind,
                )

            # タプルからNumPy配列に変換
            times = numpy.empty(row_count, dtype="datetime64[us]")
            altitudes = numpy.empty(row_count, dtype=numpy.float64)
            temperatures = numpy.empty(row_count, dtype=numpy.float64)

            if include_wind:
                wind_x = numpy.empty(row_count, dtype=numpy.float64)
                wind_y = numpy.empty(row_count, dtype=numpy.float64)
                wind_speed = numpy.empty(row_count, dtype=numpy.float64)
                wind_angle = numpy.empty(row_count, dtype=numpy.float64)

                for i, row in enumerate(rows):
                    times[i] = row[0]
                    altitudes[i] = row[1] if row[1] is not None else numpy.nan
                    temperatures[i] = row[2] if row[2] is not None else numpy.nan
                    wind_x[i] = row[3] if row[3] is not None else numpy.nan
                    wind_y[i] = row[4] if row[4] is not None else numpy.nan
                    wind_speed[i] = row[5] if row[5] is not None else numpy.nan
                    wind_angle[i] = row[6] if row[6] is not None else numpy.nan

                logging.info(
                    "Elapsed time: %.2f sec (numpy sampled from %s, %d columns, %s rows)",
                    time.perf_counter() - start,
                    level.table,
                    col_count,
                    f"{row_count:,}",
                )
                return {
                    "time": times,
                    "altitude": altitudes,
                    "temperature": temperatures,
                    "count": row_count,
                    "wind_x": wind_x,
                    "wind_y": wind_y,
                    "wind_speed": wind_speed,
                    "wind_angle": wind_angle,
                }

            for i, row in enumerate(rows):
                times[i] = row[0]
                altitudes[i] = row[1] if row[1] is not None else numpy.nan
                temperatures[i] = row[2] if row[2] is not None else numpy.nan

            logging.info(
                "Elapsed time: %.2f sec (numpy sampled from %s, %d columns, %s rows)",
                time.perf_counter() - start,
                level.table,
                col_count,
                f"{row_count:,}",
            )
            return {
                "time": times,
                "altitude": altitudes,
                "temperature": temperatures,
                "count": row_count,
            }

    except psycopg2.Error as e:
        logging.warning(
            "Error fetching from materialized view %s: %s, falling back to raw data",
            level.table,
            str(e),
        )
        return fetch_by_time_numpy(
            conn,
            time_start,
            time_end,
            distance=100,
            max_altitude=max_altitude,
            include_wind=include_wind,
        )


def fetch_latest(
    conn: PgConnection,
    limit: int,
    distance: float | None = None,
    columns: list[str] | None = None,
) -> Sequence[dict[str, Any]]:
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
        DataRangeResult: earliest, latest, countを含むデータ

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
        return DataRangeResult(
            earliest=result["earliest"],
            latest=result["latest"],
            count=result["count"],
        )
    else:
        # データがない場合
        return DataRangeResult(earliest=None, latest=None, count=0)


def get_aggregation_level(days: float) -> AggregationLevel:
    """
    期間に応じた適切な集約レベルを取得する

    Args:
        days: クエリ対象の期間（日数）

    Returns:
        適切な集約レベルの設定

    """
    for level in AGGREGATION_LEVELS:
        if days <= level.max_days:
            return level
    return AGGREGATION_LEVELS[-1]


def fetch_aggregated_by_time(
    conn: PgConnection,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    max_altitude: float | None = None,
) -> Sequence[dict[str, Any]]:
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
        level.table,
        days,
        level.time_interval,
        level.altitude_bin,
    )

    # フォールバック時に使用するカラムリスト
    fallback_columns = [
        "time", "altitude", "temperature",
        "wind_x", "wind_y", "wind_speed", "wind_angle",
    ]

    # 生データの場合は既存の関数を使用
    if level.table == "meteorological_data":
        return fetch_by_time(
            conn,
            time_start,
            time_end,
            distance=100,  # 集約ビューは既にdistance<=100でフィルタ済み
            columns=fallback_columns,
            max_altitude=max_altitude,
        )

    # マテリアライズドビューが存在するか確認
    view_exists = check_materialized_views_exist(conn)
    if not view_exists.get(level.table, False):
        logging.warning(
            "Materialized view %s does not exist, falling back to raw data",
            level.table,
        )
        return fetch_by_time(
            conn,
            time_start,
            time_end,
            distance=100,
            columns=fallback_columns,
            max_altitude=max_altitude,
        )

    # サンプリングデータを取得（実際のデータ点を使用）
    start = time.perf_counter()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if max_altitude is not None:
                query = f"""
                    SELECT
                        time,
                        altitude,
                        temperature,
                        wind_x,
                        wind_y,
                        wind_speed,
                        wind_angle
                    FROM {level.table}
                    WHERE time_bucket >= %s
                      AND time_bucket <= %s
                      AND altitude <= %s
                    ORDER BY time
                """  # noqa: S608
                # データベースはnaive datetime（サーバローカルタイム=JST）で保存されているため、
                # クエリ時もタイムゾーン情報を除去してnaive datetimeとして比較する
                cur.execute(
                    query,
                    (
                        time_start.replace(tzinfo=None),
                        time_end.replace(tzinfo=None),
                        max_altitude,
                    ),
                )
            else:
                query = f"""
                    SELECT
                        time,
                        altitude,
                        temperature,
                        wind_x,
                        wind_y,
                        wind_speed,
                        wind_angle
                    FROM {level.table}
                    WHERE time_bucket >= %s
                      AND time_bucket <= %s
                    ORDER BY time
                """  # noqa: S608
                # データベースはnaive datetime（サーバローカルタイム=JST）で保存されているため、
                # クエリ時もタイムゾーン情報を除去してnaive datetimeとして比較する
                cur.execute(
                    query,
                    (
                        time_start.replace(tzinfo=None),
                        time_end.replace(tzinfo=None),
                    ),
                )

            cur.itersize = 10000
            data = cur.fetchall()

            logging.info(
                "Elapsed time: %.2f sec (sampled data from %s, %s rows)",
                time.perf_counter() - start,
                level.table,
                f"{len(data):,}",
            )

            # データが空の場合は生データにフォールバック
            if not data:
                logging.warning(
                    "No data in materialized view %s, falling back to raw data",
                    level.table,
                )
                return fetch_by_time(
                    conn,
                    time_start,
                    time_end,
                    distance=100,
                    columns=fallback_columns,
                    max_altitude=max_altitude,
                )

            return data

    except psycopg2.Error as e:
        logging.warning(
            "Error fetching from materialized view %s: %s, falling back to raw data",
            level.table,
            str(e),
        )
        return fetch_by_time(
            conn,
            time_start,
            time_end,
            distance=100,
            columns=fallback_columns,
            max_altitude=max_altitude,
        )


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
            row = cur.fetchone()
            exists = row[0] if row else False
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
        except Exception:
            logging.exception("Failed to get stats for %s", view)
            stats[view] = {"error": True}

    return stats


SCHEMA_CONFIG = "config.schema"

if __name__ == "__main__":
    import multiprocessing

    import docopt
    import my_lib.config
    import my_lib.logger

    import modes.receiver
    from modes.config import load_from_dict

    assert __doc__ is not None  # noqa: S101
    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    debug_mode = args["-D"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config_dict = my_lib.config.load(config_file, pathlib.Path(SCHEMA_CONFIG))
    config = load_from_dict(config_dict, pathlib.Path.cwd())

    measurement_queue: multiprocessing.Queue[MeasurementData] = multiprocessing.Queue()

    modes.receiver.start(config, measurement_queue)

    conn = open(
        config.database.host,
        config.database.port,
        config.database.name,
        config.database.user,
        config.database.password,
    )

    db_config = DBConfig(
        host=config.database.host,
        port=config.database.port,
        name=config.database.name,
        user=config.database.user,
        password=config.database.password,
    )

    store_queue(
        conn, measurement_queue, config.liveness.file.collector, db_config, config.slack
    )
