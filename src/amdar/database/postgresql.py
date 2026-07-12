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
import pathlib
import queue
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import my_lib.footprint
import my_lib.notify.slack
import my_lib.time
import numpy as np
import psycopg2
import psycopg2.errors
import psycopg2.extensions
import psycopg2.extras
from numpy.typing import NDArray

import amdar.constants
from amdar.config import DatabaseConfig
from amdar.constants import DEFAULT_DISTANCE_KM, get_db_schema_path, sanitize_columns
from amdar.core.types import MethodType, WindData

if TYPE_CHECKING:
    import multiprocessing
    from collections.abc import Sequence

    from psycopg2.extensions import connection as PgConnection


class ReconnectError(Exception):
    """データベース再接続に失敗した場合の例外"""


class TerminationRequestedError(Exception):
    """終了が要求された場合の例外"""


# スキーマファイルのパス
_SCHEMA_FILE = get_db_schema_path("postgres.schema")

# PostgreSQL SQLSTATE: invalid_catalog_name（データベースが存在しない）
_PGCODE_INVALID_CATALOG_NAME = "3D000"


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
    method: MethodType = amdar.constants.MODE_S_METHOD


@dataclass(frozen=True)
class DataRangeResult:
    """データ範囲クエリの結果"""

    earliest: datetime.datetime | None
    latest: datetime.datetime | None
    count: int


@dataclass(frozen=True)
class MethodLastReceived:
    """受信方式別の最終受信時刻"""

    mode_s: datetime.datetime | None
    vdl2: datetime.datetime | None


@dataclass(frozen=True)
class MethodObservationCounts:
    """受信方式別の観測数"""

    mode_s: int
    vdl2: int


@dataclass(frozen=True)
class AggregateRowCounts:
    """集約テーブルの行数"""

    halfhourly_altitude_grid: int
    threehour_altitude_grid: int

    def to_dict(self) -> dict[str, int]:
        """API レスポンス用に辞書に変換する"""
        return {
            "halfhourly_altitude_grid": self.halfhourly_altitude_grid,
            "threehour_altitude_grid": self.threehour_altitude_grid,
        }


@dataclass(frozen=True)
class ReceiverQualityResult:
    """受信品質スナップショット（/api/metrics, /api/receiver-quality 用）"""

    last_hour: MethodObservationCounts
    last_24h: MethodObservationCounts
    last_received: MethodLastReceived
    aggregate_rows: AggregateRowCounts


@dataclass(frozen=True)
class AggregationLevel:
    """集約レベルの設定"""

    table: str
    time_interval: str
    altitude_bin: int
    max_days: int


@dataclass(frozen=True)
class MaterializedViewsStatus:
    """集約テーブル（旧マテリアライズドビュー）の存在状態"""

    halfhourly_altitude_grid: bool = False
    threehour_altitude_grid: bool = False

    def get(self, table_name: str) -> bool:
        """テーブル名で存在状態を取得する

        Args:
            table_name: テーブル名 ("halfhourly_altitude_grid" または "threehour_altitude_grid")

        Returns:
            テーブルが存在する場合 True
        """
        if table_name == "halfhourly_altitude_grid":
            return self.halfhourly_altitude_grid
        if table_name == "threehour_altitude_grid":
            return self.threehour_altitude_grid
        return False


@dataclass
class MaterializedViewStats:
    """集約テーブルの統計情報"""

    row_count: int
    earliest: datetime.datetime | None
    latest: datetime.datetime | None
    error: bool = False

    def to_dict(self) -> dict[str, Any]:
        """API レスポンス用に辞書に変換する"""
        if self.error:
            return {"error": True}
        return {
            "row_count": self.row_count,
            "earliest": self.earliest,
            "latest": self.latest,
        }


@dataclass
class AllMaterializedViewStats:
    """全集約テーブルの統計情報"""

    halfhourly_altitude_grid: MaterializedViewStats
    threehour_altitude_grid: MaterializedViewStats

    def to_dict(self) -> dict[str, dict[str, Any]]:
        """API レスポンス用に辞書に変換する"""
        return {
            "halfhourly_altitude_grid": self.halfhourly_altitude_grid.to_dict(),
            "threehour_altitude_grid": self.threehour_altitude_grid.to_dict(),
        }


@dataclass
class MaterializedViewRefreshResult:
    """集約テーブル更新結果

    各テーブルの更新にかかった時間（秒）を保持する。
    -1 はエラーを示す。
    """

    halfhourly_altitude_grid: float
    threehour_altitude_grid: float

    def to_dict(self) -> dict[str, float]:
        """API レスポンス用に辞書に変換する"""
        return {
            "halfhourly_altitude_grid": self.halfhourly_altitude_grid,
            "threehour_altitude_grid": self.threehour_altitude_grid,
        }


@dataclass
class NumpyFetchResult:
    """NumPy 配列形式のデータ取得結果

    fetch_by_time_numpy / fetch_aggregated_numpy から返される形式。
    グラフ描画に必要なデータを保持する。
    """

    time: NDArray[np.datetime64]
    altitude: NDArray[np.float64]
    temperature: NDArray[np.float64]
    count: int
    wind_x: NDArray[np.float64] | None = None
    wind_y: NDArray[np.float64] | None = None
    wind_speed: NDArray[np.float64] | None = None
    wind_angle: NDArray[np.float64] | None = None


def _to_local_wall_time(dt: datetime.datetime) -> datetime.datetime:
    """aware datetime をローカルタイム（JST）の naive datetime に変換する

    numpy の datetime64 はタイムゾーンを保持できないため、
    グラフ表示用にローカルタイムの壁時計時刻へ変換してから格納する
    （TIMESTAMPTZ 移行前の naive JST 格納時代と同じ表示になる）。
    """
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(my_lib.time.get_zoneinfo()).replace(tzinfo=None)


def _convert_rows_to_numpy_arrays(
    rows: Sequence[tuple[Any, ...]],
    include_wind: bool = False,
) -> NumpyFetchResult:
    """行データをNumPy配列に変換する共通関数

    time はローカルタイム（JST）の壁時計時刻として datetime64 に格納される。

    Args:
        rows: データベースから取得した行のリスト
        include_wind: 風データを含めるか

    Returns:
        NumpyFetchResult: NumPy配列形式のデータ

    """
    row_count = len(rows)
    if row_count == 0:
        return NumpyFetchResult(
            time=np.array([], dtype="datetime64[us]"),
            altitude=np.array([], dtype=np.float64),
            temperature=np.array([], dtype=np.float64),
            count=0,
            wind_x=np.array([], dtype=np.float64) if include_wind else None,
            wind_y=np.array([], dtype=np.float64) if include_wind else None,
            wind_speed=np.array([], dtype=np.float64) if include_wind else None,
            wind_angle=np.array([], dtype=np.float64) if include_wind else None,
        )

    # タプルのリストからNumPy配列に一括変換
    # 時間、高度、温度を事前確保した配列に直接書き込み
    times = np.empty(row_count, dtype="datetime64[us]")
    altitudes = np.empty(row_count, dtype=np.float64)
    temperatures = np.empty(row_count, dtype=np.float64)

    if include_wind:
        wind_x = np.empty(row_count, dtype=np.float64)
        wind_y = np.empty(row_count, dtype=np.float64)
        wind_speed = np.empty(row_count, dtype=np.float64)
        wind_angle = np.empty(row_count, dtype=np.float64)

        for i, row in enumerate(rows):
            times[i] = _to_local_wall_time(row[0])
            altitudes[i] = row[1] if row[1] is not None else np.nan
            temperatures[i] = row[2] if row[2] is not None else np.nan
            wind_x[i] = row[3] if row[3] is not None else np.nan
            wind_y[i] = row[4] if row[4] is not None else np.nan
            wind_speed[i] = row[5] if row[5] is not None else np.nan
            wind_angle[i] = row[6] if row[6] is not None else np.nan

        return NumpyFetchResult(
            time=times,
            altitude=altitudes,
            temperature=temperatures,
            count=row_count,
            wind_x=wind_x,
            wind_y=wind_y,
            wind_speed=wind_speed,
            wind_angle=wind_angle,
        )

    for i, row in enumerate(rows):
        times[i] = _to_local_wall_time(row[0])
        altitudes[i] = row[1] if row[1] is not None else np.nan
        temperatures[i] = row[2] if row[2] is not None else np.nan

    return NumpyFetchResult(
        time=times,
        altitude=altitudes,
        temperature=temperatures,
        count=row_count,
    )


# 期間に応じたサンプリングレベルの定義
# 長期間では時間×高度帯から代表点を1つ選ぶことでデータ量を削減しつつ品質を維持
AGGREGATION_LEVELS: list[AggregationLevel] = [
    # 14日以内は生データ（高精度分析用）
    AggregationLevel(table="meteorological_data", time_interval="raw", altitude_bin=0, max_days=14),
    # 14-90日は30分×250m帯から代表点をサンプリング（中期分析用）
    AggregationLevel(table="halfhourly_altitude_grid", time_interval="30 min", altitude_bin=250, max_days=90),
    # 90日以上は3時間×250m帯から代表点をサンプリング（長期トレンド用）
    AggregationLevel(
        table="threehour_altitude_grid", time_interval="3 hours", altitude_bin=250, max_days=9999
    ),
]


# 有効なカラム名（SQLインジェクション対策用バリデーション）
VALID_METEOROLOGICAL_COLUMNS: tuple[str, ...] = (
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
)

_should_terminate = threading.Event()


# ===============================
# 集約テーブル定義（増分更新）
# ===============================

# バケット計算の基準タイムゾーン（既存データとのバケット境界の連続性のため JST 固定）
_BUCKET_TZ = amdar.constants.AGGREGATE_BUCKET_TIMEZONE


def _halfhourly_bucket_expr() -> str:
    """30分バケットの time_bucket 計算式（JST 基準、timestamptz を返す）"""
    return (
        f"(date_trunc('hour', time AT TIME ZONE '{_BUCKET_TZ}') "
        f"+ (floor(EXTRACT(minute FROM time AT TIME ZONE '{_BUCKET_TZ}') / 30) "
        f"* interval '30 minutes')) AT TIME ZONE '{_BUCKET_TZ}'"
    )


def _threehour_bucket_expr() -> str:
    """3時間バケットの time_bucket 計算式（JST 基準、timestamptz を返す）"""
    return (
        f"(date_trunc('hour', time AT TIME ZONE '{_BUCKET_TZ}') "
        f"- (mod(EXTRACT(hour FROM time AT TIME ZONE '{_BUCKET_TZ}')::int, 3) "
        f"* interval '1 hour')) AT TIME ZONE '{_BUCKET_TZ}'"
    )


@dataclass(frozen=True)
class AggregateTableSpec:
    """集約テーブルの定義

    Attributes:
        table: テーブル名
        bucket_expr: time_bucket を計算する SQL 式
        bucket_seconds: バケット幅（秒）
        refresh_window_seconds: 増分更新ウィンドウ（秒）
    """

    table: str
    bucket_expr: str
    bucket_seconds: int
    refresh_window_seconds: int


_AGGREGATE_TABLE_SPECS: tuple[AggregateTableSpec, ...] = (
    AggregateTableSpec(
        table="halfhourly_altitude_grid",
        bucket_expr=_halfhourly_bucket_expr(),
        bucket_seconds=amdar.constants.AGGREGATE_HALFHOURLY_BUCKET_SECONDS,
        refresh_window_seconds=amdar.constants.AGGREGATE_HALFHOURLY_REFRESH_WINDOW_SECONDS,
    ),
    AggregateTableSpec(
        table="threehour_altitude_grid",
        bucket_expr=_threehour_bucket_expr(),
        bucket_seconds=amdar.constants.AGGREGATE_THREEHOUR_BUCKET_SECONDS,
        refresh_window_seconds=amdar.constants.AGGREGATE_THREEHOUR_REFRESH_WINDOW_SECONDS,
    ),
)

# 集約テーブル名の一覧
AGGREGATE_TABLES: tuple[str, ...] = tuple(spec.table for spec in _AGGREGATE_TABLE_SPECS)

# 集約テーブルのカラム（INSERT / SELECT の順序）
_AGGREGATE_COLUMNS = (
    "time_bucket, altitude_bin, time, altitude, temperature, wind_x, wind_y, wind_speed, wind_angle"
)


def _to_naive_datetime(dt: datetime.datetime) -> datetime.datetime:
    """DEPRECATED: aware datetime をそのまま返すシム

    time カラムの TIMESTAMPTZ 移行により naive 変換は不要になった。
    aware datetime をそのままプレースホルダに渡せばよい。
    呼び出し側の除去が完了するまでの互換用として残している。

    Args:
        dt: タイムゾーン付きの datetime

    Returns:
        引数をそのまま返す（変換しない）
    """
    return dt


def _is_database_missing_error(error: psycopg2.OperationalError, database: str) -> bool:
    """接続エラーが「データベースが存在しない」ことを示すか判定する

    SQLSTATE 3D000 (invalid_catalog_name) で判定する。ただし psycopg2 は
    接続時のエラーに pgcode を設定しないため、その場合はサーバーの
    FATAL メッセージ（database "<name>" does not exist）で判定する。

    Args:
        error: 接続時に発生した OperationalError
        database: 接続しようとしたデータベース名

    Returns:
        データベースが存在しないエラーの場合 True
    """
    if getattr(error, "pgcode", None) is not None:
        return error.pgcode == _PGCODE_INVALID_CATALOG_NAME
    return f'database "{database}" does not exist' in str(error)


def open(
    host: str, port: int, database: str, user: str, password: str, apply_schema: bool = True
) -> PgConnection:
    """データベースに接続する

    データベースが存在しない場合は作成する。

    Args:
        host: ホスト名
        port: ポート番号
        database: データベース名
        user: ユーザー名
        password: パスワード
        apply_schema: True の場合、接続時にスキーマファイル（DDL）を適用する

    Returns:
        データベース接続
    """
    connection_params: dict[str, Any] = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        # TCPキープアライブ設定（プロキシのアイドルタイムアウト対策）
        "keepalives": 1,  # キープアライブを有効化
        "keepalives_idle": 30,  # 30秒アイドル後にキープアライブ送信開始
        "keepalives_interval": 10,  # 10秒間隔でキープアライブ送信
        "keepalives_count": 3,  # 3回応答がなければ切断
    }

    try:
        conn = psycopg2.connect(**connection_params)
    except psycopg2.OperationalError as e:
        if not _is_database_missing_error(e, database):
            raise

        # postgres データベースに接続してデータベースを作成
        admin_params = connection_params.copy()
        admin_params["database"] = "postgres"

        admin_conn = psycopg2.connect(**admin_params)
        try:
            admin_conn.autocommit = True
            with admin_conn.cursor() as cur:
                # データベース名をエスケープしてSQLインジェクションを防ぐ
                cur.execute(f"CREATE DATABASE {psycopg2.extensions.quote_ident(database, admin_conn)}")
        finally:
            admin_conn.close()

        # 新しく作成したデータベースに接続
        conn = psycopg2.connect(**connection_params)

    conn.autocommit = True

    if apply_schema:
        # 外部スキーマファイルからスキーマを読み込んで実行
        _execute_schema(conn)

    return conn


def apply_schema(conn: PgConnection) -> None:
    """スキーマファイル（schema/postgres.schema）を適用する

    open(apply_schema=False) で接続した場合などに、明示的にスキーマを適用するために使用する。

    Args:
        conn: データベース接続
    """
    _execute_schema(conn)


def _execute_schema(conn: PgConnection) -> None:
    """外部スキーマファイルを読み込んで実行"""
    schema_sql = _SCHEMA_FILE.read_text(encoding="utf-8")

    with conn.cursor() as cur:
        # スキーマファイル内の各ステートメントを実行
        for raw_statement in schema_sql.split(";"):
            # コメント行を除去してから処理
            lines = raw_statement.split("\n")
            non_comment_lines = [line for line in lines if not line.strip().startswith("--")]
            statement = "\n".join(non_comment_lines).strip()
            if statement:
                cur.execute(statement)


def _insert(conn: PgConnection, data: MeasurementData) -> None:
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


def _attempt_reconnect(db_config: DatabaseConfig) -> PgConnection:
    """データベースへの再接続を試行する

    Args:
        db_config: データベース接続設定

    Returns:
        再接続成功時は新しい接続

    Raises:
        TerminationRequestedError: 終了が要求された場合
        ReconnectError: すべての再接続試行に失敗した場合

    """
    for attempt in range(1, amdar.constants.DB_MAX_RECONNECT_RETRIES + 1):
        if _should_terminate.is_set():
            raise TerminationRequestedError("終了が要求されました")

        logging.warning(
            "再接続を試行します（%d/%d回目、%.1f秒待機）...",
            attempt,
            amdar.constants.DB_MAX_RECONNECT_RETRIES,
            amdar.constants.DB_RECONNECT_DELAY_SECONDS,
        )
        time.sleep(amdar.constants.DB_RECONNECT_DELAY_SECONDS)

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

    error_message = f"すべての再接続試行（{amdar.constants.DB_MAX_RECONNECT_RETRIES}回）に失敗しました"
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
        self.pending_data: MeasurementData | None = None  # DBエラー時に保持するデータ

    def reset_errors(self) -> None:
        self.consecutive_errors = 0

    def increment_errors(self) -> bool:
        """エラーカウントを増加し、上限に達したかを返す"""
        self.consecutive_errors += 1
        return self.consecutive_errors >= self.max_consecutive_errors


def store_queue(
    conn: PgConnection,
    measurement_queue: multiprocessing.Queue[MeasurementData],
    liveness_file: pathlib.Path,
    db_config: DatabaseConfig,
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

    while not state.should_stop and not _should_terminate.is_set():
        try:
            _process_one_item(state, measurement_queue, liveness_file)

            if count != 0 and state.processed_count >= count:
                break

        except queue.Empty:
            continue

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            _handle_db_error(state, e, db_config, slack_config)

        except psycopg2.Error as e:
            # 接続系以外の DB エラー（DataError 等）はデータ起因の恒久エラーであり、
            # 再試行しても成功しないため当該レコードを破棄して処理を継続する
            _handle_data_error(state, e)

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
    """キューから1件取得してDBに保存する

    ペンディングデータがある場合は優先的に処理し、
    DBエラー時はデータをペンディングとして保持することでデータロスを防ぐ。
    """
    # ペンディングデータがあれば優先処理（キューから取り出さない）
    data = state.pending_data if state.pending_data is not None else measurement_queue.get(timeout=1)

    # INSERT 前にペンディングとして保持
    state.pending_data = data

    # INSERT 実行（エラー時は例外がスローされ、pending_data は保持されたまま）
    _insert(state.conn, data)

    # 成功したらペンディングをクリア
    state.pending_data = None
    my_lib.footprint.update(liveness_file)
    state.reset_errors()
    state.processed_count += 1


def _handle_db_error(
    state: _StoreState,
    error: Exception,
    db_config: DatabaseConfig,
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


def _handle_data_error(state: _StoreState, error: Exception) -> None:
    """データ起因の DB エラーを処理する

    同一レコードの再試行では回復しないため、当該レコードを破棄して
    後続データの処理を継続する（保持し続けるとワーカー全体が停止し、
    以降の観測データがすべて失われる）。
    """
    logging.error(
        "データ起因のDBエラーのためレコードを破棄します: %s (data=%s)",
        str(error),
        state.pending_data,
    )
    state.pending_data = None
    # 失敗したトランザクションが残っている場合に備えてロールバック
    with contextlib.suppress(Exception):
        state.conn.rollback()


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

    # 同一レコードの無限再試行（poison message）を防ぐため当該レコードは破棄する。
    # 連続エラーカウントは維持し、システム起因の連続失敗時は従来どおり停止する。
    if state.pending_data is not None:
        logging.error("処理に失敗したレコードを破棄します: %s", state.pending_data)
        state.pending_data = None
    with contextlib.suppress(Exception):
        state.conn.rollback()

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
    _should_terminate.set()


def _build_raw_data_filter(
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    distance: float,
    max_altitude: float | None,
) -> tuple[str, list[Any]]:
    """グラフ用生データクエリの WHERE 句とパラメータを構築する

    集約テーブルと同じ品質フィルタ（温度閾値、高度 0〜13000m）を適用し、
    期間 14 日前後（生データ / 集約テーブル）で表示データの条件が変わらないようにする。

    Args:
        time_start: 開始時刻（aware datetime）
        time_end: 終了時刻（aware datetime）
        distance: 距離フィルタ
        max_altitude: 最大高度フィルタ（None の場合は品質フィルタの上限のみ）

    Returns:
        (WHERE 句, パラメータリスト)
    """
    altitude_max = (
        amdar.constants.GRAPH_ALT_MAX
        if max_altitude is None
        else min(max_altitude, amdar.constants.GRAPH_ALT_MAX)
    )
    where = (
        "time >= %s AND time <= %s AND distance <= %s "
        "AND altitude IS NOT NULL AND altitude >= %s AND altitude <= %s "
        "AND temperature > %s"
    )
    params: list[Any] = [
        time_start,
        time_end,
        distance,
        amdar.constants.GRAPH_ALT_MIN,
        altitude_max,
        amdar.constants.GRAPH_TEMPERATURE_THRESHOLD,
    ]
    return where, params


def _build_aggregate_filter(
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    max_altitude: float | None,
) -> tuple[str, list[Any]]:
    """集約テーブルクエリの WHERE 句とパラメータを構築する"""
    where = "time_bucket >= %s AND time_bucket <= %s"
    params: list[Any] = [time_start, time_end]
    if max_altitude is not None:
        where += " AND altitude <= %s"
        params.append(max_altitude)
    return where, params


def _warn_if_row_limit_reached(row_count: int, context: str) -> None:
    """行数が上限に達した場合に警告を出す"""
    if row_count >= amdar.constants.RAW_FETCH_ROW_LIMIT:
        logging.warning(
            "行数が上限（%s 行）に達したため、結果が切り詰められている可能性があります（%s）",
            f"{amdar.constants.RAW_FETCH_ROW_LIMIT:,}",
            context,
        )


def fetch_by_time(
    conn: PgConnection,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    distance: float,
    columns: list[str] | None = None,
    max_altitude: float | None = None,
) -> Sequence[dict[str, Any]]:
    """
    指定された時間範囲と距離でデータを取得する

    集約テーブルと同じ品質フィルタ（温度閾値、高度 0〜13000m）を適用する。
    取得行数は RAW_FETCH_ROW_LIMIT で制限される。

    Args:
        conn: データベース接続
        time_start: 開始時刻（aware datetime）
        time_end: 終了時刻（aware datetime）
        distance: 距離フィルタ
        columns: 取得するカラムのリスト。Noneの場合はデフォルト['time', 'altitude', 'temperature', 'distance']
        max_altitude: 最大高度フィルタ（Noneの場合はフィルタなし）

    Returns:
        取得されたデータのリスト（time は aware datetime）

    """
    if columns is None:
        columns = ["time", "altitude", "temperature", "distance"]

    # カラム名をサニタイズ（SQLインジェクション対策）
    columns_str = sanitize_columns(columns, VALID_METEOROLOGICAL_COLUMNS)

    where, params = _build_raw_data_filter(time_start, time_end, distance, max_altitude)

    start = time.perf_counter()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        query = (
            f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
            f"WHERE {where} ORDER BY time LIMIT %s"
        )
        cur.execute(query, (*params, amdar.constants.RAW_FETCH_ROW_LIMIT))
        data = cur.fetchall()

        _warn_if_row_limit_reached(len(data), "fetch_by_time")

        logging.info(
            "Elapsed time: %.2f sec (selected %d columns, %s rows)",
            time.perf_counter() - start,
            columns_str.count(",") + 1,
            f"{len(data):,}",
        )

        return data


def fetch_by_time_numpy(
    conn: PgConnection,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    distance: float,
    max_altitude: float | None = None,
    include_wind: bool = False,
) -> NumpyFetchResult:
    """
    指定された時間範囲と距離でデータをNumPy配列として取得する（高速版）

    RealDictCursor を使わず、タプル形式で取得してNumPy配列に直接変換することで
    大量データの取得を高速化する。ORDER BY も省略してパフォーマンスを向上。
    集約テーブルと同じ品質フィルタ（温度閾値、高度 0〜13000m）を適用する。
    取得行数は RAW_FETCH_ROW_LIMIT で制限される。

    Args:
        conn: データベース接続
        time_start: 開始時刻（aware datetime）
        time_end: 終了時刻（aware datetime）
        distance: 距離フィルタ
        max_altitude: 最大高度フィルタ（Noneの場合はフィルタなし）
        include_wind: 風データを含めるか

    Returns:
        NumpyFetchResult: NumPy配列形式のデータ取得結果

    """
    # カラム選択
    if include_wind:
        columns = "time, altitude, temperature, wind_x, wind_y, wind_speed, wind_angle"
        col_count = 7
    else:
        columns = "time, altitude, temperature"
        col_count = 3

    where, params = _build_raw_data_filter(time_start, time_end, distance, max_altitude)

    start = time.perf_counter()
    with conn.cursor() as cur:
        # ORDER BY を省略（グラフ描画には時間順序が不要）
        query = (
            f"SELECT {columns} FROM meteorological_data "  # noqa: S608
            f"WHERE {where} LIMIT %s"
        )
        cur.execute(query, (*params, amdar.constants.RAW_FETCH_ROW_LIMIT))

        # タプル形式で全データ取得
        rows = cur.fetchall()

        _warn_if_row_limit_reached(len(rows), "fetch_by_time_numpy")

        result = _convert_rows_to_numpy_arrays(rows, include_wind)

        logging.info(
            "Elapsed time: %.2f sec (numpy fetch, %d columns, %s rows)",
            time.perf_counter() - start,
            col_count,
            f"{result.count:,}",
        )

        return result


def fetch_aggregated_numpy(
    conn: PgConnection,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    max_altitude: float | None = None,
    include_wind: bool = False,
) -> NumpyFetchResult:
    """
    期間に応じて適切な集約レベルのデータをNumPy配列として取得する（高速版）

    NOTE: 集約テーブル使用時、結果の time 列には time_bucket（バケット開始時刻）が
    入る（fetch_aggregated_by_time は代表点の実測定時刻を返す点が異なる）。

    Args:
        conn: データベース接続
        time_start: 開始時刻（aware datetime）
        time_end: 終了時刻（aware datetime）
        max_altitude: 最大高度フィルタ（Noneの場合はフィルタなし）
        include_wind: 風データを含めるか

    Returns:
        NumpyFetchResult: NumPy配列形式のデータ取得結果

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

    def _fallback_to_raw() -> NumpyFetchResult:
        return fetch_by_time_numpy(
            conn,
            time_start,
            time_end,
            distance=DEFAULT_DISTANCE_KM,
            max_altitude=max_altitude,
            include_wind=include_wind,
        )

    # 生データの場合は既存の関数を使用
    if level.table == "meteorological_data":
        return _fallback_to_raw()

    # 集約テーブルが存在するか確認
    table_exists = check_materialized_views_exist(conn)
    if not table_exists.get(level.table):
        logging.warning("Aggregate table %s does not exist, falling back to raw data", level.table)
        return _fallback_to_raw()

    # カラム選択（time_bucket を time として取得）
    if include_wind:
        columns = "time_bucket AS time, altitude, temperature, wind_x, wind_y, wind_speed, wind_angle"
        col_count = 7
    else:
        columns = "time_bucket AS time, altitude, temperature"
        col_count = 3

    where, params = _build_aggregate_filter(time_start, time_end, max_altitude)

    start = time.perf_counter()
    try:
        with conn.cursor() as cur:
            query = f"SELECT {columns} FROM {level.table} WHERE {where}"  # noqa: S608
            cur.execute(query, params)

            rows = cur.fetchall()

            if len(rows) == 0:
                logging.warning("No data in aggregate table %s, falling back to raw data", level.table)
                return _fallback_to_raw()

            result = _convert_rows_to_numpy_arrays(rows, include_wind)

            logging.info(
                "Elapsed time: %.2f sec (numpy sampled from %s, %d columns, %s rows)",
                time.perf_counter() - start,
                level.table,
                col_count,
                f"{result.count:,}",
            )

            return result

    except psycopg2.Error as e:
        logging.warning(
            "Error fetching from aggregate table %s: %s, falling back to raw data",
            level.table,
            str(e),
        )
        return _fallback_to_raw()


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
    columns_str = sanitize_columns(columns, VALID_METEOROLOGICAL_COLUMNS)

    start = time.perf_counter()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # 距離フィルタの有無で条件分岐
        if distance is not None:
            query = (
                f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
                f"WHERE altitude IS NOT NULL AND temperature IS NOT NULL "
                f"AND temperature > %s AND distance <= %s "
                f"ORDER BY time DESC LIMIT %s"
            )
            cur.execute(query, (amdar.constants.GRAPH_TEMPERATURE_THRESHOLD, distance, limit))
        else:
            query = (
                f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
                f"WHERE altitude IS NOT NULL AND temperature IS NOT NULL "
                f"AND temperature > %s "
                f"ORDER BY time DESC LIMIT %s"
            )
            cur.execute(query, (amdar.constants.GRAPH_TEMPERATURE_THRESHOLD, limit))

        data = cur.fetchall()

        logging.info(
            "Elapsed time: %.2f sec (selected %d columns, %s rows)",
            time.perf_counter() - start,
            columns_str.count(",") + 1,
            f"{len(data):,}",
        )

        return data


# fetch_data_range の TTL キャッシュ
# COUNT(*) の全件スキャンを毎回実行しないため、一定時間結果を保持する
_data_range_cache_lock = threading.Lock()
_data_range_cache: DataRangeResult | None = None
_data_range_cache_time: float = 0.0


def _clear_data_range_cache() -> None:
    """fetch_data_range のキャッシュをクリアする（主にテスト用）"""
    global _data_range_cache, _data_range_cache_time
    with _data_range_cache_lock:
        _data_range_cache = None
        _data_range_cache_time = 0.0


def fetch_data_range(conn: PgConnection) -> DataRangeResult:
    """
    データベースの最古・最新データの日時とレコード数を取得する

    COUNT(*) の全件スキャンを伴うため、結果は DATA_RANGE_CACHE_TTL_SECONDS の間
    モジュールレベルでキャッシュされる。

    Args:
        conn: データベース接続

    Returns:
        DataRangeResult: earliest, latest, countを含むデータ

    """
    global _data_range_cache, _data_range_cache_time

    with _data_range_cache_lock:
        if (
            _data_range_cache is not None
            and (time.time() - _data_range_cache_time) < amdar.constants.DATA_RANGE_CACHE_TTL_SECONDS
        ):
            return _data_range_cache

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
        data_range = DataRangeResult(
            earliest=result["earliest"],
            latest=result["latest"],
            count=result["count"],
        )
    else:
        # データがない場合
        data_range = DataRangeResult(earliest=None, latest=None, count=0)

    with _data_range_cache_lock:
        _data_range_cache = data_range
        _data_range_cache_time = time.time()

    return data_range


def fetch_last_received_by_method(conn: PgConnection) -> MethodLastReceived:
    """
    受信方式（Mode S / VDL2）別の最終受信時刻を取得する

    Args:
        conn: データベース接続

    Returns:
        MethodLastReceived: mode_s, vdl2 の最終受信時刻

    """
    query = """
    SELECT
        method,
        MAX(time) as last_received
    FROM meteorological_data
    WHERE method IN ('mode-s', 'vdl2')
    GROUP BY method
    """

    start = time.perf_counter()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        results = cur.fetchall()

    logging.info(
        "Elapsed time: %.2f sec (last received by method query)",
        time.perf_counter() - start,
    )

    mode_s_time = None
    vdl2_time = None

    for row in results:
        if row["method"] == amdar.constants.MODE_S_METHOD:
            mode_s_time = row["last_received"]
        elif row["method"] == amdar.constants.VDL2_METHOD:
            vdl2_time = row["last_received"]

    return MethodLastReceived(mode_s=mode_s_time, vdl2=vdl2_time)


def fetch_observation_counts_by_method(conn: PgConnection, hours: int) -> MethodObservationCounts:
    """
    直近 N 時間の観測数を受信方式（Mode S / VDL2）別に取得する

    Args:
        conn: データベース接続
        hours: 集計対象の直近時間数

    Returns:
        MethodObservationCounts: mode_s, vdl2 の観測数

    """
    since = my_lib.time.now() - datetime.timedelta(hours=hours)

    query = """
    SELECT
        method,
        COUNT(*) as count
    FROM meteorological_data
    WHERE time >= %s AND method IN ('mode-s', 'vdl2')
    GROUP BY method
    """

    start = time.perf_counter()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, (since,))
        results = cur.fetchall()

    logging.info(
        "Elapsed time: %.2f sec (observation counts query, last %d hours)",
        time.perf_counter() - start,
        hours,
    )

    mode_s_count = 0
    vdl2_count = 0

    for row in results:
        if row["method"] == amdar.constants.MODE_S_METHOD:
            mode_s_count = row["count"]
        elif row["method"] == amdar.constants.VDL2_METHOD:
            vdl2_count = row["count"]

    return MethodObservationCounts(mode_s=mode_s_count, vdl2=vdl2_count)


# fetch_receiver_quality の TTL キャッシュ
# /api/metrics のスクレイプ毎に集計クエリを実行しないため、一定時間結果を保持する
_receiver_quality_cache_lock = threading.Lock()
_receiver_quality_cache: ReceiverQualityResult | None = None
_receiver_quality_cache_time: float = 0.0


def _clear_receiver_quality_cache() -> None:
    """fetch_receiver_quality のキャッシュをクリアする（主にテスト用）"""
    global _receiver_quality_cache, _receiver_quality_cache_time
    with _receiver_quality_cache_lock:
        _receiver_quality_cache = None
        _receiver_quality_cache_time = 0.0


def fetch_receiver_quality(conn: PgConnection) -> ReceiverQualityResult:
    """
    受信品質スナップショット（観測数・最終受信時刻・集約テーブル行数）を取得する

    集計クエリを伴うため、結果は RECEIVER_QUALITY_CACHE_TTL_SECONDS の間
    モジュールレベルでキャッシュされる。

    Args:
        conn: データベース接続

    Returns:
        ReceiverQualityResult: 受信品質スナップショット

    """
    global _receiver_quality_cache, _receiver_quality_cache_time

    with _receiver_quality_cache_lock:
        if (
            _receiver_quality_cache is not None
            and (time.time() - _receiver_quality_cache_time)
            < amdar.constants.RECEIVER_QUALITY_CACHE_TTL_SECONDS
        ):
            return _receiver_quality_cache

    last_hour = fetch_observation_counts_by_method(conn, hours=1)
    last_24h = fetch_observation_counts_by_method(conn, hours=24)
    last_received = fetch_last_received_by_method(conn)
    view_stats = get_materialized_view_stats(conn)

    result = ReceiverQualityResult(
        last_hour=last_hour,
        last_24h=last_24h,
        last_received=last_received,
        aggregate_rows=AggregateRowCounts(
            halfhourly_altitude_grid=view_stats.halfhourly_altitude_grid.row_count,
            threehour_altitude_grid=view_stats.threehour_altitude_grid.row_count,
        ),
    )

    with _receiver_quality_cache_lock:
        _receiver_quality_cache = result
        _receiver_quality_cache_time = time.time()

    return result


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

    NOTE: 集約テーブル使用時、結果の time 列には代表点の実測定時刻が入る
    （fetch_aggregated_numpy は time_bucket を time として返す点が異なる）。

    Args:
        conn: データベース接続
        time_start: 開始時刻（aware datetime）
        time_end: 終了時刻（aware datetime）
        max_altitude: 最大高度フィルタ（Noneの場合はフィルタなし）

    Returns:
        取得されたデータのリスト（生データ形式に変換済み、time は aware datetime）

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
        "time",
        "altitude",
        "temperature",
        "wind_x",
        "wind_y",
        "wind_speed",
        "wind_angle",
    ]

    def _fallback_to_raw() -> Sequence[dict[str, Any]]:
        return fetch_by_time(
            conn,
            time_start,
            time_end,
            distance=DEFAULT_DISTANCE_KM,  # 集約テーブルは既にdistance<=100でフィルタ済み
            columns=fallback_columns,
            max_altitude=max_altitude,
        )

    # 生データの場合は既存の関数を使用
    if level.table == "meteorological_data":
        return _fallback_to_raw()

    # 集約テーブルが存在するか確認
    table_exists = check_materialized_views_exist(conn)
    if not table_exists.get(level.table):
        logging.warning("Aggregate table %s does not exist, falling back to raw data", level.table)
        return _fallback_to_raw()

    where, params = _build_aggregate_filter(time_start, time_end, max_altitude)

    # サンプリングデータを取得（実際のデータ点を使用）
    start = time.perf_counter()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
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
                WHERE {where}
                ORDER BY time
            """  # noqa: S608
            cur.execute(query, params)
            data = cur.fetchall()

            logging.info(
                "Elapsed time: %.2f sec (sampled data from %s, %s rows)",
                time.perf_counter() - start,
                level.table,
                f"{len(data):,}",
            )

            # データが空の場合は生データにフォールバック
            if not data:
                logging.warning("No data in aggregate table %s, falling back to raw data", level.table)
                return _fallback_to_raw()

            return data

    except psycopg2.Error as e:
        logging.warning(
            "Error fetching from aggregate table %s: %s, falling back to raw data",
            level.table,
            str(e),
        )
        return _fallback_to_raw()


def _align_to_bucket_start(dt: datetime.datetime, bucket_seconds: int) -> datetime.datetime:
    """datetime をバケット境界（JST 基準）に切り下げる

    増分更新の DELETE / INSERT で同じバケット境界を使うことで、
    バケット途中のデータだけが再集約されて重複キーになるのを防ぐ。

    Args:
        dt: aware datetime
        bucket_seconds: バケット幅（秒）

    Returns:
        バケット開始時刻に切り下げた aware datetime（JST）
    """
    zone = my_lib.time.get_zoneinfo()
    local = dt.astimezone(zone)
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    seconds_into_day = (local - midnight).total_seconds()
    aligned_seconds = int(seconds_into_day // bucket_seconds) * bucket_seconds
    return midnight + datetime.timedelta(seconds=aligned_seconds)


def _build_aggregate_insert_sql(spec: AggregateTableSpec, with_time_filter: bool) -> str:
    """集約テーブルへの INSERT ... SELECT 文を構築する

    パラメータ順: (distance, temperature 閾値, 高度下限, 高度上限 [, time 下限])
    """
    time_filter = "AND time >= %s" if with_time_filter else ""
    altitude_bin = amdar.constants.AGGREGATE_ALTITUDE_BIN_METERS
    return f"""
        INSERT INTO {spec.table} ({_AGGREGATE_COLUMNS})
        SELECT DISTINCT ON (time_bucket, altitude_bin)
            {spec.bucket_expr} AS time_bucket,
            (floor(altitude / {altitude_bin}) * {altitude_bin})::int AS altitude_bin,
            time,
            altitude,
            temperature,
            wind_x,
            wind_y,
            wind_speed,
            wind_angle
        FROM meteorological_data
        WHERE distance <= %s
          AND temperature > %s
          AND altitude IS NOT NULL
          AND altitude >= %s
          AND altitude <= %s
          {time_filter}
        ORDER BY time_bucket, altitude_bin, time DESC
    """  # noqa: S608


def _aggregate_quality_filter_params() -> tuple[Any, ...]:
    """集約 SELECT の品質フィルタパラメータ"""
    return (
        DEFAULT_DISTANCE_KM,
        amdar.constants.GRAPH_TEMPERATURE_THRESHOLD,
        amdar.constants.GRAPH_ALT_MIN,
        amdar.constants.GRAPH_ALT_MAX,
    )


def _refresh_aggregate_table_incremental(conn: PgConnection, spec: AggregateTableSpec) -> float:
    """集約テーブルを増分更新する

    直近ウィンドウ（refresh_window_seconds）のバケットを削除して再集約する。
    DELETE と INSERT は1トランザクションで実行する。

    Returns:
        更新にかかった時間（秒）

    Raises:
        psycopg2.Error: 更新に失敗した場合
    """
    start = time.perf_counter()
    window_start = _align_to_bucket_start(
        my_lib.time.now() - datetime.timedelta(seconds=spec.refresh_window_seconds),
        spec.bucket_seconds,
    )

    insert_sql = _build_aggregate_insert_sql(spec, with_time_filter=True)

    # conn は autocommit のため、明示的に BEGIN/COMMIT で1トランザクションにする
    with conn.cursor() as cur:
        cur.execute("BEGIN")
        try:
            cur.execute(
                f"DELETE FROM {spec.table} WHERE time_bucket >= %s",  # noqa: S608
                (window_start,),
            )
            cur.execute(insert_sql, (*_aggregate_quality_filter_params(), window_start))
            inserted = cur.rowcount
            cur.execute("COMMIT")
        except Exception:
            with contextlib.suppress(Exception):
                cur.execute("ROLLBACK")
            raise

    elapsed = time.perf_counter() - start
    logging.info(
        "Refreshed %s incrementally (window >= %s, %d rows) in %.2f sec",
        spec.table,
        window_start,
        inserted,
        elapsed,
    )
    return elapsed


def _rebuild_aggregate_table(conn: PgConnection, spec: AggregateTableSpec) -> float:
    """集約テーブルを全量再構築する（TRUNCATE + 全期間 INSERT）

    Returns:
        再構築にかかった時間（秒）

    Raises:
        psycopg2.Error: 再構築に失敗した場合
    """
    start = time.perf_counter()
    insert_sql = _build_aggregate_insert_sql(spec, with_time_filter=False)

    with conn.cursor() as cur:
        cur.execute("BEGIN")
        try:
            cur.execute(f"TRUNCATE {spec.table}")
            cur.execute(insert_sql, _aggregate_quality_filter_params())
            inserted = cur.rowcount
            cur.execute("COMMIT")
        except Exception:
            with contextlib.suppress(Exception):
                cur.execute("ROLLBACK")
            raise

    elapsed = time.perf_counter() - start
    logging.info("Rebuilt %s (%d rows) in %.2f sec", spec.table, inserted, elapsed)
    return elapsed


def refresh_materialized_views(conn: PgConnection) -> MaterializedViewRefreshResult:
    """
    全ての集約テーブルを増分更新する

    名前は互換性のため維持している（旧実装ではマテリアライズドビューを
    REFRESH していたが、現在は増分集約テーブルの更新を行う）。
    各テーブルは独立に更新され、片方の失敗はもう片方の更新を妨げない。

    Args:
        conn: データベース接続

    Returns:
        各テーブルの更新にかかった時間（秒）。エラー時は -1
    """
    results: dict[str, float] = {}
    for spec in _AGGREGATE_TABLE_SPECS:
        try:
            results[spec.table] = _refresh_aggregate_table_incremental(conn, spec)
        except Exception:
            logging.exception("Failed to refresh %s", spec.table)
            with contextlib.suppress(Exception):
                conn.rollback()
            results[spec.table] = -1.0

    return MaterializedViewRefreshResult(
        halfhourly_altitude_grid=results["halfhourly_altitude_grid"],
        threehour_altitude_grid=results["threehour_altitude_grid"],
    )


def rebuild_aggregate_tables(conn: PgConnection) -> MaterializedViewRefreshResult:
    """
    全ての集約テーブルを全量再構築する（TRUNCATE + 全期間 INSERT）

    初期構築やデータ修復時に使用する。データ量によっては長時間かかる。
    各テーブルは独立に再構築され、片方の失敗はもう片方を妨げない。

    Args:
        conn: データベース接続

    Returns:
        各テーブルの再構築にかかった時間（秒）。エラー時は -1
    """
    results: dict[str, float] = {}
    for spec in _AGGREGATE_TABLE_SPECS:
        try:
            results[spec.table] = _rebuild_aggregate_table(conn, spec)
        except Exception:
            logging.exception("Failed to rebuild %s", spec.table)
            with contextlib.suppress(Exception):
                conn.rollback()
            results[spec.table] = -1.0

    return MaterializedViewRefreshResult(
        halfhourly_altitude_grid=results["halfhourly_altitude_grid"],
        threehour_altitude_grid=results["threehour_altitude_grid"],
    )


def check_materialized_views_exist(conn: PgConnection) -> MaterializedViewsStatus:
    """
    集約テーブル（旧マテリアライズドビュー）の存在を確認する

    名前は互換性のため維持している。to_regclass による1回のクエリで
    テーブル・ビューいずれの形態でも存在を検出する。

    Args:
        conn: データベース接続

    Returns:
        集約テーブルの存在状態

    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass(%s) IS NOT NULL, to_regclass(%s) IS NOT NULL",
            AGGREGATE_TABLES,
        )
        row = cur.fetchone()

    if row is None:
        return MaterializedViewsStatus()

    return MaterializedViewsStatus(
        halfhourly_altitude_grid=bool(row[0]),
        threehour_altitude_grid=bool(row[1]),
    )


def _fetch_view_stats(conn: PgConnection, table: str) -> MaterializedViewStats:
    """単一の集約テーブルの統計情報を取得する"""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(*) as row_count,
                    MIN(time_bucket) as earliest,
                    MAX(time_bucket) as latest
                FROM {table}
                """  # noqa: S608
            )
            result = cur.fetchone()
            if result:
                return MaterializedViewStats(
                    row_count=result["row_count"],
                    earliest=result["earliest"],
                    latest=result["latest"],
                )
            return MaterializedViewStats(row_count=0, earliest=None, latest=None)
    except Exception:
        logging.exception("Failed to get stats for %s", table)
        return MaterializedViewStats(row_count=0, earliest=None, latest=None, error=True)


def get_materialized_view_stats(conn: PgConnection) -> AllMaterializedViewStats:
    """
    集約テーブルの統計情報を取得する

    Args:
        conn: データベース接続

    Returns:
        各テーブルの統計情報

    """
    return AllMaterializedViewStats(
        halfhourly_altitude_grid=_fetch_view_stats(conn, "halfhourly_altitude_grid"),
        threehour_altitude_grid=_fetch_view_stats(conn, "threehour_altitude_grid"),
    )


if __name__ == "__main__":
    import multiprocessing

    import docopt
    import my_lib.logger

    import amdar.config
    import amdar.sources.modes.receiver

    assert __doc__ is not None  # noqa: S101
    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    debug_mode = args["-D"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = amdar.config.load_config(config_file)

    measurement_queue: multiprocessing.Queue[MeasurementData] = multiprocessing.Queue()

    amdar.sources.modes.receiver.start(config, measurement_queue)

    conn = open(
        config.database.host,
        config.database.port,
        config.database.name,
        config.database.user,
        config.database.password,
    )

    db_config = DatabaseConfig(
        host=config.database.host,
        port=config.database.port,
        name=config.database.name,
        user=config.database.user,
        password=config.database.password,
    )

    store_queue(conn, measurement_queue, config.liveness.file.collector, db_config, config.slack)
