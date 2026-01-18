"""AMDAR 共通定数"""

import functools
import pathlib
from typing import Literal

# 設定スキーマファイル名
SCHEMA_FILENAME = "config.schema"

# ===============================
# 単位変換定数
# ===============================
FEET_TO_METERS: float = 0.3048
METERS_TO_FEET: float = 1 / 0.3048
KNOTS_TO_MS: float = 0.514444
KM_PER_DEGREE_LATITUDE: float = 111.0

# デフォルト距離フィルタ（km）
# データ取得時に観測地点からこの距離以内のデータのみを対象とする
DEFAULT_DISTANCE_KM: float = 100.0

# ===============================
# 受信メソッド定数
# ===============================
MethodType = Literal["mode-s", "vdl2"]
MODE_S_METHOD: MethodType = "mode-s"
VDL2_METHOD: MethodType = "vdl2"

# ===============================
# デフォルト基準座標（東京駅）
# ===============================
DEFAULT_REFERENCE_LATITUDE: float = 35.682677
DEFAULT_REFERENCE_LONGITUDE: float = 139.762230

# ===============================
# タイムアウト・インターバル定数
# ===============================

# ヘルスチェック猶予期間
CONTAINER_STARTUP_GRACE_PERIOD_SECONDS: int = 120  # コンテナ起動後の猶予期間
VDL2_STARTUP_GRACE_PERIOD_SECONDS: int = 10 * 60 * 60  # VDL2 用の猶予期間（10時間）

# VDL2 フラグメントタイムアウト
VDL2_FRAGMENT_TIMEOUT_SECONDS: int = 300  # 5分

# ===============================
# Mode-S receiver 設定
# ===============================
MODES_RECEIVER_MAX_RETRIES: int = 10  # 最大再接続回数
MODES_RECEIVER_BASE_DELAY: float = 2.0  # 再接続遅延の初期値（秒）
MODES_RECEIVER_MAX_DELAY: float = 60.0  # 再接続遅延の最大値（秒）
MODES_RECEIVER_SOCKET_TIMEOUT: float = 30.0  # ソケットタイムアウト（秒）

# ===============================
# Database 再接続設定
# ===============================
DB_MAX_RECONNECT_RETRIES: int = 5  # 最大再接続回数
DB_RECONNECT_DELAY_SECONDS: float = 5.0  # 再接続遅延（秒）

# ジョブ管理
JOB_EXPIRY_SECONDS: int = 30 * 60  # 30分（結果の保持期間）
JOB_CLEANUP_INTERVAL_SECONDS: int = 60  # 1分（クリーンアップ間隔）
JOB_TIMEOUT_SECONDS: int = 20 * 60  # 20分（ジョブタイムアウト）

# キャッシュ
CACHE_TTL_SECONDS: int = 30 * 60  # 30分
ETAG_TIME_ROUND_SECONDS: int = 10 * 60  # 10分

# 事前生成
PREGENERATION_INTERVAL_SECONDS: int = 25 * 60  # 25分
DEFAULT_PREGENERATION_DAYS: int = 7  # デフォルト表示期間（日）

# グラフ生成タイムアウト（期間別）
GRAPH_GEN_TIMEOUT_7DAYS_SECONDS: int = 60  # 7日以内: 60秒
GRAPH_GEN_TIMEOUT_30DAYS_SECONDS: int = 120  # 30日以内: 120秒
GRAPH_GEN_TIMEOUT_90DAYS_SECONDS: int = 180  # 90日以内: 180秒
GRAPH_GEN_TIMEOUT_OVER90DAYS_SECONDS: int = 300  # 90日以上: 300秒
GRAPH_JOB_TIMEOUT_BUFFER_SECONDS: int = 60  # ジョブタイムアウト用バッファ（キュー待ち考慮）

# VDL2 liveness チェックタイムアウト
VDL2_LIVENESS_TIMEOUT_SECONDS: int = 8 * 60 * 60  # 8時間

# グラフキャッシュ
CACHE_START_TIME_TOLERANCE_SECONDS: int = 30 * 60  # 開始日時の許容差: 30分

# Cache-Control ヘッダー設定
CACHE_CONTROL_MAX_AGE_RESULT: int = 1800  # グラフ結果キャッシュ（30分）
CACHE_CONTROL_MAX_AGE_STATUS: int = 600  # ステータス情報キャッシュ（10分）

# ===============================
# グラフ表示用定数
# ===============================

# 温度閾値・範囲
GRAPH_TEMPERATURE_THRESHOLD: int = -100  # 異常値閾値
GRAPH_TEMP_MIN_DEFAULT: int = -80  # limit_altitude=False 時の最小温度
GRAPH_TEMP_MAX_DEFAULT: int = 30  # limit_altitude=False 時の最大温度
GRAPH_TEMP_MIN_LIMITED: int = -20  # limit_altitude=True 時の最小温度
GRAPH_TEMP_MAX_LIMITED: int = 40  # limit_altitude=True 時の最大温度

# 高度範囲
GRAPH_ALT_MIN: int = 0
GRAPH_ALT_MAX: int = 13000
GRAPH_ALTITUDE_LIMIT: int = 2000  # limit_altitude=True 時の上限高度

# 画像解像度
GRAPH_IMAGE_DPI: float = 200.0

# グラフ名の型定義
GraphName = Literal[
    "scatter_2d",
    "scatter_3d",
    "contour_2d",
    "contour_3d",
    "density",
    "heatmap",
    "temperature",
    "wind_direction",
]


@functools.cache
def _get_repo_root() -> pathlib.Path:
    """リポジトリルートを取得（キャッシュ済み）

    src/amdar/constants.py から3階層上がリポジトリルート。
    """
    return pathlib.Path(__file__).parent.parent.parent


def get_schema_path() -> pathlib.Path:
    """設定スキーマファイルのパスを取得する

    リポジトリルート/config.schema を取得する。
    作業ディレクトリに依存せず、常に正しいパスを返す。

    Returns:
        スキーマファイルの絶対パス
    """
    return _get_repo_root() / SCHEMA_FILENAME


def sanitize_columns(
    columns: list[str],
    valid_columns: tuple[str, ...],
) -> str:
    """カラム名をサニタイズして SQL 用の文字列で返す

    Args:
        columns: リクエストされたカラムリスト
        valid_columns: 有効なカラムのタプル

    Returns:
        サニタイズ済みのカラム文字列（カンマ区切り）

    Raises:
        ValueError: 有効なカラムがない場合
    """
    sanitized = [col for col in columns if col in valid_columns]
    if not sanitized:
        msg = "No valid columns specified"
        raise ValueError(msg)
    return ", ".join(sanitized)


def get_db_schema_path(schema_name: str) -> pathlib.Path:
    """データベーススキーマファイルのパスを取得する

    リポジトリルート/schema/ ディレクトリからスキーマファイルを取得する。

    Args:
        schema_name: スキーマファイル名（例: "postgres.schema", "sqlite.schema"）

    Returns:
        スキーマファイルの絶対パス
    """
    return _get_repo_root() / "schema" / schema_name
