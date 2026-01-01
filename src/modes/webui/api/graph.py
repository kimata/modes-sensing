#!/usr/bin/env python3
"""
気象データをグラフにプロットします．

Usage:
  graph.py [-c CONFIG] [-p DAYS] [-D]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
  -p DAYS           : グラフ化する対象区間(日数)を指定します。[default: 7]
  -D                : デバッグモードで動作します。
"""

from __future__ import annotations

import atexit
import concurrent.futures
import datetime
import hashlib
import io
import json
import logging
import multiprocessing
import multiprocessing.pool
import pathlib
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict

import flask
import matplotlib  # noqa: ICN001

matplotlib.use("Agg")  # pyplotのimport前に設定する必要がある
import matplotlib.dates
import matplotlib.font_manager
import matplotlib.pyplot  # noqa: ICN001
import matplotlib.ticker
import mpl_toolkits.mplot3d  # noqa: F401
import my_lib.panel_config
import my_lib.pil_util
import my_lib.plot_util
import my_lib.time
import numpy  # noqa: ICN001
import pandas  # noqa: ICN001
import PIL.Image
import PIL.ImageDraw
import PIL.ImageFont
import scipy.interpolate

import modes.database_postgresql
from modes.webui.api.job_manager import JobManager, JobStatus

if TYPE_CHECKING:
    from collections.abc import Callable

    from matplotlib.axes import Axes
    from matplotlib.figure import Figure
    from psycopg2.extensions import connection as PgConnection  # noqa: N812


class GraphDefinition(TypedDict):
    """グラフ定義"""

    func: Callable[..., Figure]
    size: tuple[int, int]
    future: concurrent.futures.Future[Figure] | None


class PreparedData(TypedDict):
    """準備済みデータ"""

    dataframe: pandas.DataFrame | None
    count: int

def get_font_config(config_dict: dict[str, Any]) -> my_lib.panel_config.FontConfig:
    """辞書形式のフォント設定をFontConfigオブジェクトに変換する"""
    return my_lib.panel_config.FontConfig(
        path=pathlib.Path(config_dict["path"]),
        map=config_dict["map"],
    )


IMAGE_DPI = 200.0

TEMPERATURE_THRESHOLD = -100
# 動的温度範囲設定用の定数
TEMP_MIN_DEFAULT = -80  # limit_altitude=False時
TEMP_MAX_DEFAULT = 30
TEMP_MIN_LIMITED = -20  # limit_altitude=True時
TEMP_MAX_LIMITED = 40
ALT_MIN = 0
ALT_MAX = 13000
ALTITUDE_LIMIT = 2000  # 高度制限時の最大値


def get_temperature_range(limit_altitude: bool = False) -> tuple[int, int]:
    """limit_altitudeに応じた温度範囲を取得"""
    if limit_altitude:
        return TEMP_MIN_LIMITED, TEMP_MAX_LIMITED
    else:
        return TEMP_MIN_DEFAULT, TEMP_MAX_DEFAULT


TICK_LABEL_SIZE = 8
CONTOUR_SIZE = 8
ERROR_SIZE = 30

AXIS_LABEL_SIZE = 12
TITLE_SIZE = 20

TIME_AXIS_LABEL = "日時"
ALT_AXIS_LABEL = "高度 (m)"
TEMP_AXIS_LABEL = "温度 (℃)"

blueprint = flask.Blueprint("modes-sensing-graph", __name__)


# グローバルプロセスプール管理（matplotlib マルチスレッド問題対応）
class ProcessPoolManager:
    """シングルトンパターンでプロセスプールを管理"""

    _instance = None
    _lock = multiprocessing.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance.pool = None
        return cls._instance

    def get_pool(self):
        """プロセスプールを取得（必要に応じて作成）"""
        if self.pool is None:
            with self._lock:
                if self.pool is None:
                    # CPUコア数に基づいてプロセス数を決定（最大10、最小1）
                    max_workers = min(max(multiprocessing.cpu_count() // 2, 1), 10)
                    self.pool = multiprocessing.Pool(processes=max_workers)
                    # アプリ終了時にプールをクリーンアップ
                    atexit.register(self.cleanup)
                    logging.info("Created global process pool with %d workers", max_workers)
        return self.pool

    def cleanup(self):
        """プロセスプールのクリーンアップ"""
        if self.pool is not None:
            try:
                self.pool.close()
                self.pool.join()
                self.pool = None
                logging.info("Cleaned up global process pool")
            except Exception as e:
                logging.warning("Error cleaning up process pool: %s", e)


# プロセスプールマネージャーのインスタンス
_pool_manager = ProcessPoolManager()

# 非同期ジョブの完了を追跡するためのデータ構造
# job_id -> (async_result, graph_name)
_pending_async_results: dict[str, tuple[multiprocessing.pool.AsyncResult, str]] = {}
_async_results_lock = threading.Lock()
_result_checker_started = False


def _start_result_checker_thread() -> None:
    """非同期ジョブの完了をポーリングするバックグラウンドスレッドを開始"""
    global _result_checker_started  # noqa: PLW0603
    if _result_checker_started:
        return

    def result_checker_loop() -> None:
        while True:
            time.sleep(0.5)  # 0.5秒ごとにチェック
            try:
                _check_pending_results()
            except Exception:
                logging.exception("Error in result checker thread")

    thread = threading.Thread(target=result_checker_loop, daemon=True, name="AsyncResultChecker")
    thread.start()
    _result_checker_started = True
    logging.info("Started async result checker thread")


def _check_pending_results() -> None:
    """保留中の非同期結果をチェックし、完了したものを処理"""
    with _async_results_lock:
        completed_jobs = []
        for job_id, (async_result, graph_name) in list(_pending_async_results.items()):
            if not _check_single_job(job_id, async_result, graph_name):
                continue
            completed_jobs.append(job_id)

        for job_id in completed_jobs:
            del _pending_async_results[job_id]


def _estimate_progress_and_stage(job_id: str) -> tuple[int, str]:
    """ジョブの進捗を推定して返す

    実測に基づく推定時間:
    - 1週間: 約16秒
    - 1ヶ月: 約21秒
    - 3ヶ月: 約133秒 (2分13秒)
    - 4ヶ月: 約212秒 (3分32秒)

    バッファを含めて少し長めに設定している。

    """
    job = _job_manager.get_job(job_id)
    if not job or not job.started_at:
        return 10, "開始中..."

    elapsed = time.time() - job.started_at
    # 期間に応じた推定処理時間を計算（実測+バッファ）
    period_days = (job.time_end - job.time_start).total_seconds() / 86400

    if period_days <= 7:
        estimated_total = 30  # 1週間以内: 約30秒（実測16秒+バッファ）
    elif period_days <= 30:
        estimated_total = 60  # 1ヶ月以内: 約1分（実測21秒+バッファ）
    elif period_days <= 90:
        estimated_total = 180  # 3ヶ月以内: 約3分（実測2分13秒+バッファ）
    elif period_days <= 180:
        estimated_total = 600  # 6ヶ月以内: 約10分（外挿+バッファ）
    else:
        estimated_total = 900  # それ以上: 約15分

    # 進捗を推定（10-95%の範囲）
    progress = min(95, 10 + int((elapsed / estimated_total) * 85))

    # 段階を推定
    if elapsed < 2:
        stage = "データベース接続中..."
    elif elapsed < estimated_total * 0.4:
        stage = "データ取得中..."
    elif elapsed < estimated_total * 0.7:
        stage = "データ処理中..."
    elif elapsed < estimated_total * 0.9:
        stage = "グラフ描画中..."
    else:
        stage = "画像生成中..."

    return progress, stage


def _check_single_job(job_id: str, async_result: multiprocessing.pool.AsyncResult, graph_name: str) -> bool:
    """単一のジョブをチェックし、完了していればTrueを返す"""
    try:
        if not async_result.ready():
            # 未完了の場合は進捗を更新
            progress, stage = _estimate_progress_and_stage(job_id)
            _job_manager.update_status(
                job_id, JobStatus.PROCESSING, progress=progress, stage=stage
            )
            return False

        try:
            result = async_result.get(timeout=1)
            image_bytes, elapsed = result
            _job_manager.update_status(
                job_id, JobStatus.COMPLETED, result=image_bytes, progress=100, stage="完了"
            )
            logging.info(
                "Job %s completed for %s (%.2f sec) via polling", job_id, graph_name, elapsed
            )
        except Exception:
            logging.exception("Job %s failed for %s", job_id, graph_name)
            _job_manager.update_status(
                job_id, JobStatus.FAILED, error="Job execution failed", stage="エラー"
            )
        return True
    except Exception:
        logging.exception("Error checking job %s", job_id)
        return True


def connect_database(config: dict[str, Any]) -> PgConnection:
    return modes.database_postgresql.open(
        config["database"]["host"],
        config["database"]["port"],
        config["database"]["name"],
        config["database"]["user"],
        config["database"]["pass"],
    )


def set_title(title_text: str) -> None:
    matplotlib.pyplot.title(title_text, fontsize=TITLE_SIZE, fontweight="bold", pad=20)


def set_tick_label_size(ax: Axes, is_3d: bool = False) -> None:
    ax.tick_params(axis="x", labelsize=TICK_LABEL_SIZE)
    ax.tick_params(axis="y", labelsize=TICK_LABEL_SIZE)
    if is_3d:
        ax.tick_params(axis="z", labelsize=TICK_LABEL_SIZE)  # type: ignore[arg-type]


def set_axis_labels(
    ax: Axes,
    xlabel: str | None = None,
    ylabel: str | None = None,
    zlabel: str | None = None,
) -> None:
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=AXIS_LABEL_SIZE)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=AXIS_LABEL_SIZE)
    if zlabel:
        ax.set_zlabel(zlabel, fontsize=AXIS_LABEL_SIZE)  # type: ignore[attr-defined]


def set_temperature_range(ax: Axes, axis: str = "x", limit_altitude: bool = False) -> None:
    # limit_altitudeに応じた温度範囲を動的に取得
    temp_min, temp_max = get_temperature_range(limit_altitude)

    if axis == "x":
        ax.set_xlim(temp_min, temp_max)
    else:
        ax.set_ylim(temp_min, temp_max)


def set_altitude_range(ax: Axes, axis: str = "x", limit_altitude: bool = False) -> None:
    alt_max = ALTITUDE_LIMIT if limit_altitude else ALT_MAX
    if axis == "x":
        ax.set_xlim(ALT_MIN, alt_max)
    else:
        ax.set_ylim(ALT_MIN, alt_max)


def apply_time_axis_format(ax: Axes, time_range_days: float) -> None:
    import matplotlib.dates

    if time_range_days <= 1:
        ax.xaxis.set_major_locator(matplotlib.dates.HourLocator(interval=3))
        ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%-H時"))
    elif time_range_days <= 3:
        ax.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=1))
        ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%-d日\n%-H時"))
    elif time_range_days <= 7:
        ax.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=2))
        ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%-m月%-d日"))
    else:
        ax.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=int(time_range_days / 5)))
        ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%-m月%-d日"))


def append_colorbar(scatter, shrink=0.8, pad=0.01, aspect=35, fraction=0.046, limit_altitude=False):  # noqa: PLR0913
    """
    カラーバーを追加（サイズを縮小してプロットエリアを拡大）

    Args:
        scatter: プロット要素
        shrink: カラーバーの高さの縮小率 (デフォルト: 0.8)
        pad: プロットエリアとカラーバーの間隔 (デフォルト: 0.01)
        aspect: カラーバーの幅の比率 (デフォルト: 35、より細く)
        fraction: カラーバーの幅の割合 (デフォルト: 0.046)
        limit_altitude: 高度制限のフラグ (デフォルト: False)

    """
    # limit_altitudeに応じた温度範囲を動的に設定
    temp_min, temp_max = get_temperature_range(limit_altitude)
    scatter.set_clim(temp_min, temp_max)

    cbar = matplotlib.pyplot.colorbar(scatter, shrink=shrink, pad=pad, aspect=aspect, fraction=fraction)
    cbar.set_label(TEMP_AXIS_LABEL, fontsize=AXIS_LABEL_SIZE)
    set_tick_label_size(cbar.ax)

    return cbar


def create_grid(  # noqa: PLR0913
    time_numeric, altitudes, temperatures, grid_points=100, time_range=None, limit_altitude=False
):
    """グリッド作成を最適化（データ前処理改善、メモリ効率向上）"""
    # データが既にprepare_dataで前処理されているため、追加フィルタリングは最小限
    if len(time_numeric) == 0:
        # 空データの場合
        time_min, time_max = 0, 1
        alt_min = ALT_MIN
        if limit_altitude:
            alt_max = ALTITUDE_LIMIT
            alt_grid_points = int((alt_max - alt_min) / 50) + 1
        else:
            alt_max = ALT_MAX
            alt_grid_points = grid_points

        time_grid = numpy.linspace(time_min, time_max, grid_points)
        alt_grid = numpy.linspace(alt_min, alt_max, alt_grid_points)
        time_mesh, alt_mesh = numpy.meshgrid(time_grid, alt_grid, indexing="xy")
        temp_grid = numpy.full_like(time_mesh, numpy.nan)

        return {
            "time_mesh": time_mesh,
            "alt_mesh": alt_mesh,
            "temp_grid": temp_grid,
            "time_min": time_min,
            "time_max": time_max,
            "alt_min": alt_min,
            "alt_max": alt_max,
        }

    # グリッド範囲設定
    if time_range is not None:
        time_min, time_max = time_range
        # 実際のデータ範囲に制限
        actual_time_min, actual_time_max = time_numeric.min(), time_numeric.max()
        time_min = max(time_min, actual_time_min)
        time_max = min(time_max, actual_time_max)
    else:
        time_min, time_max = time_numeric.min(), time_numeric.max()

    # 高度範囲とグリッド密度をlimit_altitudeに応じて設定
    alt_min = ALT_MIN
    if limit_altitude:
        alt_max = ALTITUDE_LIMIT  # 2000m
        # 50m刻みにするため、2000m / 50m = 40点の高度グリッド
        alt_grid_points = int((alt_max - alt_min) / 50) + 1
    else:
        alt_max = ALT_MAX  # 13000m
        alt_grid_points = grid_points

    # 連続メモリ配置でグリッド作成
    time_grid = numpy.linspace(time_min, time_max, grid_points, dtype=numpy.float64)
    alt_grid = numpy.linspace(alt_min, alt_max, alt_grid_points, dtype=numpy.float64)
    time_mesh, alt_mesh = numpy.meshgrid(time_grid, alt_grid, indexing="xy")

    # データが既に前処理済みなので、範囲チェックのみ
    if time_range is not None:
        range_mask = (time_numeric >= time_min) & (time_numeric <= time_max)
        if not range_mask.any() or len(time_numeric[range_mask]) < 3:
            temp_grid = numpy.full_like(time_mesh, numpy.nan)
        else:
            filtered_time = time_numeric[range_mask]
            filtered_alt = altitudes[range_mask]
            filtered_temp = temperatures[range_mask]
            # 連続メモリ配置で補間処理を高速化
            points = numpy.column_stack((filtered_time, filtered_alt))
            points = numpy.ascontiguousarray(points)
            temp_values = numpy.ascontiguousarray(filtered_temp)
            temp_grid = scipy.interpolate.griddata(
                points, temp_values, (time_mesh, alt_mesh), method="linear", fill_value=numpy.nan
            )
    elif len(time_numeric) < 3:
        temp_grid = numpy.full_like(time_mesh, numpy.nan)
    else:
        # 連続メモリ配置で補間処理を高速化
        points = numpy.column_stack((time_numeric, altitudes))
        points = numpy.ascontiguousarray(points)
        temp_values = numpy.ascontiguousarray(temperatures)
        temp_grid = scipy.interpolate.griddata(
            points, temp_values, (time_mesh, alt_mesh), method="linear", fill_value=numpy.nan
        )

    return {
        "time_mesh": time_mesh,
        "alt_mesh": alt_mesh,
        "temp_grid": temp_grid,
        "time_min": time_min,
        "time_max": time_max,
        "alt_min": alt_min,
        "alt_max": alt_max,
    }


def create_figure(figsize=(12, 8)):
    """余白を最適化した図を作成"""
    fig, ax = matplotlib.pyplot.subplots(figsize=figsize)

    # 余白を削減してプロットエリアを拡大
    fig.subplots_adjust(
        left=0.08,  # 左余白
        bottom=0.08,  # 下余白
        right=0.94,  # 右余白（カラーバーの目盛テキスト用スペースを確保）
        top=0.90,  # 上余白（タイトル用スペースを拡大）
    )

    return fig, ax


def set_axis_2d_default(ax, time_range, limit_altitude=False):
    set_axis_labels(ax, TIME_AXIS_LABEL, ALT_AXIS_LABEL)

    set_altitude_range(ax, axis="y", limit_altitude=limit_altitude)

    # 高度軸の目盛りを設定（limit_altitude=Trueの場合は200m間隔）
    if limit_altitude:
        ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(200))
    else:
        ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(2000))

    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    set_tick_label_size(ax)

    apply_time_axis_format(
        ax, float(matplotlib.dates.date2num(time_range[-1]) - matplotlib.dates.date2num(time_range[0]))
    )


def conver_to_img(fig):
    buf = io.BytesIO()
    matplotlib.pyplot.savefig(buf, format="png", dpi=IMAGE_DPI, facecolor="white", transparent=False)

    buf.seek(0)

    img = PIL.Image.open(buf).copy()

    buf.close()

    matplotlib.pyplot.clf()
    matplotlib.pyplot.close(fig)

    return img


def create_no_data_image(config, graph_name, text="データがありません"):
    """データがない場合の画像を生成する"""
    # グラフサイズを取得
    size = GRAPH_DEF_MAP[graph_name].size

    # 新しい画像を作成（白背景）
    img = PIL.Image.new("RGB", size, color="white")

    # フォントサイズをDPIに合わせて調整（20pt）
    font_size = int(ERROR_SIZE * IMAGE_DPI / 72)

    # my_lib.pil_utilを使用してフォントを取得
    font_config = get_font_config(config["font"])
    font = my_lib.pil_util.get_font(font_config, "jp_bold", font_size)

    pos = (size[0] // 2, size[1] // 2)

    my_lib.pil_util.draw_text(img, text, pos, font, align="center", color="#666")

    return img


def prepare_data(raw_data):
    """データ前処理を最適化（無効データ除去、メモリ効率向上）"""
    if not raw_data:
        return {
            "count": 0,
            "times": numpy.array([]),
            "time_numeric": numpy.array([]),
            "altitudes": numpy.array([]),
            "temperatures": numpy.array([]),
            "dataframe": pandas.DataFrame(),
        }

    # 全データを一括でnumpy配列に変換（メモリ効率向上）
    data_length = len(raw_data)
    temperatures = numpy.empty(data_length, dtype=numpy.float64)
    altitudes = numpy.empty(data_length, dtype=numpy.float64)

    # 一括データ抽出（リスト内包表記より高速）
    for i, record in enumerate(raw_data):
        temperatures[i] = record["temperature"]
        altitudes[i] = record["altitude"]

    # 複合条件による無効データフィルタリング（一度に処理）
    valid_mask = (
        (temperatures > TEMPERATURE_THRESHOLD)
        & (numpy.isfinite(temperatures))
        & (numpy.isfinite(altitudes))
        & (altitudes >= ALT_MIN)
        & (altitudes <= ALT_MAX)
    )

    if not valid_mask.any():
        return {
            "count": 0,
            "times": numpy.array([]),
            "time_numeric": numpy.array([]),
            "altitudes": numpy.array([]),
            "temperatures": numpy.array([]),
            "dataframe": pandas.DataFrame(),
        }

    # 有効データのみを連続メモリ配置で抽出
    valid_indices = numpy.where(valid_mask)[0]
    valid_count = len(valid_indices)

    # 連続メモリ配列として確保（キャッシュ効率向上）
    clean_temperatures = numpy.ascontiguousarray(temperatures[valid_mask])
    clean_altitudes = numpy.ascontiguousarray(altitudes[valid_mask])

    # 時間データの効率的処理
    times_list = [raw_data[i]["time"] for i in valid_indices]

    # pandas.to_datetimeの最適化設定
    times = pandas.to_datetime(times_list, utc=False, cache=True).to_numpy()

    # matplotlib.dates.date2numをベクトル化
    time_numeric = numpy.ascontiguousarray(matplotlib.dates.date2num(times))

    # DataFrame作成は風向グラフでのみ必要（遅延作成）
    # 必要な場合のみフィルタリングされたデータでDataFrame作成
    filtered_records = [raw_data[i] for i in valid_indices] if valid_count < data_length else raw_data
    clean_df = pandas.DataFrame(filtered_records) if filtered_records else pandas.DataFrame()

    return {
        "count": valid_count,
        "times": times,
        "time_numeric": time_numeric,
        "altitudes": clean_altitudes,
        "temperatures": clean_temperatures,
        "dataframe": clean_df,
    }


def prepare_data_numpy(numpy_data: dict) -> dict:
    """NumPy配列形式のデータから描画用データを準備する（高速版）

    fetch_by_time_numpy / fetch_aggregated_numpy から返されたデータを
    グラフ描画用の形式に変換する。Pythonループを使わずベクトル化処理のみ。

    Args:
        numpy_data: fetch_by_time_numpy から返された辞書
            {
                "time": numpy.ndarray,
                "altitude": numpy.ndarray,
                "temperature": numpy.ndarray,
                "wind_x": numpy.ndarray (オプション),
                "wind_y": numpy.ndarray (オプション),
                "wind_speed": numpy.ndarray (オプション),
                "wind_angle": numpy.ndarray (オプション),
                "count": int,
            }

    Returns:
        グラフ描画用のデータ辞書

    """
    if numpy_data["count"] == 0:
        return {
            "count": 0,
            "times": numpy.array([], dtype="datetime64[us]"),
            "time_numeric": numpy.array([], dtype=numpy.float64),
            "altitudes": numpy.array([], dtype=numpy.float64),
            "temperatures": numpy.array([], dtype=numpy.float64),
            "dataframe": pandas.DataFrame(),
            # 風データ
            "wind_x": numpy.array([], dtype=numpy.float64),
            "wind_y": numpy.array([], dtype=numpy.float64),
            "wind_speed": numpy.array([], dtype=numpy.float64),
            "wind_angle": numpy.array([], dtype=numpy.float64),
        }

    times = numpy_data["time"]
    altitudes = numpy_data["altitude"]
    temperatures = numpy_data["temperature"]

    # 複合条件による無効データフィルタリング（ベクトル化）
    valid_mask = (
        (temperatures > TEMPERATURE_THRESHOLD)
        & numpy.isfinite(temperatures)
        & numpy.isfinite(altitudes)
        & (altitudes >= ALT_MIN)
        & (altitudes <= ALT_MAX)
    )

    valid_count = numpy.count_nonzero(valid_mask)

    if valid_count == 0:
        return {
            "count": 0,
            "times": numpy.array([], dtype="datetime64[us]"),
            "time_numeric": numpy.array([], dtype=numpy.float64),
            "altitudes": numpy.array([], dtype=numpy.float64),
            "temperatures": numpy.array([], dtype=numpy.float64),
            "dataframe": pandas.DataFrame(),
            "wind_x": numpy.array([], dtype=numpy.float64),
            "wind_y": numpy.array([], dtype=numpy.float64),
            "wind_speed": numpy.array([], dtype=numpy.float64),
            "wind_angle": numpy.array([], dtype=numpy.float64),
        }

    # 有効データのみを連続メモリ配置で抽出（ベクトル化）
    clean_times = times[valid_mask]
    clean_altitudes = numpy.ascontiguousarray(altitudes[valid_mask])
    clean_temperatures = numpy.ascontiguousarray(temperatures[valid_mask])

    # datetime64[us] から matplotlib の date number に変換（ベクトル化）
    # matplotlib 3.3以降: date number のエポックは 1970-01-01 = 0.0
    # numpy の datetime64[us] は 1970-01-01 からのマイクロ秒
    time_numeric = clean_times.astype("float64") / (86400 * 1e6)
    time_numeric = numpy.ascontiguousarray(time_numeric)

    # 風データの処理
    result: dict = {
        "count": valid_count,
        "times": clean_times,
        "time_numeric": time_numeric,
        "altitudes": clean_altitudes,
        "temperatures": clean_temperatures,
        "dataframe": pandas.DataFrame(),  # 必要時に後で作成
    }

    # 風データがあれば追加
    if "wind_x" in numpy_data:
        result["wind_x"] = numpy.ascontiguousarray(numpy_data["wind_x"][valid_mask])
        result["wind_y"] = numpy.ascontiguousarray(numpy_data["wind_y"][valid_mask])
        result["wind_speed"] = numpy.ascontiguousarray(numpy_data["wind_speed"][valid_mask])
        result["wind_angle"] = numpy.ascontiguousarray(numpy_data["wind_angle"][valid_mask])
    else:
        result["wind_x"] = numpy.array([], dtype=numpy.float64)
        result["wind_y"] = numpy.array([], dtype=numpy.float64)
        result["wind_speed"] = numpy.array([], dtype=numpy.float64)
        result["wind_angle"] = numpy.array([], dtype=numpy.float64)

    return result


def set_font(font_config_dict):
    try:
        # 辞書形式のフォント設定をFontConfigオブジェクトに変換
        font_config = get_font_config(font_config_dict)

        for font_file in font_config.map.values():
            matplotlib.font_manager.fontManager.addfont(font_config.path.resolve() / font_file)

        font_name = my_lib.plot_util.get_plot_font(font_config, "jp_medium", 12).get_name()

        matplotlib.pyplot.rcParams["font.family"] = [font_name, "sans-serif"]
        matplotlib.pyplot.rcParams["font.sans-serif"] = [font_name] + matplotlib.pyplot.rcParams[
            "font.sans-serif"
        ]
    except Exception:
        logging.exception("Failed to set font")


def set_axis_3d(ax, time_numeric, limit_altitude=False):
    set_axis_labels(ax, TIME_AXIS_LABEL, ALT_AXIS_LABEL, TEMP_AXIS_LABEL)

    time_range = time_numeric[-1] - time_numeric[0]
    apply_time_axis_format(ax, time_range)

    # 高度軸の最大値を設定
    alt_max = ALTITUDE_LIMIT if limit_altitude else ALT_MAX

    # 高度軸の目盛りを設定（limit_altitude=Trueの場合は200m間隔）
    if limit_altitude:
        ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(200))
    else:
        ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(2000))

    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

    set_tick_label_size(ax, is_3d=True)

    ax.set_ylim(ALT_MIN, alt_max)
    # 温度軸の範囲設定（limit_altitudeによって変更）
    temp_min, temp_max = get_temperature_range(limit_altitude)
    ax.set_zlim(temp_min, temp_max)


def create_3d_figure(figsize=(12, 8)):
    """余白を最適化した3D図を作成"""
    fig = matplotlib.pyplot.figure(figsize=figsize)
    ax = fig.add_subplot(111, projection="3d")

    # 3D図の余白を削減してプロットエリアを拡大
    fig.subplots_adjust(
        left=0.02,  # 左余白
        bottom=0.05,  # 下余白
        right=0.94,  # 右余白（カラーバーをより右に、プロットエリアを拡大）
        top=0.91,  # 上余白（タイトル用スペースを拡大）
    )

    return fig, ax


def setup_3d_colorbar_and_layout(ax):
    """3Dプロットの余白とレイアウトを最適化"""
    ax.view_init(elev=25, azim=35)
    # 3Dプロットの位置を調整（左、下、幅、高さ）
    # プロットエリアを拡大（幅を0.82から0.86に）
    ax.set_position([0.02, 0.05, 0.86, 0.88])


def plot_scatter_3d(data, figsize, limit_altitude=False):
    logging.info("Staring plot scatter 3d (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    fig, ax = create_3d_figure(figsize)
    scatter = ax.scatter(
        data["time_numeric"],
        data["altitudes"],
        data["temperatures"],
        c=data["temperatures"],
        cmap="plasma",
        marker="o",
        s=15,
        alpha=0.9,
        rasterized=True,
        edgecolors="none",
    )

    set_axis_3d(ax, data["time_numeric"], limit_altitude)
    append_colorbar(scatter, shrink=0.6, pad=0.01, aspect=35, limit_altitude=limit_altitude)
    setup_3d_colorbar_and_layout(ax)

    set_title("航空機の気象データ (3D)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_density(data, figsize, limit_altitude=False):
    logging.info("Staring plot density (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    fig, ax = create_figure(figsize)

    scatter = ax.scatter(
        data["altitudes"],
        data["temperatures"],
        c=data["temperatures"],
        cmap="plasma",
        s=15,
        alpha=0.9,
        rasterized=True,
        edgecolors="none",
    )

    set_axis_labels(ax, ALT_AXIS_LABEL, TEMP_AXIS_LABEL)
    set_altitude_range(ax, axis="x", limit_altitude=limit_altitude)
    set_temperature_range(ax, axis="y", limit_altitude=limit_altitude)
    set_tick_label_size(ax)

    append_colorbar(scatter, shrink=1.0, pad=0.01, aspect=35, fraction=0.03, limit_altitude=limit_altitude)

    ax.grid(True, alpha=0.7)

    set_title("航空機の気象データ (高度・温度分布)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_contour_2d(data, figsize, plot_time_start=None, plot_time_end=None, limit_altitude=False):
    logging.info("Staring plot contour (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    # プロット時間範囲が指定されている場合は、グリッドをその範囲で作成
    # ただし、実際のデータ範囲を超えないように制限
    if plot_time_start and plot_time_end:
        plot_time_min = matplotlib.dates.date2num(plot_time_start)
        plot_time_max = matplotlib.dates.date2num(plot_time_end)
        # 実際のデータ範囲内に制限
        if len(data["time_numeric"]) > 0:
            actual_min = data["time_numeric"].min()
            actual_max = data["time_numeric"].max()
            plot_time_min = max(plot_time_min, actual_min)
            plot_time_max = min(plot_time_max, actual_max)
        grid = create_grid(
            data["time_numeric"],
            data["altitudes"],
            data["temperatures"],
            grid_points=80,
            time_range=(plot_time_min, plot_time_max),
            limit_altitude=limit_altitude,
        )
    else:
        grid = create_grid(
            data["time_numeric"],
            data["altitudes"],
            data["temperatures"],
            grid_points=80,
            limit_altitude=limit_altitude,
        )

    fig, ax = create_figure(figsize)

    # limit_altitudeに応じた温度範囲と刻みを動的に設定
    temp_min, temp_max = get_temperature_range(limit_altitude)
    if limit_altitude:
        levels = numpy.arange(temp_min, temp_max + 1, 5)
    else:
        levels = numpy.arange(temp_min, temp_max + 1, 10)
    contour = ax.contour(
        grid["time_mesh"], grid["alt_mesh"], grid["temp_grid"], levels=levels, colors="black", linewidths=0.5
    )
    contourf = ax.contourf(
        grid["time_mesh"],
        grid["alt_mesh"],
        grid["temp_grid"],
        levels=levels,
        cmap="plasma",
        alpha=0.9,
    )

    ax.clabel(contour, inline=True, fontsize=CONTOUR_SIZE, fmt="%d℃")

    # プロット時間範囲が指定されている場合はそれを使用、そうでなければグリッド範囲を使用
    if plot_time_start and plot_time_end:
        time_range = [plot_time_start, plot_time_end]
    else:
        time_range = [
            matplotlib.dates.num2date(grid["time_min"]),
            matplotlib.dates.num2date(grid["time_max"]),
        ]

    set_axis_2d_default(ax, time_range, limit_altitude)

    append_colorbar(contourf, shrink=1.0, pad=0.01, aspect=35, fraction=0.03, limit_altitude=limit_altitude)

    set_title("航空機の気象データ (等高線)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_heatmap(data, figsize, plot_time_start=None, plot_time_end=None, limit_altitude=False):
    logging.info("Staring plot heatmap (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    # プロット時間範囲が指定されている場合は、グリッドをその範囲で作成
    # ただし、実際のデータ範囲を超えないように制限
    if plot_time_start and plot_time_end:
        plot_time_min = matplotlib.dates.date2num(plot_time_start)
        plot_time_max = matplotlib.dates.date2num(plot_time_end)
        # 実際のデータ範囲内に制限
        if len(data["time_numeric"]) > 0:
            actual_min = data["time_numeric"].min()
            actual_max = data["time_numeric"].max()
            plot_time_min = max(plot_time_min, actual_min)
            plot_time_max = min(plot_time_max, actual_max)
        grid = create_grid(
            data["time_numeric"],
            data["altitudes"],
            data["temperatures"],
            grid_points=80,
            time_range=(plot_time_min, plot_time_max),
            limit_altitude=limit_altitude,
        )
    else:
        grid = create_grid(
            data["time_numeric"],
            data["altitudes"],
            data["temperatures"],
            grid_points=80,
            limit_altitude=limit_altitude,
        )

    fig, ax = create_figure(figsize)

    im = ax.imshow(
        grid["temp_grid"],
        extent=(grid["time_min"], grid["time_max"], grid["alt_min"], grid["alt_max"]),
        aspect="auto",
        origin="lower",
        cmap="plasma",
        alpha=0.9,
        vmin=get_temperature_range(limit_altitude)[0],
        vmax=get_temperature_range(limit_altitude)[1],
    )

    # プロット時間範囲が指定されている場合はそれを使用、そうでなければグリッド範囲を使用
    if plot_time_start and plot_time_end:
        time_range = [plot_time_start, plot_time_end]
    else:
        time_range = [
            matplotlib.dates.num2date(grid["time_min"]),
            matplotlib.dates.num2date(grid["time_max"]),
        ]

    set_axis_2d_default(ax, time_range, limit_altitude)

    append_colorbar(im, shrink=1.0, pad=0.01, aspect=35, fraction=0.03, limit_altitude=limit_altitude)

    set_title("航空機の気象データ (ヒートマップ)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_scatter_2d(data, figsize, limit_altitude=False):
    logging.info("Staring plot 2d scatter (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    fig, ax = create_figure(figsize)

    sc = ax.scatter(
        data["times"],
        data["altitudes"],
        c=data["temperatures"],
        cmap="plasma",
        s=15,
        alpha=0.9,
        rasterized=True,
        edgecolors="none",
    )

    set_axis_2d_default(
        ax,
        [
            matplotlib.dates.num2date(data["time_numeric"].min()),
            matplotlib.dates.num2date(data["time_numeric"].max()),
        ],
        limit_altitude,
    )

    append_colorbar(sc, shrink=1.0, pad=0.01, aspect=35, fraction=0.03, limit_altitude=limit_altitude)

    ax.grid(True, alpha=0.7)

    set_title("航空機の気象データ")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_contour_3d(data, figsize, limit_altitude=False):
    logging.info("Starting plot contour 3d (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    # グリッドデータを作成
    grid = create_grid(
        data["time_numeric"],
        data["altitudes"],
        data["temperatures"],
        grid_points=60,
        limit_altitude=limit_altitude,
    )

    fig, ax = create_3d_figure(figsize)

    # 3Dサーフェスプロットを作成
    surf = ax.plot_surface(
        grid["time_mesh"],
        grid["alt_mesh"],
        grid["temp_grid"],
        cmap="plasma",
        alpha=0.9,
        antialiased=True,
        rstride=1,
        cstride=1,
        linewidth=0,
        edgecolor="none",
        vmin=get_temperature_range(limit_altitude)[0],
        vmax=get_temperature_range(limit_altitude)[1],
    )

    # 等高線を追加
    temp_min, temp_max = get_temperature_range(limit_altitude)
    levels = numpy.arange(temp_min, temp_max + 1, 10)
    ax.contour(
        grid["time_mesh"],
        grid["alt_mesh"],
        grid["temp_grid"],
        levels=levels,
        colors="black",
        linewidths=0.5,
        alpha=0.3,
        offset=temp_min,  # 底面に等高線を投影
    )

    set_axis_3d(ax, data["time_numeric"], limit_altitude)
    append_colorbar(surf, shrink=0.6, pad=0.01, aspect=35, limit_altitude=limit_altitude)
    setup_3d_colorbar_and_layout(ax)

    set_title("航空機の気象データ (3D)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def _validate_wind_dataframe(data):
    """風データのDataFrame検証とカラムチェック"""
    if "dataframe" not in data or len(data["dataframe"]) == 0:
        logging.warning("Wind data not available for wind direction plot")
        raise ValueError("Wind data not available")

    df = data["dataframe"]
    required_columns = ["time", "altitude", "wind_x", "wind_y", "wind_speed", "wind_angle"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logging.warning("Missing wind data columns: %s", missing_columns)
        logging.warning("Available columns: %s", list(df.columns))
        msg = f"Missing wind data columns: {missing_columns}"
        raise ValueError(msg)

    return df


def _extract_and_filter_wind_data(df, limit_altitude=False):
    """風データの抽出とフィルタリング"""
    # NumPyベースの高速前処理
    altitudes = df["altitude"].to_numpy()
    wind_x = df["wind_x"].to_numpy()
    wind_y = df["wind_y"].to_numpy()

    # 時間データの効率的変換
    if "time_numeric" in df.columns:
        time_numeric = df["time_numeric"].to_numpy()
    else:
        time_numeric = matplotlib.dates.date2num(df["time"].to_numpy())

    # 無風データを事前除外（ベクトル化）
    wind_speed = numpy.sqrt(wind_x**2 + wind_y**2)
    valid_wind_mask = wind_speed > 0.1

    # 高度制限の適用
    if limit_altitude:
        altitude_mask = altitudes <= ALTITUDE_LIMIT
        valid_wind_mask = valid_wind_mask & altitude_mask

    if not valid_wind_mask.any():
        logging.warning(
            "No valid wind vectors after filtering (speed: %s, limit_altitude: %s)",
            (wind_speed > 0.1).sum(),
            limit_altitude,
        )
        raise ValueError("No valid wind vectors after filtering")

    return {
        "altitudes": altitudes[valid_wind_mask],
        "wind_x": wind_x[valid_wind_mask],
        "wind_y": wind_y[valid_wind_mask],
        "time_numeric": time_numeric[valid_wind_mask],
    }


def _create_wind_bins(valid_data, limit_altitude=False):
    """風データのビニング処理"""
    from collections import defaultdict

    valid_altitudes = valid_data["altitudes"]
    valid_time_numeric = valid_data["time_numeric"]
    valid_wind_x = valid_data["wind_x"]
    valid_wind_y = valid_data["wind_y"]

    # 高度ビニング（limit_altitudeに応じて範囲と間隔を調整）
    if limit_altitude:
        # 2000mまでの範囲で、より細かい間隔
        altitude_bins = numpy.arange(0, ALTITUDE_LIMIT + 100, 100)
    else:
        # 従来通り13000mまで、200m間隔
        altitude_bins = numpy.arange(0, 13000, 200)

    altitude_bin_indices = numpy.searchsorted(altitude_bins, valid_altitudes, side="right") - 1
    altitude_bin_indices = numpy.clip(altitude_bin_indices, 0, len(altitude_bins) - 2)

    # 時間ビニング
    time_range = valid_time_numeric.max() - valid_time_numeric.min()
    if time_range <= 1:
        time_bins = 48  # 30分間隔
    elif time_range <= 3:
        time_bins = 24  # 3時間間隔
    else:
        time_bins = int(time_range * 4)  # 6時間間隔

    time_bin_edges = numpy.linspace(valid_time_numeric.min(), valid_time_numeric.max(), time_bins + 1)
    time_bin_indices = numpy.searchsorted(time_bin_edges, valid_time_numeric, side="right") - 1
    time_bin_indices = numpy.clip(time_bin_indices, 0, time_bins - 1)

    # ビニング集計
    bin_data = defaultdict(lambda: {"wind_x": [], "wind_y": [], "time_numeric": []})

    for i in range(len(valid_altitudes)):
        bin_key = (time_bin_indices[i], altitude_bin_indices[i])
        bin_data[bin_key]["wind_x"].append(valid_wind_x[i])
        bin_data[bin_key]["wind_y"].append(valid_wind_y[i])
        bin_data[bin_key]["time_numeric"].append(valid_time_numeric[i])

    return bin_data, altitude_bins


def _prepare_wind_data(data, limit_altitude=False):
    """風データの前処理とビニング処理（最適化版）"""
    df = _validate_wind_dataframe(data)
    valid_data = _extract_and_filter_wind_data(df, limit_altitude)
    bin_data, altitude_bins = _create_wind_bins(valid_data, limit_altitude)

    # 集計結果をDataFrameに変換
    grouped_data = []
    for (time_idx, alt_idx), values in bin_data.items():
        if len(values["wind_x"]) > 0:  # 空のビンをスキップ
            grouped_data.append(
                {
                    "time_bin": time_idx,
                    "altitude_bin": altitude_bins[alt_idx],
                    "wind_x": numpy.mean(values["wind_x"]),
                    "wind_y": numpy.mean(values["wind_y"]),
                    "time_numeric": numpy.mean(values["time_numeric"]),
                }
            )

    if not grouped_data:
        logging.warning("No valid wind data after binning")
        raise ValueError("No valid wind data after binning")

    grouped = pandas.DataFrame(grouped_data)

    # 風速と風向を再計算（ベクトル化）
    grouped["wind_speed"] = numpy.sqrt(grouped["wind_x"] ** 2 + grouped["wind_y"] ** 2)
    grouped["wind_angle"] = (90 - numpy.degrees(numpy.arctan2(grouped["wind_y"], grouped["wind_x"]))) % 360

    return grouped


def plot_wind_direction(data, figsize, limit_altitude=False):
    logging.info("Starting plot wind direction (limit_altitude: %s)", limit_altitude)
    start = time.perf_counter()

    # デバッグ情報
    if "dataframe" in data and len(data["dataframe"]) > 0:
        df = data["dataframe"]
        logging.info("Available columns in dataframe: %s", list(df.columns))
        logging.info("Dataframe shape: %s", df.shape)

    # データ前処理
    grouped = _prepare_wind_data(data, limit_altitude)

    # ベクトル計算（limit_altitudeに応じて高度範囲を調整）
    time_range = grouped["time_numeric"].max() - grouped["time_numeric"].min()
    altitude_range = ALTITUDE_LIMIT if limit_altitude else ALT_MAX
    u_scale = time_range / 30
    v_scale = altitude_range / 30

    wind_magnitude = numpy.sqrt(grouped["wind_x"] ** 2 + grouped["wind_y"] ** 2)
    # 風向きベクトルの符号を反転（wind_x, wind_yは風が来る方向、矢印は風が来る方向を指すべき）
    grouped["u_normalized"] = -(grouped["wind_x"] / wind_magnitude) * u_scale
    grouped["v_normalized"] = -(grouped["wind_y"] / wind_magnitude) * v_scale

    grouped = grouped.dropna()
    if len(grouped) == 0:
        logging.warning("No valid wind vectors after angle conversion")
        raise ValueError("No valid wind vectors after angle conversion")

    # プロット作成
    fig, ax = create_figure(figsize)
    wind_speeds = grouped["wind_speed"].to_numpy()
    wind_speeds_clipped = numpy.clip(wind_speeds, 0, 100)

    quiver = ax.quiver(
        grouped["time_numeric"],
        grouped["altitude_bin"],
        grouped["u_normalized"],
        grouped["v_normalized"],
        wind_speeds_clipped,
        cmap="plasma",
        scale=1,
        scale_units="xy",
        angles="xy",
        alpha=0.9,
        width=0.002,
        headwidth=3,
        headlength=5,
        minlength=0,
        pivot="middle",
    )

    quiver.set_clim(0, 100)

    set_axis_2d_default(
        ax,
        [
            matplotlib.dates.num2date(grouped["time_numeric"].min()),
            matplotlib.dates.num2date(grouped["time_numeric"].max()),
        ],
        limit_altitude,
    )

    cbar = matplotlib.pyplot.colorbar(quiver, shrink=0.8, pad=0.01, aspect=35, fraction=0.046)
    cbar.set_label("風速 (m/s)", fontsize=AXIS_LABEL_SIZE)
    set_tick_label_size(cbar.ax)

    set_title("航空機観測による風向・風速分布")

    img = conver_to_img(fig)
    return (img, time.perf_counter() - start)


def plot_temperature(data, figsize, limit_altitude=False):
    logging.info("Starting plot temperature timeseries (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    fig, ax = create_figure(figsize)

    # 高度範囲の定義（limit_altitudeによって変更）
    if limit_altitude:
        altitude_ranges = [
            {"min": 400, "max": 600, "label": "500±100m", "color": "blue", "marker": "o"},
            {"min": 900, "max": 1100, "label": "1000±100m", "color": "green", "marker": "s"},
            {"min": 1400, "max": 1600, "label": "1500±100m", "color": "orange", "marker": "^"},
        ]
    else:
        altitude_ranges = [
            {"min": 1400, "max": 1600, "label": "1500±100m", "color": "blue", "marker": "o"},
            {"min": 2900, "max": 3100, "label": "3000±100m", "color": "green", "marker": "s"},
            {"min": 4400, "max": 4600, "label": "4500±100m", "color": "orange", "marker": "^"},
            {"min": 5900, "max": 6100, "label": "6000±100m", "color": "red", "marker": "d"},
        ]

    # 各高度範囲のデータをプロット
    for alt_range in altitude_ranges:
        # 高度範囲でフィルタリング
        mask = (data["altitudes"] >= alt_range["min"]) & (data["altitudes"] <= alt_range["max"])
        if not numpy.any(mask):
            continue

        filtered_temps = data["temperatures"][mask]
        filtered_time_numeric = data["time_numeric"][mask]

        # 時系列でソート
        sort_indices = numpy.argsort(filtered_time_numeric)
        sorted_times = filtered_time_numeric[sort_indices]
        sorted_temps = filtered_temps[sort_indices]

        # 同じ時間帯のデータを平均化（30分間隔でビニング）
        if len(sorted_times) > 1:
            # 30分 = 0.020833日
            bin_size = 0.020833
            unique_times = []
            avg_temps = []

            current_bin_start = sorted_times[0]
            current_temps = []

            for i, time_val in enumerate(sorted_times):
                if time_val <= current_bin_start + bin_size:
                    current_temps.append(sorted_temps[i])
                else:
                    if current_temps:
                        unique_times.append(current_bin_start + bin_size / 2)
                        avg_temps.append(numpy.mean(current_temps))

                    current_bin_start = time_val
                    current_temps = [sorted_temps[i]]

            # 最後のビンを処理
            if current_temps:
                unique_times.append(current_bin_start + bin_size / 2)
                avg_temps.append(numpy.mean(current_temps))

            # プロット
            ax.plot(
                unique_times,
                avg_temps,
                color=alt_range["color"],
                marker=alt_range["marker"],
                markersize=4,
                linewidth=2,
                label=alt_range["label"],
                alpha=0.8,
            )

    # 軸の設定
    ax.set_xlabel("日時")
    ax.set_ylabel("温度 (℃)")
    ax.grid(True, alpha=0.7)

    time_range = data["time_numeric"].max() - data["time_numeric"].min()
    apply_time_axis_format(ax, time_range)

    # Y軸の範囲設定（limit_altitudeによって変更）
    temp_min, temp_max = get_temperature_range(limit_altitude)
    ax.set_ylim(temp_min, temp_max)

    # 凡例の追加
    ax.legend(loc="upper right", framealpha=0.9)

    set_title("高度別温度の時系列変化")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


@dataclass
class GraphDef:
    """グラフ定義"""

    func: Callable[..., tuple[PIL.Image.Image, float]]
    size: tuple[int, int]
    file: str


GRAPH_DEF_MAP: dict[str, GraphDef] = {
    "scatter_2d": GraphDef(func=plot_scatter_2d, size=(2400, 1600), file="scatter_2d.png"),
    "scatter_3d": GraphDef(func=plot_scatter_3d, size=(2800, 2800), file="scatter_3d.png"),
    "contour_2d": GraphDef(func=plot_contour_2d, size=(2400, 1600), file="contour_2d.png"),
    "contour_3d": GraphDef(func=plot_contour_3d, size=(2800, 2800), file="contour_3d.png"),
    "density": GraphDef(func=plot_density, size=(2400, 1600), file="density.png"),
    "heatmap": GraphDef(func=plot_heatmap, size=(2400, 1600), file="heatmap.png"),
    "temperature": GraphDef(func=plot_temperature, size=(2400, 1600), file="temperature.png"),
    "wind_direction": GraphDef(func=plot_wind_direction, size=(2400, 1600), file="wind_direction.png"),
}


# =============================================================================
# キャッシュ機能
# =============================================================================
CACHE_TTL_SECONDS = 30 * 60  # 30分

# git commit ハッシュのキャッシュ（プロセス起動時に一度だけ取得）
_git_commit_hash: str | None = None


def get_git_commit_hash() -> str:
    """現在の git commit ハッシュを取得する（キャッシュ付き）"""
    global _git_commit_hash  # noqa: PLW0603
    if _git_commit_hash is not None:
        return _git_commit_hash

    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        _git_commit_hash = result.stdout.strip()[:12] if result.returncode == 0 else "unknown"
    except Exception:
        logging.warning("Failed to get git commit hash")
        _git_commit_hash = "unknown"

    return _git_commit_hash


def generate_cache_key(
    graph_name: str,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
) -> str:
    """キャッシュキーを生成する"""
    git_commit = get_git_commit_hash()
    # タイムスタンプを秒単位で丸める（ミリ秒の違いでキャッシュミスを防ぐ）
    start_ts = int(time_start.timestamp())
    end_ts = int(time_end.timestamp())
    key_string = f"{graph_name}:{start_ts}:{end_ts}:{limit_altitude}:{git_commit}"
    # ファイル名として使いやすいようにハッシュ化
    return hashlib.sha256(key_string.encode()).hexdigest()[:32]


def get_cache_file_path(cache_dir: pathlib.Path, cache_key: str) -> pathlib.Path:
    """キャッシュファイルのパスを取得する"""
    return cache_dir / f"{cache_key}.png"


def get_cached_image(cache_dir: pathlib.Path, cache_key: str) -> bytes | None:
    """キャッシュから画像を取得する（TTL チェック付き）"""
    cache_file = get_cache_file_path(cache_dir, cache_key)

    if not cache_file.exists():
        return None

    # ファイルの更新時刻をチェック
    mtime = cache_file.stat().st_mtime
    if time.time() - mtime > CACHE_TTL_SECONDS:
        # TTL 超過
        return None

    try:
        return cache_file.read_bytes()
    except Exception:
        logging.warning("Failed to read cache file: %s", cache_file)
        return None


def save_to_cache(cache_dir: pathlib.Path, cache_key: str, image_bytes: bytes) -> None:
    """画像をキャッシュに保存する"""
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = get_cache_file_path(cache_dir, cache_key)
        cache_file.write_bytes(image_bytes)
    except Exception:
        logging.warning("Failed to save to cache: %s", cache_key)


def plot_in_subprocess(config, graph_name, time_start, time_end, figsize, limit_altitude=False):  # noqa: PLR0913, PLR0915
    """子プロセス内でデータ取得からグラフ描画まで一貫して実行する関数"""
    import matplotlib  # noqa: ICN001

    matplotlib.use("Agg")

    import matplotlib.pyplot  # noqa: ICN001

    # デバッグ: 子プロセスに渡された時間範囲を記録
    period_days = (time_end - time_start).total_seconds() / 86400
    logging.info(
        "[DEBUG] plot_in_subprocess() for %s: start=%s, end=%s, period=%.2f days",
        graph_name,
        time_start,
        time_end,
        period_days,
    )

    # データベース接続とデータ取得を子プロセス内で実行
    conn = connect_database(config)

    # heatmapとcontourグラフの場合、端の部分のプロットを改善するためデータ取得範囲を10%拡張
    if graph_name in ["heatmap", "contour_2d"]:
        time_range = time_end - time_start
        extension = time_range * 0.1  # 10%拡張
        extended_time_start = time_start - extension
        extended_time_end = time_end + extension
    else:
        extended_time_start = time_start
        extended_time_end = time_end

    # 風向グラフの場合は風データも取得
    include_wind = graph_name == "wind_direction"

    # 高速版NumPyフェッチ関数を使用
    # 期間が7日を超える場合は集約データを使用（パフォーマンス最適化）
    if period_days > 7:
        # 集約データを使用（期間に応じて自動的に適切なレベルを選択）
        numpy_data = modes.database_postgresql.fetch_aggregated_numpy(
            conn,
            extended_time_start,
            extended_time_end,
            max_altitude=ALTITUDE_LIMIT if limit_altitude else None,
            include_wind=include_wind,
        )
    else:
        # 7日以内は生データを使用
        numpy_data = modes.database_postgresql.fetch_by_time_numpy(
            conn,
            extended_time_start,
            extended_time_end,
            config["filter"]["area"]["distance"],
            max_altitude=ALTITUDE_LIMIT if limit_altitude else None,
            include_wind=include_wind,
        )
    conn.close()

    # デバッグ: 取得したデータの時間範囲を確認
    if numpy_data["count"] > 0:
        times = numpy_data["time"]
        logging.info(
            "Data range for %s: %s to %s (%d rows)",
            graph_name,
            times.min(),
            times.max(),
            numpy_data["count"],
        )
    else:
        logging.warning("No data fetched for %s", graph_name)

    # データ準備（高速版NumPy処理）
    data = prepare_data_numpy(numpy_data)

    if data["count"] < 10:
        # データがない場合の画像を生成
        try:
            img = create_no_data_image(config, graph_name)
            bytes_io = io.BytesIO()
            img.save(bytes_io, "PNG")
            bytes_io.seek(0)
            return bytes_io.getvalue(), 0
        except Exception:
            logging.exception("Failed to create no data image")
            img = create_no_data_image(config, graph_name, "グラフの作成に失敗しました")
            bytes_io = io.BytesIO()
            img.save(bytes_io, "PNG")
            bytes_io.seek(0)
            return bytes_io.getvalue(), 0

    set_font(config["font"])

    try:
        # heatmapとcontourグラフの場合、元の時間範囲を渡してプロット範囲を制限
        if graph_name in ["heatmap", "contour_2d"]:
            img, elapsed = GRAPH_DEF_MAP[graph_name].func(
                data, figsize, time_start, time_end, limit_altitude
            )
        else:
            img, elapsed = GRAPH_DEF_MAP[graph_name].func(data, figsize, limit_altitude)
    except Exception as e:
        logging.warning("Failed to generate %s: %s", graph_name, str(e))
        # エラー時は「データなし」画像を生成
        try:
            img = create_no_data_image(config, graph_name)
            elapsed = 0
        except Exception:
            logging.exception("Failed to create no data image")
            img = create_no_data_image(config, graph_name, "グラフの作成に失敗しました")
            elapsed = 0

    # PIL.Imageを直接returnできないので、bytesに変換して返す
    bytes_io = io.BytesIO()
    img.save(bytes_io, "PNG")
    bytes_io.seek(0)

    image_size = len(bytes_io.getvalue())
    logging.info(
        "[DEBUG] plot_in_subprocess() completed for %s: elapsed=%.2f sec, image_size=%d bytes",
        graph_name,
        elapsed,
        image_size,
    )

    return bytes_io.getvalue(), elapsed


def calculate_timeout(time_start, time_end):
    """
    期間に応じてタイムアウト値を決定する

    Args:
        time_start: 開始時刻
        time_end: 終了時刻

    Returns:
        タイムアウト秒数

    """
    days = (time_end - time_start).total_seconds() / 86400
    if days <= 7:
        return 60  # 1週間以内: 60秒
    elif days <= 30:
        return 120  # 1ヶ月以内: 120秒
    elif days <= 90:
        return 180  # 3ヶ月以内: 180秒
    else:
        return 300  # それ以上: 300秒


def plot(config, graph_name, time_start, time_end, limit_altitude=False):
    # デバッグ: plot()に渡された時間範囲を記録
    period_days = (time_end - time_start).total_seconds() / 86400
    logging.info(
        "[DEBUG] plot() called for %s: start=%s, end=%s, period=%.2f days, limit_altitude=%s",
        graph_name,
        time_start,
        time_end,
        period_days,
        limit_altitude,
    )

    # キャッシュチェック
    cache_dir_path = config.get("webapp", {}).get("cache_dir_path")
    cache_key = generate_cache_key(graph_name, time_start, time_end, limit_altitude)
    cached_image: bytes | None = None

    if cache_dir_path:
        cache_dir = pathlib.Path(cache_dir_path)
        cached_image = get_cached_image(cache_dir, cache_key)
        if cached_image:
            logging.info(
                "Serving %s from cache (key: %s, size: %d bytes)",
                graph_name,
                cache_key[:8],
                len(cached_image),
            )
            return cached_image

    # グラフサイズを計算
    figsize = tuple(x / IMAGE_DPI for x in GRAPH_DEF_MAP[graph_name].size)

    # 期間に応じたタイムアウト値を計算
    timeout_seconds = calculate_timeout(time_start, time_end)

    # グローバルプロセスプールを使用してデータ取得から描画まで実行
    pool = _pool_manager.get_pool()
    logging.info("Got process pool for %s, calling apply()", graph_name)
    try:
        # タイムアウト付きでプロセスプールを使用（ハング回避）
        async_result = pool.apply_async(
            plot_in_subprocess, (config, graph_name, time_start, time_end, figsize, limit_altitude)
        )
        logging.info("Process pool apply_async() called for %s", graph_name)

        result = async_result.get(timeout=timeout_seconds)
        logging.info("Process pool apply_async() returned for %s", graph_name)
        image_bytes, elapsed = result

        if elapsed > 0:
            logging.info("elapsed time: %s = %.3f sec", graph_name, elapsed)
        else:
            logging.info("No data available for %s", graph_name)

        logging.info(
            "plot() returning for %s, size: %d bytes", graph_name, len(image_bytes) if image_bytes else 0
        )

        # キャッシュに保存
        if cache_dir_path and image_bytes:
            save_to_cache(pathlib.Path(cache_dir_path), cache_key, image_bytes)

        return image_bytes
    except multiprocessing.TimeoutError:
        logging.exception("Timeout in plot generation for %s (%d seconds)", graph_name, timeout_seconds)
        msg = f"Plot generation timed out for {graph_name}"
        raise RuntimeError(msg) from None
    except Exception:
        logging.exception("Error in plot generation for %s", graph_name)
        # エラー時は直接エラー画像を生成
        try:
            img = create_no_data_image(config, graph_name, "グラフの作成に失敗しました")
            bytes_io = io.BytesIO()
            img.save(bytes_io, "PNG")
            bytes_io.seek(0)
            return bytes_io.getvalue()
        except Exception:
            # 最終的にフォールバック画像を返す
            logging.exception("Failed to create error image for %s", graph_name)
            return b""


@blueprint.route("/api/refresh-aggregates", methods=["POST"])
def refresh_aggregates():
    """マテリアライズドビュー（集約データ）を更新するAPI"""
    try:
        config = flask.current_app.config["CONFIG"]
        conn = connect_database(config)

        # ビューを更新
        timings = modes.database_postgresql.refresh_materialized_views(conn)

        # 統計情報を取得
        stats = modes.database_postgresql.get_materialized_view_stats(conn)
        conn.close()

        return flask.jsonify({
            "status": "success",
            "refresh_times": timings,
            "stats": stats,
        })

    except Exception as e:
        logging.exception("Error refreshing materialized views")
        return flask.jsonify({"error": "Failed to refresh views", "details": str(e)}), 500


@blueprint.route("/api/aggregate-stats", methods=["GET"])
def aggregate_stats():
    """マテリアライズドビューの統計情報を取得するAPI"""
    try:
        config = flask.current_app.config["CONFIG"]
        conn = connect_database(config)

        # ビューの存在確認
        exists = modes.database_postgresql.check_materialized_views_exist(conn)

        # 統計情報を取得
        stats = modes.database_postgresql.get_materialized_view_stats(conn)
        conn.close()

        return flask.jsonify({
            "exists": exists,
            "stats": stats,
        })

    except Exception as e:
        logging.exception("Error getting aggregate stats")
        return flask.jsonify({"error": "Failed to get stats", "details": str(e)}), 500


@blueprint.route("/api/data-range", methods=["GET"])
def data_range():
    """データベースの最古・最新データの日時を返すAPI"""
    try:
        config = flask.current_app.config["CONFIG"]
        conn = connect_database(config)

        # データ範囲を取得
        result = modes.database_postgresql.fetch_data_range(conn)
        conn.close()

        if result.earliest and result.latest:
            # タイムゾーン情報を追加してJSONシリアライゼーション可能にする
            earliest = result.earliest
            latest = result.latest

            # データベースはJST naive datetimeで保存されている（TZ=Asia/Tokyoのアプリケーション側で統一）
            # タイムゾーン情報がない場合はJSTとして扱う
            if earliest.tzinfo is None:
                earliest = earliest.replace(tzinfo=my_lib.time.get_zoneinfo())
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=my_lib.time.get_zoneinfo())

            response_data = {
                "earliest": earliest.isoformat(),
                "latest": latest.isoformat(),
                "count": result.count,
            }
        else:
            # データがない場合
            response_data = {"earliest": None, "latest": None, "count": 0}

        return flask.jsonify(response_data)

    except Exception as e:
        logging.exception("Error fetching data range")
        return flask.jsonify({"error": "データ範囲の取得に失敗しました", "details": str(e)}), 500


@blueprint.route("/api/graph/<path:graph_name>", methods=["GET"])
def graph(graph_name):  # noqa: PLR0915
    # デフォルト値を設定
    default_time_end = my_lib.time.now()
    default_time_start = default_time_end - datetime.timedelta(days=1)

    # パラメータから時間を取得（JSON文字列として）
    time_end_str = flask.request.args.get("end", None)
    time_start_str = flask.request.args.get("start", None)
    limit_altitude_str = flask.request.args.get("limit_altitude", "false")  # デフォルトでfalse

    # デバッグ: 受信したパラメータを記録
    logging.info(
        "[DEBUG] Raw params for %s: start_str=%r, end_str=%r, limit_altitude_str=%r",
        graph_name,
        time_start_str,
        time_end_str,
        limit_altitude_str,
    )

    # 文字列をUTC時間のdatetimeに変換してからローカルタイムに変換
    if time_end_str:
        try:
            parsed_end = json.loads(time_end_str)
            logging.info("[DEBUG] Parsed end JSON: %r", parsed_end)
            time_end = datetime.datetime.fromisoformat(parsed_end.replace("Z", "+00:00"))
            time_end = time_end.astimezone(my_lib.time.get_zoneinfo())
        except Exception:
            logging.exception("[DEBUG] Failed to parse end time")
            time_end = default_time_end
    else:
        logging.info("[DEBUG] No end param, using default: %s", default_time_end)
        time_end = default_time_end

    if time_start_str:
        try:
            parsed_start = json.loads(time_start_str)
            logging.info("[DEBUG] Parsed start JSON: %r", parsed_start)
            time_start = datetime.datetime.fromisoformat(parsed_start.replace("Z", "+00:00"))
            time_start = time_start.astimezone(my_lib.time.get_zoneinfo())
        except Exception:
            logging.exception("[DEBUG] Failed to parse start time")
            time_start = default_time_start
    else:
        logging.info("[DEBUG] No start param, using default: %s", default_time_start)
        time_start = default_time_start

    # 高度制限パラメータの処理
    limit_altitude = limit_altitude_str.lower() == "true"

    # リクエストの期間を計算
    request_days = (time_end - time_start).total_seconds() / 86400
    logging.info(
        "request: %s graph (start: %s, end: %s, limit_altitude: %s, period: %.2f days)",
        graph_name,
        time_start,
        time_end,
        limit_altitude,
        request_days,
    )

    config = flask.current_app.config["CONFIG"]

    # キャッシュ用の ETag を生成（git commit を含む）
    cache_key = generate_cache_key(graph_name, time_start, time_end, limit_altitude)
    etag = f'"{cache_key}"'

    # 条件付きリクエストのチェック
    if_none_match = flask.request.headers.get("If-None-Match")
    if if_none_match and if_none_match == etag:
        # ETag が一致すれば 304 Not Modified を返す
        logging.info("Returning 304 Not Modified for %s (ETag matched)", graph_name)
        return flask.Response(status=304, headers={"ETag": etag})

    # グラフ生成を試行
    try:
        logging.info("Starting plot generation for %s", graph_name)
        image_bytes = plot(config, graph_name, time_start, time_end, limit_altitude)
        logging.info(
            "Plot generation completed for %s, image size: %d bytes",
            graph_name,
            len(image_bytes) if image_bytes else 0,
        )

        res = flask.Response(image_bytes, mimetype="image/png")
        logging.info("Flask response created for %s", graph_name)

        # ブラウザキャッシュを有効化（30分）
        res.headers["Cache-Control"] = "private, max-age=1800"  # 30分
        res.headers["ETag"] = etag
        res.headers["X-Content-Type-Options"] = "nosniff"

    except Exception as e:
        logging.exception("Error generating graph %s", graph_name)

        # エラー発生時はエラー画像を生成
        import io

        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(12, 8))
        ax.text(
            0.5,
            0.5,
            f"Graph generation failed\n{graph_name}\nError: {str(e)[:100]}...",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=14,
            bbox={"boxstyle": "round,pad=0.3", "facecolor": "lightcoral", "alpha": 0.7},
        )
        ax.axis("off")

        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=IMAGE_DPI, bbox_inches="tight", facecolor="white")
        buf.seek(0)
        error_image_bytes = buf.read()
        buf.close()
        plt.close(fig)

        res = flask.Response(error_image_bytes, mimetype="image/png")

        # エラー時はキャッシュしない（CDN/プロキシも含めて確実に）
        res.headers["Cache-Control"] = "private, no-cache, no-store, must-revalidate, max-age=0"
        res.headers["Pragma"] = "no-cache"
        res.headers["Expires"] = "0"
        res.headers["Vary"] = "Accept, Accept-Encoding"
        res.headers["X-Content-Type-Options"] = "nosniff"
        logging.info("Error response prepared for %s", graph_name)

    logging.info("Returning response for %s", graph_name)
    return res


@blueprint.route("/api/debug/date-parse", methods=["GET"])
def debug_date_parse():  # noqa: PLR0915
    """デバッグ用：日付パース処理をテストするAPI"""
    import json

    time_end_str = flask.request.args.get("end", None)
    time_start_str = flask.request.args.get("start", None)

    result = {
        "raw_params": {
            "start": time_start_str,
            "end": time_end_str,
        },
        "parsed": {},
        "aggregation": {},
        "data_sample": {},
    }

    default_time_end = my_lib.time.now()
    default_time_start = default_time_end - datetime.timedelta(days=1)

    # 日付パース
    if time_end_str:
        try:
            parsed_end = json.loads(time_end_str)
            time_end = datetime.datetime.fromisoformat(parsed_end.replace("Z", "+00:00"))
            time_end = time_end.astimezone(my_lib.time.get_zoneinfo())
            result["parsed"]["end"] = {
                "json_parsed": parsed_end,
                "datetime": str(time_end),
                "utc": str(time_end.astimezone(datetime.timezone.utc)),
            }
        except Exception as e:
            result["parsed"]["end_error"] = str(e)
            time_end = default_time_end
    else:
        time_end = default_time_end
        result["parsed"]["end"] = {"default": str(default_time_end)}

    if time_start_str:
        try:
            parsed_start = json.loads(time_start_str)
            time_start = datetime.datetime.fromisoformat(parsed_start.replace("Z", "+00:00"))
            time_start = time_start.astimezone(my_lib.time.get_zoneinfo())
            result["parsed"]["start"] = {
                "json_parsed": parsed_start,
                "datetime": str(time_start),
                "utc": str(time_start.astimezone(datetime.timezone.utc)),
            }
        except Exception as e:
            result["parsed"]["start_error"] = str(e)
            time_start = default_time_start
    else:
        time_start = default_time_start
        result["parsed"]["start"] = {"default": str(default_time_start)}

    # 期間計算
    period_days = (time_end - time_start).total_seconds() / 86400
    result["period_days"] = period_days

    # 集約レベル
    level = modes.database_postgresql.get_aggregation_level(period_days)
    result["aggregation"] = {
        "table": level.table,
        "time_interval": level.time_interval,
        "altitude_bin": level.altitude_bin,
    }

    # データサンプル取得
    try:
        config = flask.current_app.config["CONFIG"]
        conn = connect_database(config)

        # マテリアライズドビューの存在確認
        view_exists = modes.database_postgresql.check_materialized_views_exist(conn)
        result["views_exist"] = view_exists

        # データ取得テスト（最初の10件のみ）
        if period_days > 7:
            raw_data = modes.database_postgresql.fetch_aggregated_by_time(
                conn, time_start, time_end, max_altitude=None
            )
        else:
            raw_data = modes.database_postgresql.fetch_by_time(
                conn, time_start, time_end, distance=100
            )

        conn.close()

        if raw_data:
            times = [r["time"] for r in raw_data]
            result["data_sample"] = {
                "total_rows": len(raw_data),
                "min_time": str(min(times)),
                "max_time": str(max(times)),
                "first_3": [
                    {k: str(v) if isinstance(v, datetime.datetime) else v for k, v in row.items()}
                    for row in raw_data[:3]
                ],
            }
        else:
            result["data_sample"] = {"error": "No data returned"}

    except Exception as e:
        result["data_sample"] = {"error": str(e)}

    return flask.jsonify(result)


# =============================================================================
# 非同期グラフ生成API
# =============================================================================

# グローバルJobManagerインスタンス
_job_manager = JobManager()


def _parse_datetime_from_request(date_str: str | None) -> datetime.datetime | None:
    """リクエストパラメータから日時をパース"""
    if not date_str:
        return None
    try:
        # ISO形式の文字列をパース
        dt = datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        # JSTに変換
        return dt.astimezone(my_lib.time.get_zoneinfo())
    except Exception:
        logging.exception("Failed to parse datetime: %s", date_str)
        return None


def _start_job_async(  # noqa: PLR0913
    config: dict[str, Any],
    job_id: str,
    graph_name: str,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
) -> None:
    """プロセスプールを使用してジョブを非同期実行（ポーリング方式）"""
    _job_manager.update_status(job_id, JobStatus.PROCESSING, progress=10, stage="開始中...")

    pool = _pool_manager.get_pool()
    figsize = tuple(x / IMAGE_DPI for x in GRAPH_DEF_MAP[graph_name].size)

    # ポーリングスレッドを起動（まだ起動していない場合）
    _start_result_checker_thread()

    # コールバックを使わずにAsyncResultを直接取得
    async_result = pool.apply_async(
        plot_in_subprocess,
        (config, graph_name, time_start, time_end, figsize, limit_altitude),
    )

    # 保留中の結果リストに追加（ポーリングスレッドが監視）
    with _async_results_lock:
        _pending_async_results[job_id] = (async_result, graph_name)

    logging.info("Started async job %s for %s (polling mode)", job_id, graph_name)


@blueprint.route("/api/graph/job", methods=["POST"])
def create_graph_job():
    """
    グラフ生成ジョブを登録

    Request Body:
    {
        "graphs": ["scatter_2d", "contour_2d", ...],  // 複数のグラフ名
        "start": "2025-01-01T00:00:00Z",             // ISO形式
        "end": "2025-01-07T00:00:00Z",               // ISO形式
        "limit_altitude": false                       // 高度制限フラグ
    }

    Response:
    {
        "jobs": [
            {"job_id": "uuid-1", "graph_name": "scatter_2d"},
            {"job_id": "uuid-2", "graph_name": "contour_2d"},
            ...
        ]
    }
    """
    try:
        data = flask.request.get_json()
        if not data:
            return flask.jsonify({"error": "Request body is required"}), 400

        # パラメータ解析
        time_start = _parse_datetime_from_request(data.get("start"))
        time_end = _parse_datetime_from_request(data.get("end"))
        limit_altitude = data.get("limit_altitude", False)

        if not time_start or not time_end:
            return flask.jsonify({"error": "start and end are required"}), 400

        # グラフ名のリストを取得
        graphs = data.get("graphs", [])
        if not graphs:
            return flask.jsonify({"error": "graphs list is required"}), 400

        config = flask.current_app.config["CONFIG"]
        jobs = []

        for graph_name in graphs:
            if graph_name not in GRAPH_DEF_MAP:
                logging.warning("Unknown graph name: %s", graph_name)
                continue

            job_id = _job_manager.create_job(graph_name, time_start, time_end, limit_altitude)
            jobs.append({"job_id": job_id, "graph_name": graph_name})

            # プロセスプールでジョブを開始
            _start_job_async(config, job_id, graph_name, time_start, time_end, limit_altitude)

        return flask.jsonify({"jobs": jobs})

    except Exception as e:
        logging.exception("Error creating graph jobs")
        return flask.jsonify({"error": str(e)}), 500


@blueprint.route("/api/graph/job/<job_id>/status", methods=["GET"])
def get_job_status(job_id: str):
    """
    ジョブステータスを取得

    Response:
    {
        "job_id": "uuid",
        "status": "processing",  // pending, processing, completed, failed, timeout
        "progress": 50,          // 0-100
        "graph_name": "scatter_2d",
        "error": null,
        "elapsed_seconds": 12.5  // 処理開始からの経過時間
    }
    """
    status_dict = _job_manager.get_job_status_dict(job_id)

    if not status_dict:
        return flask.jsonify({"error": "Job not found"}), 404

    return flask.jsonify(status_dict)


@blueprint.route("/api/graph/jobs/status", methods=["POST"])
def get_jobs_status_batch():
    """
    複数ジョブのステータスを一括取得（ポーリング効率化）

    Request Body:
    {
        "job_ids": ["uuid-1", "uuid-2", ...]
    }

    Response:
    {
        "jobs": {
            "uuid-1": {"status": "completed", "progress": 100, ...},
            "uuid-2": {"status": "processing", "progress": 45, ...}
        }
    }
    """
    try:
        data = flask.request.get_json()
        if not data:
            return flask.jsonify({"error": "Request body is required"}), 400

        job_ids = data.get("job_ids", [])
        results: dict[str, dict[str, Any]] = {}

        for job_id in job_ids:
            status_dict = _job_manager.get_job_status_dict(job_id)
            if status_dict:
                # job_idはキーとして使うので、辞書から除外
                results[job_id] = {
                    "status": status_dict["status"],
                    "progress": status_dict["progress"],
                    "graph_name": status_dict["graph_name"],
                    "error": status_dict["error"],
                    "elapsed_seconds": status_dict["elapsed_seconds"],
                    "stage": status_dict["stage"],
                }

        return flask.jsonify({"jobs": results})

    except Exception as e:
        logging.exception("Error getting jobs status")
        return flask.jsonify({"error": str(e)}), 500


@blueprint.route("/api/graph/job/<job_id>/result", methods=["GET"])
def get_job_result(job_id: str):
    """
    ジョブ結果（PNG画像）を取得

    Response: image/png または JSON error
    """
    job = _job_manager.get_job(job_id)

    if not job:
        return flask.jsonify({"error": "Job not found"}), 404

    if job.status in {JobStatus.PENDING, JobStatus.PROCESSING}:
        return (
            flask.jsonify(
                {"error": "Job not completed", "status": job.status.value, "progress": job.progress}
            ),
            202,
        )  # Accepted but not ready

    if job.status in {JobStatus.FAILED, JobStatus.TIMEOUT}:
        return (
            flask.jsonify({"error": job.error or "Job failed", "status": job.status.value}),
            500,
        )

    # 完了した場合は画像を返す
    if not job.result:
        return flask.jsonify({"error": "No result available"}), 500

    res = flask.Response(job.result, mimetype="image/png")
    res.headers["Cache-Control"] = "private, max-age=600"  # 10分間キャッシュ可能
    return res


@blueprint.route("/api/graph/jobs/stats", methods=["GET"])
def get_jobs_stats():
    """
    ジョブ統計情報を取得（デバッグ用）

    Response:
    {
        "pending": 2,
        "processing": 3,
        "completed": 10,
        "failed": 1,
        "total": 16
    }
    """
    stats = _job_manager.get_stats()
    return flask.jsonify(stats)


if __name__ == "__main__":

    def plot_local(raw_data):
        data = prepare_data(raw_data)

        if data is None:
            logging.warning("プロット用のデータがありません")
            return

        set_font(config["font"])

        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
            futures: dict[str, concurrent.futures.Future] = {}
            for graph_name, graph_def in GRAPH_DEF_MAP.items():
                figsize = tuple(x / IMAGE_DPI for x in graph_def.size)
                futures[graph_name] = executor.submit(graph_def.func, data, figsize)

            for graph_name, graph_def in GRAPH_DEF_MAP.items():
                img, elapsed = futures[graph_name].result()
                img.save(graph_def.file)

                logging.info("elapsed time: %s = %.3f sec", graph_name, elapsed)

    import docopt
    import my_lib.config
    import my_lib.logger
    import my_lib.time

    import modes.database_postgresql

    assert __doc__ is not None  # noqa: S101
    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    period_days = int(args["-p"])
    debug_mode = args["-D"]

    my_lib.logger.init("modes sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = my_lib.config.load(config_file)

    conn = modes.database_postgresql.open(
        config["database"]["host"],
        config["database"]["port"],
        config["database"]["name"],
        config["database"]["user"],
        config["database"]["pass"],
    )
    time_end = my_lib.time.now()
    time_start = time_end - datetime.timedelta(days=period_days)

    plot_local(
        modes.database_postgresql.fetch_by_time(
            conn,
            time_start,
            time_end,
            config["filter"]["area"]["distance"],
            columns=[
                "time",
                "altitude",
                "temperature",
                "distance",
                "wind_x",
                "wind_y",
                "wind_speed",
                "wind_angle",
            ],
        )
    )
