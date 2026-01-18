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
import functools
import gc
import io
import json
import logging
import multiprocessing
import multiprocessing.pool
import pathlib
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

import flask
import matplotlib

matplotlib.use("Agg")  # pyplotのimport前に設定する必要がある
import matplotlib.dates
import matplotlib.font_manager
import matplotlib.pyplot
import matplotlib.ticker
import mpl_toolkits.mplot3d  # noqa: F401
import my_lib.git_util
import my_lib.panel_config
import my_lib.pil_util
import my_lib.plot_util
import my_lib.time
import numpy
import pandas
import PIL.Image
import PIL.ImageDraw
import PIL.ImageFont
import scipy.interpolate

import amdar.config
import amdar.database.postgresql
import amdar.viewer.api.progress_estimation
from amdar.constants import (
    CACHE_CONTROL_MAX_AGE_RESULT,
    CACHE_CONTROL_MAX_AGE_STATUS,
    CACHE_START_TIME_TOLERANCE_SECONDS,
    CACHE_TTL_SECONDS,
    DEFAULT_DISTANCE_KM,
    ETAG_TIME_ROUND_SECONDS,
    GRAPH_ALT_MAX,
    GRAPH_ALT_MIN,
    GRAPH_ALTITUDE_LIMIT,
    GRAPH_GEN_TIMEOUT_7DAYS_SECONDS,
    GRAPH_GEN_TIMEOUT_30DAYS_SECONDS,
    GRAPH_GEN_TIMEOUT_90DAYS_SECONDS,
    GRAPH_GEN_TIMEOUT_OVER90DAYS_SECONDS,
    GRAPH_IMAGE_DPI,
    GRAPH_JOB_TIMEOUT_BUFFER_SECONDS,
    GRAPH_TEMP_MAX_DEFAULT,
    GRAPH_TEMP_MAX_LIMITED,
    GRAPH_TEMP_MIN_DEFAULT,
    GRAPH_TEMP_MIN_LIMITED,
    GRAPH_TEMPERATURE_THRESHOLD,
    GraphName,
)
from amdar.viewer.api.job_manager import JobManager, JobStatus

if TYPE_CHECKING:
    from collections.abc import Callable

    from matplotlib.axes import Axes
    from psycopg2.extensions import connection as PgConnection


@dataclass
class PreparedData:
    """準備済みデータ

    DataFrameは風向グラフでのみ使用するため、遅延作成する。
    """

    count: int
    times: numpy.ndarray
    time_numeric: numpy.ndarray
    altitudes: numpy.ndarray
    temperatures: numpy.ndarray
    wind_x: numpy.ndarray = field(default_factory=lambda: numpy.array([], dtype=numpy.float32))
    wind_y: numpy.ndarray = field(default_factory=lambda: numpy.array([], dtype=numpy.float32))
    wind_speed: numpy.ndarray = field(default_factory=lambda: numpy.array([], dtype=numpy.float32))
    wind_angle: numpy.ndarray = field(default_factory=lambda: numpy.array([], dtype=numpy.float32))
    _dataframe: pandas.DataFrame | None = field(default=None, repr=False)

    @property
    def dataframe(self) -> pandas.DataFrame:
        """風向グラフ用DataFrameを遅延作成"""
        if self._dataframe is not None:
            return self._dataframe

        if self.count == 0:
            self._dataframe = pandas.DataFrame()
            return self._dataframe

        df_data: dict[str, numpy.ndarray] = {
            "time": self.times,
            "time_numeric": self.time_numeric,
            "altitude": self.altitudes,
            "temperature": self.temperatures,
        }
        if len(self.wind_x) > 0:
            df_data["wind_x"] = self.wind_x
            df_data["wind_y"] = self.wind_y
            df_data["wind_speed"] = self.wind_speed
            df_data["wind_angle"] = self.wind_angle

        self._dataframe = pandas.DataFrame(df_data)
        return self._dataframe


@dataclass
class WindFilteredData:
    """風データフィルタリング結果"""

    altitudes: numpy.ndarray
    wind_x: numpy.ndarray
    wind_y: numpy.ndarray
    time_numeric: numpy.ndarray


@dataclass
class GridData:
    """補間グリッドデータ

    _create_grid 関数から返される、等高線・ヒートマップ描画用のグリッドデータ。
    """

    time_mesh: numpy.ndarray
    """時間軸のメッシュグリッド"""
    alt_mesh: numpy.ndarray
    """高度軸のメッシュグリッド"""
    temp_grid: numpy.ndarray
    """温度の補間グリッド"""
    time_min: float
    """時間範囲の最小値"""
    time_max: float
    """時間範囲の最大値"""
    alt_min: float
    """高度範囲の最小値"""
    alt_max: float
    """高度範囲の最大値"""


def _get_font_config(font_config: amdar.config.FontConfig) -> my_lib.panel_config.FontConfig:
    """FontConfigをmy_lib.panel_config.FontConfigに変換する"""
    return my_lib.panel_config.FontConfig(
        path=font_config.path,
        map=font_config.map,
    )


def get_temperature_range(limit_altitude: bool = False) -> tuple[int, int]:
    """limit_altitudeに応じた温度範囲を取得"""
    if limit_altitude:
        return GRAPH_TEMP_MIN_LIMITED, GRAPH_TEMP_MAX_LIMITED
    return GRAPH_TEMP_MIN_DEFAULT, GRAPH_TEMP_MAX_DEFAULT


_TICK_LABEL_SIZE = 8
_CONTOUR_SIZE = 8
_ERROR_SIZE = 30

_AXIS_LABEL_SIZE = 12
_TITLE_SIZE = 20

_TIME_AXIS_LABEL = "日時"
_ALT_AXIS_LABEL = "高度 (m)"
_TEMP_AXIS_LABEL = "温度 (℃)"

blueprint = flask.Blueprint("modes-sensing-graph", __name__)


# グローバルプロセスプール管理（matplotlib マルチスレッド問題対応）
class ProcessPoolManager:
    """シングルトンパターンでプロセスプールを管理"""

    _instance: ProcessPoolManager | None = None
    # NOTE: シングルトンの同期にはthreading.Lockを使用
    # multiprocessing.Lockはfork時にIPCプリミティブの問題を起こす可能性がある
    _lock = threading.Lock()
    pool: multiprocessing.pool.Pool | None

    def __new__(cls) -> ProcessPoolManager:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance.pool = None
                    cls._instance = instance
        assert cls._instance is not None  # noqa: S101 (シングルトンパターン)
        return cls._instance

    def get_pool(self):
        """プロセスプールを取得（必要に応じて作成）"""
        if self.pool is None:
            with self._lock:
                if self.pool is None:
                    # CPUコア数に基づいてプロセス数を決定（最大10、最小2）
                    # 最小2: Kubernetes Pod等でCPU制限されている環境でも並列性を確保
                    max_workers = min(max(multiprocessing.cpu_count() // 2, 2), 10)
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
# job_id -> (async_result, graph_name, cache_dir)
_pending_async_results: dict[str, tuple[multiprocessing.pool.AsyncResult, GraphName, pathlib.Path]] = {}
_async_results_lock = threading.Lock()
_result_checker_started = False


def _start_result_checker_thread() -> None:
    """非同期ジョブの完了をポーリングするバックグラウンドスレッドを開始"""
    global _result_checker_started
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
        for job_id, (async_result, graph_name, cache_dir) in list(_pending_async_results.items()):
            if not _check_single_job(job_id, async_result, cast(GraphName, graph_name), cache_dir):
                continue
            completed_jobs.append(job_id)

        for job_id in completed_jobs:
            del _pending_async_results[job_id]


def _estimate_progress_and_stage(job_id: str) -> tuple[int, str]:
    """ジョブの進捗を推定して返す

    履歴ベースの推定時間を使用し、履歴がない場合はデフォルト値を使用。
    推定時間には3秒のバッファを追加。

    """
    job = _job_manager.get_job(job_id)
    if not job or not job.started_at:
        return 10, "開始中..."

    elapsed = time.time() - job.started_at

    # 期間を時間単位で計算
    duration_hours = (job.time_end - job.time_start).total_seconds() / 3600

    # 履歴から推定時間を取得（+3秒のバッファ）
    history = amdar.viewer.api.progress_estimation.generation_time_history
    estimated_total = history.get_estimated_time(job.graph_name, duration_hours, job.limit_altitude) + 3.0

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


def _check_single_job(
    job_id: str,
    async_result: multiprocessing.pool.AsyncResult,
    graph_name: GraphName,
    cache_dir: pathlib.Path,
) -> bool:
    """単一のジョブをチェックし、完了していればTrueを返す"""
    try:
        if not async_result.ready():
            # タイムアウト検出: ジョブがキュー待ちまたは実行中で時間超過している場合
            job = _job_manager.get_job(job_id)
            if job and job.started_at:
                elapsed = time.time() - job.started_at
                # 期間に応じたタイムアウト（_calculate_timeout と同じロジック + バッファ）
                days = (job.time_end - job.time_start).total_seconds() / 86400
                if days <= 7:
                    max_timeout = GRAPH_GEN_TIMEOUT_7DAYS_SECONDS + GRAPH_JOB_TIMEOUT_BUFFER_SECONDS
                elif days <= 30:
                    max_timeout = GRAPH_GEN_TIMEOUT_30DAYS_SECONDS + GRAPH_JOB_TIMEOUT_BUFFER_SECONDS
                elif days <= 90:
                    max_timeout = GRAPH_GEN_TIMEOUT_90DAYS_SECONDS + GRAPH_JOB_TIMEOUT_BUFFER_SECONDS
                else:
                    max_timeout = GRAPH_GEN_TIMEOUT_OVER90DAYS_SECONDS + GRAPH_JOB_TIMEOUT_BUFFER_SECONDS

                if elapsed > max_timeout:
                    logging.warning(
                        "Job %s for %s timed out after %.1f sec (max: %d sec, queued or stuck)",
                        job_id,
                        graph_name,
                        elapsed,
                        max_timeout,
                    )
                    _job_manager.update_status(
                        job_id,
                        JobStatus.TIMEOUT,
                        error=f"ジョブがタイムアウトしました（{int(elapsed)}秒経過）",
                        stage="タイムアウト",
                    )
                    return True

            # 未完了の場合は進捗を更新
            progress, stage = _estimate_progress_and_stage(job_id)
            _job_manager.update_status(job_id, JobStatus.PROCESSING, progress=progress, stage=stage)
            return False

        try:
            result = async_result.get(timeout=1)
            image_bytes, elapsed = result
            _job_manager.update_status(
                job_id, JobStatus.COMPLETED, result=image_bytes, progress=100, stage="完了"
            )
            logging.info("Job %s completed for %s (%.2f sec) via polling", job_id, graph_name, elapsed)

            # キャッシュに保存・生成時間を記録
            job = _job_manager.get_job(job_id)
            if job:
                if image_bytes:
                    save_to_cache(
                        cache_dir, graph_name, job.time_start, job.time_end, job.limit_altitude, image_bytes
                    )
                # 生成時間を履歴に記録
                duration_hours = (job.time_end - job.time_start).total_seconds() / 3600
                amdar.viewer.api.progress_estimation.generation_time_history.record(
                    graph_name, duration_hours, job.limit_altitude, elapsed
                )
        except Exception:
            logging.exception("Job %s failed for %s", job_id, graph_name)
            _job_manager.update_status(job_id, JobStatus.FAILED, error="Job execution failed", stage="エラー")
        return True
    except Exception:
        logging.exception("Error checking job %s", job_id)
        return True


def _connect_database(config: amdar.config.Config) -> PgConnection:
    return amdar.database.postgresql.open(
        config.database.host,
        config.database.port,
        config.database.name,
        config.database.user,
        config.database.password,
    )


def _set_title(title_text: str) -> None:
    matplotlib.pyplot.title(title_text, fontsize=_TITLE_SIZE, fontweight="bold", pad=20)


def _set_tick_label_size(ax: Axes, is_3d: bool = False) -> None:
    ax.tick_params(axis="x", labelsize=_TICK_LABEL_SIZE)
    ax.tick_params(axis="y", labelsize=_TICK_LABEL_SIZE)
    if is_3d:
        ax.tick_params(axis="z", labelsize=_TICK_LABEL_SIZE)  # type: ignore[arg-type]


def _set_axis_labels(
    ax: Axes,
    xlabel: str | None = None,
    ylabel: str | None = None,
    zlabel: str | None = None,
) -> None:
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=_AXIS_LABEL_SIZE)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=_AXIS_LABEL_SIZE)
    if zlabel:
        ax.set_zlabel(zlabel, fontsize=_AXIS_LABEL_SIZE)  # type: ignore[attr-defined]


def _set_temperature_range(ax: Axes, axis: str = "x", limit_altitude: bool = False) -> None:
    # limit_altitudeに応じた温度範囲を動的に取得
    temp_min, temp_max = get_temperature_range(limit_altitude)

    if axis == "x":
        ax.set_xlim(temp_min, temp_max)
    else:
        ax.set_ylim(temp_min, temp_max)


def _set_altitude_range(ax: Axes, axis: str = "x", limit_altitude: bool = False) -> None:
    alt_max = GRAPH_ALTITUDE_LIMIT if limit_altitude else GRAPH_ALT_MAX
    if axis == "x":
        ax.set_xlim(GRAPH_ALT_MIN, alt_max)
    else:
        ax.set_ylim(GRAPH_ALT_MIN, alt_max)


def _apply_time_axis_format(ax: Axes, time_range_days: float) -> None:
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


def _apply_time_axis_format_3d(ax: Any, time_numeric: numpy.ndarray) -> None:
    """3Dグラフ用の時間軸フォーマット（目盛り間引き対応）

    3Dグラフでは matplotlib.dates の Locator が正しく動作しないため、
    手動で目盛り位置とラベルを設定する。
    """
    import matplotlib.dates

    if len(time_numeric) == 0:
        return

    time_min = time_numeric.min()
    time_max = time_numeric.max()
    time_range_days = time_max - time_min

    # 期間に応じて目盛り数を決定（2Dグラフと同様のロジック）
    if time_range_days <= 1:
        # 1日以内: 3時間間隔
        interval_days = 3 / 24
        date_format = "%-H時"
    elif time_range_days <= 3:
        # 3日以内: 1日間隔
        interval_days = 1
        date_format = "%-d日"
    elif time_range_days <= 7:
        # 7日以内: 2日間隔
        interval_days = 2
        date_format = "%-m月%-d日"
    elif time_range_days <= 30:
        # 1ヶ月以内: 約5-6個の目盛り
        interval_days = max(1, int(time_range_days / 5))
        date_format = "%-m月%-d日"
    elif time_range_days <= 90:
        # 3ヶ月以内: 約5-6個の目盛り
        interval_days = max(7, int(time_range_days / 6))
        date_format = "%-m月%-d日"
    else:
        # それ以上: 約5-6個の目盛り
        interval_days = max(14, int(time_range_days / 5))
        date_format = "%-m月%-d日"

    # 目盛り位置を計算
    tick_positions = []
    tick_labels = []
    current = time_min
    while current <= time_max:
        tick_positions.append(current)
        # matplotlib の日付数値から datetime に変換
        dt = matplotlib.dates.num2date(current)
        tick_labels.append(dt.strftime(date_format))
        current += interval_days

    # 最後の目盛りが time_max に近い場合は追加しない（重複防止）
    if tick_positions and (time_max - tick_positions[-1]) < interval_days * 0.3:
        pass  # 最後の目盛りをそのまま使用
    elif time_max - tick_positions[-1] > interval_days * 0.5:
        # 最後の目盛りと time_max の間隔が大きい場合は追加
        tick_positions.append(time_max)
        dt = matplotlib.dates.num2date(time_max)
        tick_labels.append(dt.strftime(date_format))

    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels)


def _append_colorbar(scatter, shrink=0.8, pad=0.01, aspect=35, fraction=0.046, limit_altitude=False):
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
    cbar.set_label(_TEMP_AXIS_LABEL, fontsize=_AXIS_LABEL_SIZE)
    _set_tick_label_size(cbar.ax)

    return cbar


def _create_grid(
    time_numeric: numpy.ndarray,
    altitudes: numpy.ndarray,
    temperatures: numpy.ndarray,
    grid_points: int = 100,
    time_range: tuple[float, float] | None = None,
    limit_altitude: bool = False,
) -> GridData:
    """グリッド作成を最適化（データ前処理改善、メモリ効率向上）

    Args:
        time_numeric: 時間データ（matplotlib日付数値）
        altitudes: 高度データ
        temperatures: 温度データ
        grid_points: グリッドポイント数
        time_range: 時間範囲 (min, max)
        limit_altitude: 高度制限フラグ

    Returns:
        GridData: 補間グリッドデータ
    """
    # データが既にprepare_dataで前処理されているため、追加フィルタリングは最小限
    if len(time_numeric) == 0:
        # 空データの場合
        time_min, time_max = 0.0, 1.0
        alt_min = float(GRAPH_ALT_MIN)
        if limit_altitude:
            alt_max = float(GRAPH_ALTITUDE_LIMIT)
            alt_grid_points = int((alt_max - alt_min) / 50) + 1
        else:
            alt_max = float(GRAPH_ALT_MAX)
            alt_grid_points = grid_points

        time_grid = numpy.linspace(time_min, time_max, grid_points)
        alt_grid = numpy.linspace(alt_min, alt_max, alt_grid_points)
        time_mesh, alt_mesh = numpy.meshgrid(time_grid, alt_grid, indexing="xy")
        temp_grid = numpy.full_like(time_mesh, numpy.nan)

        return GridData(
            time_mesh=time_mesh,
            alt_mesh=alt_mesh,
            temp_grid=temp_grid,
            time_min=time_min,
            time_max=time_max,
            alt_min=alt_min,
            alt_max=alt_max,
        )

    # グリッド範囲設定
    if time_range is not None:
        time_min, time_max = time_range
        # 実際のデータ範囲に制限
        actual_time_min, actual_time_max = float(time_numeric.min()), float(time_numeric.max())
        time_min = max(time_min, actual_time_min)
        time_max = min(time_max, actual_time_max)
    else:
        time_min, time_max = float(time_numeric.min()), float(time_numeric.max())

    # 高度範囲とグリッド密度をlimit_altitudeに応じて設定
    alt_min = float(GRAPH_ALT_MIN)
    if limit_altitude:
        alt_max = float(GRAPH_ALTITUDE_LIMIT)  # 2000m
        # 50m刻みにするため、2000m / 50m = 40点の高度グリッド
        alt_grid_points = int((alt_max - alt_min) / 50) + 1
    else:
        alt_max = float(GRAPH_ALT_MAX)  # 13000m
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

    return GridData(
        time_mesh=time_mesh,
        alt_mesh=alt_mesh,
        temp_grid=temp_grid,
        time_min=time_min,
        time_max=time_max,
        alt_min=alt_min,
        alt_max=alt_max,
    )


def _create_figure(figsize=(12, 8)):
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


def _set_axis_2d_default(ax, time_range, limit_altitude=False):
    _set_axis_labels(ax, _TIME_AXIS_LABEL, _ALT_AXIS_LABEL)

    _set_altitude_range(ax, axis="y", limit_altitude=limit_altitude)

    # 高度軸の目盛りを設定（limit_altitude=Trueの場合は200m間隔）
    if limit_altitude:
        ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(200))
    else:
        ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(2000))

    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    _set_tick_label_size(ax)

    _apply_time_axis_format(
        ax, float(matplotlib.dates.date2num(time_range[-1]) - matplotlib.dates.date2num(time_range[0]))
    )


def _conver_to_img(fig):
    buf = io.BytesIO()
    matplotlib.pyplot.savefig(buf, format="png", dpi=GRAPH_IMAGE_DPI, facecolor="white", transparent=False)

    buf.seek(0)

    img = PIL.Image.open(buf).copy()

    buf.close()

    # メモリ解放: Figure を即座にクローズ
    matplotlib.pyplot.clf()
    matplotlib.pyplot.close(fig)
    matplotlib.pyplot.close("all")  # 残存する Figure も全てクローズ

    return img


def _create_no_data_image(config, graph_name, text="データがありません"):
    """データがない場合の画像を生成する"""
    # グラフサイズを取得
    size = GRAPH_DEF_MAP[graph_name].size

    # 新しい画像を作成（白背景）
    img = PIL.Image.new("RGB", size, color="white")

    # フォントサイズをDPIに合わせて調整（20pt）
    font_size = int(_ERROR_SIZE * GRAPH_IMAGE_DPI / 72)

    # my_lib.pil_utilを使用してフォントを取得
    font_config = _get_font_config(config.font)
    font = my_lib.pil_util.get_font(font_config, "jp_bold", font_size)

    pos = (size[0] // 2, size[1] // 2)

    my_lib.pil_util.draw_text(img, text, pos, font, align="center", color="#666")

    return img


def _prepare_data(raw_data) -> PreparedData:
    """データ前処理を最適化（無効データ除去、メモリ効率向上）

    注意: 現在は未使用。本番では _prepare_data_numpy を使用。
    ローカルテスト用に残している。
    """
    empty_array = numpy.array([], dtype=numpy.float32)

    if not raw_data:
        return PreparedData(
            count=0,
            times=numpy.array([], dtype="datetime64[us]"),
            time_numeric=empty_array,
            altitudes=empty_array,
            temperatures=empty_array,
        )

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
        (temperatures > GRAPH_TEMPERATURE_THRESHOLD)
        & (numpy.isfinite(temperatures))
        & (numpy.isfinite(altitudes))
        & (altitudes >= GRAPH_ALT_MIN)
        & (altitudes <= GRAPH_ALT_MAX)
    )

    if not valid_mask.any():
        return PreparedData(
            count=0,
            times=numpy.array([], dtype="datetime64[us]"),
            time_numeric=empty_array,
            altitudes=empty_array,
            temperatures=empty_array,
        )

    # 有効データのみを連続メモリ配置で抽出
    valid_indices = numpy.where(valid_mask)[0]
    valid_count = len(valid_indices)

    # 連続メモリ配列として確保（float32でメモリ効率向上）
    clean_temperatures = numpy.ascontiguousarray(temperatures[valid_mask], dtype=numpy.float32)
    clean_altitudes = numpy.ascontiguousarray(altitudes[valid_mask], dtype=numpy.float32)

    # 時間データの効率的処理
    times_list = [raw_data[i]["time"] for i in valid_indices]

    # pandas.to_datetimeの最適化設定
    times = pandas.to_datetime(times_list, utc=False, cache=True).to_numpy()

    # matplotlib.dates.date2numをベクトル化
    time_numeric = numpy.ascontiguousarray(matplotlib.dates.date2num(times))

    # DataFrame はローカルテスト用に直接設定
    filtered_records = [raw_data[i] for i in valid_indices] if valid_count < data_length else raw_data
    clean_df = pandas.DataFrame(filtered_records) if filtered_records else pandas.DataFrame()

    result = PreparedData(
        count=valid_count,
        times=times,
        time_numeric=time_numeric,
        altitudes=clean_altitudes,
        temperatures=clean_temperatures,
    )
    # ローカルテスト用にDataFrameを直接設定
    result._dataframe = clean_df
    return result


def _prepare_data_numpy(numpy_data: amdar.database.postgresql.NumpyFetchResult) -> PreparedData:
    """NumPy配列形式のデータから描画用データを準備する（高速版）

    fetch_by_time_numpy / fetch_aggregated_numpy から返されたデータを
    グラフ描画用の形式に変換する。Pythonループを使わずベクトル化処理のみ。

    最適化:
    - float32 でメモリ効率化（グラフ描画には十分な精度）
    - DataFrame は風向グラフでのみ使用するため遅延作成

    Args:
        numpy_data: fetch_by_time_numpy / fetch_aggregated_numpy から返された NumpyFetchResult

    Returns:
        グラフ描画用のPreparedData

    """
    empty_float_array = numpy.array([], dtype=numpy.float32)

    if numpy_data.count == 0:
        return PreparedData(
            count=0,
            times=numpy.array([], dtype="datetime64[us]"),
            time_numeric=empty_float_array,
            altitudes=empty_float_array,
            temperatures=empty_float_array,
        )

    times = numpy_data.time
    altitudes = numpy_data.altitude
    temperatures = numpy_data.temperature

    # 複合条件による無効データフィルタリング（ベクトル化）
    valid_mask = (
        (temperatures > GRAPH_TEMPERATURE_THRESHOLD)
        & numpy.isfinite(temperatures)
        & numpy.isfinite(altitudes)
        & (altitudes >= GRAPH_ALT_MIN)
        & (altitudes <= GRAPH_ALT_MAX)
    )

    valid_count = numpy.count_nonzero(valid_mask)

    if valid_count == 0:
        return PreparedData(
            count=0,
            times=numpy.array([], dtype="datetime64[us]"),
            time_numeric=empty_float_array,
            altitudes=empty_float_array,
            temperatures=empty_float_array,
        )

    # 有効データのみを連続メモリ配置で抽出（ベクトル化、float32でメモリ効率化）
    clean_times = times[valid_mask]
    clean_altitudes = numpy.ascontiguousarray(altitudes[valid_mask], dtype=numpy.float32)
    clean_temperatures = numpy.ascontiguousarray(temperatures[valid_mask], dtype=numpy.float32)

    # datetime64[us] から matplotlib の date number に変換（ベクトル化）
    # matplotlib 3.3以降: date number のエポックは 1970-01-01 = 0.0
    # numpy の datetime64[us] は 1970-01-01 からのマイクロ秒
    # 注意: time_numeric は日付計算の精度が必要なため float64 を維持
    time_numeric = clean_times.astype("float64") / (86400 * 1e6)
    time_numeric = numpy.ascontiguousarray(time_numeric)

    # 風データの処理（float32でメモリ効率化）
    if (
        numpy_data.wind_x is not None
        and numpy_data.wind_y is not None
        and numpy_data.wind_speed is not None
        and numpy_data.wind_angle is not None
    ):
        wind_x = numpy.ascontiguousarray(numpy_data.wind_x[valid_mask], dtype=numpy.float32)
        wind_y = numpy.ascontiguousarray(numpy_data.wind_y[valid_mask], dtype=numpy.float32)
        wind_speed = numpy.ascontiguousarray(numpy_data.wind_speed[valid_mask], dtype=numpy.float32)
        wind_angle = numpy.ascontiguousarray(numpy_data.wind_angle[valid_mask], dtype=numpy.float32)
    else:
        wind_x = empty_float_array
        wind_y = empty_float_array
        wind_speed = empty_float_array
        wind_angle = empty_float_array

    # DataFrame は PreparedData.dataframe プロパティで遅延作成
    return PreparedData(
        count=valid_count,
        times=clean_times,
        time_numeric=time_numeric,
        altitudes=clean_altitudes,
        temperatures=clean_temperatures,
        wind_x=wind_x,
        wind_y=wind_y,
        wind_speed=wind_speed,
        wind_angle=wind_angle,
    )


def set_font(font_config_src: amdar.config.FontConfig) -> None:
    try:
        font_config = _get_font_config(font_config_src)

        for font_file in font_config.map.values():
            matplotlib.font_manager.fontManager.addfont(font_config.path.resolve() / font_file)

        font_name = my_lib.plot_util.get_plot_font(font_config, "jp_medium", 12).get_name()

        matplotlib.pyplot.rcParams["font.family"] = [font_name, "sans-serif"]
        matplotlib.pyplot.rcParams["font.sans-serif"] = [font_name] + matplotlib.pyplot.rcParams[
            "font.sans-serif"
        ]
    except Exception:
        logging.exception("Failed to set font")


def _set_axis_3d(ax, time_numeric, limit_altitude=False):
    _set_axis_labels(ax, _TIME_AXIS_LABEL, _ALT_AXIS_LABEL, _TEMP_AXIS_LABEL)

    # 3D用の時間軸フォーマット（期間に応じた目盛り間引き）
    _apply_time_axis_format_3d(ax, time_numeric)

    # 高度軸の最大値を設定
    alt_max = GRAPH_ALTITUDE_LIMIT if limit_altitude else GRAPH_ALT_MAX

    # 高度軸の目盛りを設定（limit_altitude=Trueの場合は200m間隔）
    if limit_altitude:
        ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(200))
    else:
        ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(2000))

    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

    _set_tick_label_size(ax, is_3d=True)

    ax.set_ylim(GRAPH_ALT_MIN, alt_max)
    # 温度軸の範囲設定（limit_altitudeによって変更）
    temp_min, temp_max = get_temperature_range(limit_altitude)
    ax.set_zlim(temp_min, temp_max)


def _create_3d_figure(figsize=(12, 8)):
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


def _setup_3d_colorbar_and_layout(ax):
    """3Dプロットの余白とレイアウトを最適化"""
    ax.view_init(elev=25, azim=35)
    # 3Dプロットの位置を調整（左、下、幅、高さ）
    # プロットエリアを拡大（幅を0.82から0.86に）
    ax.set_position([0.02, 0.05, 0.86, 0.88])


def _plot_scatter_3d(data, figsize, limit_altitude=False):
    logging.info("Starting plot scatter 3d (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    fig, ax = _create_3d_figure(figsize)
    scatter = ax.scatter(
        data.time_numeric,
        data.altitudes,
        data.temperatures,
        c=data.temperatures,
        cmap="plasma",
        marker="o",
        s=15,
        alpha=0.9,
        rasterized=True,
        edgecolors="none",
    )

    _set_axis_3d(ax, data.time_numeric, limit_altitude)
    _append_colorbar(scatter, shrink=0.6, pad=0.01, aspect=35, limit_altitude=limit_altitude)
    _setup_3d_colorbar_and_layout(ax)

    _set_title("航空機の気象データ (3D)")

    img = _conver_to_img(fig)

    return (img, time.perf_counter() - start)


def _plot_density(data, figsize, limit_altitude=False):
    logging.info("Starting plot density (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    fig, ax = _create_figure(figsize)

    scatter = ax.scatter(
        data.altitudes,
        data.temperatures,
        c=data.temperatures,
        cmap="plasma",
        s=15,
        alpha=0.9,
        rasterized=True,
        edgecolors="none",
    )

    _set_axis_labels(ax, _ALT_AXIS_LABEL, _TEMP_AXIS_LABEL)
    _set_altitude_range(ax, axis="x", limit_altitude=limit_altitude)
    _set_temperature_range(ax, axis="y", limit_altitude=limit_altitude)
    _set_tick_label_size(ax)

    _append_colorbar(scatter, shrink=1.0, pad=0.01, aspect=35, fraction=0.03, limit_altitude=limit_altitude)

    ax.grid(True, alpha=0.7)

    _set_title("航空機の気象データ (高度・温度分布)")

    img = _conver_to_img(fig)

    return (img, time.perf_counter() - start)


def _plot_contour_2d(data, figsize, plot_time_start=None, plot_time_end=None, limit_altitude=False):
    logging.info("Starting plot contour (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    # プロット時間範囲が指定されている場合は、グリッドをその範囲で作成
    # ただし、実際のデータ範囲を超えないように制限
    if plot_time_start and plot_time_end:
        plot_time_min = matplotlib.dates.date2num(plot_time_start)
        plot_time_max = matplotlib.dates.date2num(plot_time_end)
        # 実際のデータ範囲内に制限
        if len(data.time_numeric) > 0:
            actual_min = data.time_numeric.min()
            actual_max = data.time_numeric.max()
            plot_time_min = max(plot_time_min, actual_min)
            plot_time_max = min(plot_time_max, actual_max)
        grid = _create_grid(
            data.time_numeric,
            data.altitudes,
            data.temperatures,
            grid_points=80,
            time_range=(plot_time_min, plot_time_max),
            limit_altitude=limit_altitude,
        )
    else:
        grid = _create_grid(
            data.time_numeric,
            data.altitudes,
            data.temperatures,
            grid_points=80,
            limit_altitude=limit_altitude,
        )

    fig, ax = _create_figure(figsize)

    # limit_altitudeに応じた温度範囲と刻みを動的に設定
    temp_min, temp_max = get_temperature_range(limit_altitude)
    if limit_altitude:
        levels = numpy.arange(temp_min, temp_max + 1, 5)
    else:
        levels = numpy.arange(temp_min, temp_max + 1, 10)
    contour = ax.contour(
        grid.time_mesh, grid.alt_mesh, grid.temp_grid, levels=levels, colors="black", linewidths=0.5
    )
    contourf = ax.contourf(
        grid.time_mesh,
        grid.alt_mesh,
        grid.temp_grid,
        levels=levels,
        cmap="plasma",
        alpha=0.9,
    )

    ax.clabel(contour, inline=True, fontsize=_CONTOUR_SIZE, fmt="%d℃")

    # プロット時間範囲が指定されている場合はそれを使用、そうでなければグリッド範囲を使用
    if plot_time_start and plot_time_end:
        time_range = [plot_time_start, plot_time_end]
    else:
        time_range = [
            matplotlib.dates.num2date(grid.time_min),
            matplotlib.dates.num2date(grid.time_max),
        ]

    _set_axis_2d_default(ax, time_range, limit_altitude)

    _append_colorbar(contourf, shrink=1.0, pad=0.01, aspect=35, fraction=0.03, limit_altitude=limit_altitude)

    _set_title("航空機の気象データ (等高線)")

    img = _conver_to_img(fig)

    return (img, time.perf_counter() - start)


def _plot_heatmap(data, figsize, plot_time_start=None, plot_time_end=None, limit_altitude=False):
    logging.info("Starting plot heatmap (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    # プロット時間範囲が指定されている場合は、グリッドをその範囲で作成
    # ただし、実際のデータ範囲を超えないように制限
    if plot_time_start and plot_time_end:
        plot_time_min = matplotlib.dates.date2num(plot_time_start)
        plot_time_max = matplotlib.dates.date2num(plot_time_end)
        # 実際のデータ範囲内に制限
        if len(data.time_numeric) > 0:
            actual_min = data.time_numeric.min()
            actual_max = data.time_numeric.max()
            plot_time_min = max(plot_time_min, actual_min)
            plot_time_max = min(plot_time_max, actual_max)
        grid = _create_grid(
            data.time_numeric,
            data.altitudes,
            data.temperatures,
            grid_points=80,
            time_range=(plot_time_min, plot_time_max),
            limit_altitude=limit_altitude,
        )
    else:
        grid = _create_grid(
            data.time_numeric,
            data.altitudes,
            data.temperatures,
            grid_points=80,
            limit_altitude=limit_altitude,
        )

    fig, ax = _create_figure(figsize)

    im = ax.imshow(
        grid.temp_grid,
        extent=(grid.time_min, grid.time_max, grid.alt_min, grid.alt_max),
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
            matplotlib.dates.num2date(grid.time_min),
            matplotlib.dates.num2date(grid.time_max),
        ]

    _set_axis_2d_default(ax, time_range, limit_altitude)

    _append_colorbar(im, shrink=1.0, pad=0.01, aspect=35, fraction=0.03, limit_altitude=limit_altitude)

    _set_title("航空機の気象データ (ヒートマップ)")

    img = _conver_to_img(fig)

    return (img, time.perf_counter() - start)


def _plot_scatter_2d(data, figsize, limit_altitude=False):
    logging.info("Starting plot 2d scatter (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    fig, ax = _create_figure(figsize)

    sc = ax.scatter(
        data.times,
        data.altitudes,
        c=data.temperatures,
        cmap="plasma",
        s=15,
        alpha=0.9,
        rasterized=True,
        edgecolors="none",
    )

    _set_axis_2d_default(
        ax,
        [
            matplotlib.dates.num2date(data.time_numeric.min()),
            matplotlib.dates.num2date(data.time_numeric.max()),
        ],
        limit_altitude,
    )

    _append_colorbar(sc, shrink=1.0, pad=0.01, aspect=35, fraction=0.03, limit_altitude=limit_altitude)

    ax.grid(True, alpha=0.7)

    _set_title("航空機の気象データ")

    img = _conver_to_img(fig)

    return (img, time.perf_counter() - start)


def _plot_contour_3d(data, figsize, limit_altitude=False):
    logging.info("Starting plot contour 3d (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    # グリッドデータを作成
    grid = _create_grid(
        data.time_numeric,
        data.altitudes,
        data.temperatures,
        grid_points=60,
        limit_altitude=limit_altitude,
    )

    fig, ax = _create_3d_figure(figsize)

    # 3Dサーフェスプロットを作成
    surf = ax.plot_surface(
        grid.time_mesh,
        grid.alt_mesh,
        grid.temp_grid,
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
        grid.time_mesh,
        grid.alt_mesh,
        grid.temp_grid,
        levels=levels,
        colors="black",
        linewidths=0.5,
        alpha=0.3,
        offset=temp_min,  # 底面に等高線を投影
    )

    _set_axis_3d(ax, data.time_numeric, limit_altitude)
    _append_colorbar(surf, shrink=0.6, pad=0.01, aspect=35, limit_altitude=limit_altitude)
    _setup_3d_colorbar_and_layout(ax)

    _set_title("航空機の気象データ (3D)")

    img = _conver_to_img(fig)

    return (img, time.perf_counter() - start)


def _validate_wind_dataframe(data):
    """風データのDataFrame検証とカラムチェック"""
    if len(data.dataframe) == 0:
        logging.warning("Wind data not available for wind direction plot")
        raise ValueError("Wind data not available")

    df = data.dataframe
    required_columns = ["time", "altitude", "wind_x", "wind_y", "wind_speed", "wind_angle"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logging.warning("Missing wind data columns: %s", missing_columns)
        logging.warning("Available columns: %s", list(df.columns))
        msg = f"Missing wind data columns: {missing_columns}"
        raise ValueError(msg)

    return df


def _extract_and_filter_wind_data(df: pandas.DataFrame, limit_altitude: bool = False) -> WindFilteredData:
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
        altitude_mask = altitudes <= GRAPH_ALTITUDE_LIMIT
        valid_wind_mask = valid_wind_mask & altitude_mask

    if not valid_wind_mask.any():
        logging.warning(
            "No valid wind vectors after filtering (speed: %s, limit_altitude: %s)",
            (wind_speed > 0.1).sum(),
            limit_altitude,
        )
        raise ValueError("No valid wind vectors after filtering")

    return WindFilteredData(
        altitudes=altitudes[valid_wind_mask],
        wind_x=wind_x[valid_wind_mask],
        wind_y=wind_y[valid_wind_mask],
        time_numeric=time_numeric[valid_wind_mask],
    )


def _prepare_wind_data(data, limit_altitude=False):
    """風データの前処理とビニング処理（ベクトル化版）

    pandas groupby を使用して高速化。
    従来のPythonループを完全にベクトル化処理に置き換え。
    """
    df = _validate_wind_dataframe(data)
    valid_data = _extract_and_filter_wind_data(df, limit_altitude)

    valid_altitudes = valid_data.altitudes
    valid_time_numeric = valid_data.time_numeric
    valid_wind_x = valid_data.wind_x
    valid_wind_y = valid_data.wind_y

    # 高度ビニング（limit_altitudeに応じて範囲と間隔を調整）
    if limit_altitude:
        altitude_bins = numpy.arange(0, GRAPH_ALTITUDE_LIMIT + 100, 100)
    else:
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

    # pandas groupby でベクトル化集計
    bin_df = pandas.DataFrame(
        {
            "time_bin": time_bin_indices,
            "alt_bin_idx": altitude_bin_indices,
            "wind_x": valid_wind_x,
            "wind_y": valid_wind_y,
            "time_numeric": valid_time_numeric,
        }
    )

    # グループごとに平均を計算（ベクトル化）
    grouped: Any = bin_df.groupby(["time_bin", "alt_bin_idx"], as_index=False).agg(
        {
            "wind_x": "mean",
            "wind_y": "mean",
            "time_numeric": "mean",
        }
    )

    if len(grouped) == 0:
        logging.warning("No valid wind data after binning")
        raise ValueError("No valid wind data after binning")

    # 高度ビンインデックスから実際の高度値に変換
    alt_indices: Any = grouped["alt_bin_idx"].values
    grouped["altitude_bin"] = altitude_bins[alt_indices]

    # 風速と風向を再計算（ベクトル化）
    wind_x: Any = grouped["wind_x"]
    wind_y: Any = grouped["wind_y"]
    grouped["wind_speed"] = numpy.sqrt(wind_x**2 + wind_y**2)
    grouped["wind_angle"] = (90 - numpy.degrees(numpy.arctan2(wind_y, wind_x))) % 360

    return grouped.dropna()


def _plot_wind_direction(data, figsize, limit_altitude=False):
    logging.info("Starting plot wind direction (limit_altitude: %s)", limit_altitude)
    start = time.perf_counter()

    # デバッグ情報
    if len(data.dataframe) > 0:
        df = data.dataframe
        logging.info("Available columns in dataframe: %s", list(df.columns))
        logging.info("Dataframe shape: %s", df.shape)

    # データ前処理（dropna済み）
    grouped = _prepare_wind_data(data, limit_altitude)

    if len(grouped) == 0:
        logging.warning("No valid wind vectors after angle conversion")
        raise ValueError("No valid wind vectors after angle conversion")

    # プロット作成（先に figure/axes を作成してアスペクト比を取得）
    fig, ax = _create_figure(figsize)

    # 軸の範囲を設定してレイアウトを確定
    time_min: float = float(grouped["time_numeric"].min())
    time_max: float = float(grouped["time_numeric"].max())
    alt_max = GRAPH_ALTITUDE_LIMIT if limit_altitude else GRAPH_ALT_MAX
    ax.set_xlim(time_min, time_max)
    ax.set_ylim(GRAPH_ALT_MIN, alt_max)

    # レイアウトを確定させてから transform でアスペクト比を計算
    fig.canvas.draw()

    # データ座標系からピクセル座標系への変換を使って正確なアスペクト比を取得
    # (1, 0) と (0, 1) のデータ単位ベクトルがピクセル空間でどう見えるかを計算
    transform = ax.transData
    origin = transform.transform((time_min, GRAPH_ALT_MIN))
    x_unit = transform.transform((time_min + 1, GRAPH_ALT_MIN))
    y_unit = transform.transform((time_min, GRAPH_ALT_MIN + 1))

    # ピクセル/データ単位
    pixels_per_day = numpy.linalg.norm(x_unit - origin)
    pixels_per_meter = numpy.linalg.norm(y_unit - origin)

    # 見た目の角度を正しくするための補正係数
    # 北風（wind_x=0, wind_y<0）が正しく下向きに見えるようにする
    aspect_correction = pixels_per_day / pixels_per_meter if pixels_per_meter > 0 else 1

    # 矢印の基本スケール（時間軸方向の長さ）
    time_range = time_max - time_min
    arrow_scale = time_range / 30

    gwind_x: Any = grouped["wind_x"]
    gwind_y: Any = grouped["wind_y"]
    wind_magnitude = numpy.sqrt(gwind_x**2 + gwind_y**2)
    # wind_x, wind_yは風が吹いていく方向のベクトル、矢印もその方向を指す
    # u（時間軸方向）= 東西成分、v（高度軸方向）= 南北成分
    grouped["u_normalized"] = (gwind_x / wind_magnitude) * arrow_scale
    grouped["v_normalized"] = (gwind_y / wind_magnitude) * arrow_scale * aspect_correction
    wind_speeds: Any = grouped["wind_speed"].values
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

    _set_axis_2d_default(
        ax,
        [
            matplotlib.dates.num2date(grouped["time_numeric"].min()),
            matplotlib.dates.num2date(grouped["time_numeric"].max()),
        ],
        limit_altitude,
    )

    cbar = matplotlib.pyplot.colorbar(quiver, shrink=0.8, pad=0.01, aspect=35, fraction=0.046)
    cbar.set_label("風速 (m/s)", fontsize=_AXIS_LABEL_SIZE)
    _set_tick_label_size(cbar.ax)

    _set_title("航空機観測による風向・風速分布")

    img = _conver_to_img(fig)
    return (img, time.perf_counter() - start)


def _plot_temperature(data, figsize, limit_altitude=False):
    logging.info("Starting plot temperature timeseries (limit_altitude: %s)", limit_altitude)

    start = time.perf_counter()

    fig, ax = _create_figure(figsize)

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
        mask = (data.altitudes >= alt_range["min"]) & (data.altitudes <= alt_range["max"])
        if not numpy.any(mask):
            continue

        filtered_temps = data.temperatures[mask]
        filtered_time_numeric = data.time_numeric[mask]

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

    time_range = data.time_numeric.max() - data.time_numeric.min()
    _apply_time_axis_format(ax, time_range)

    # Y軸の範囲設定（limit_altitudeによって変更）
    temp_min, temp_max = get_temperature_range(limit_altitude)
    ax.set_ylim(temp_min, temp_max)

    # 凡例の追加
    ax.legend(loc="upper right", framealpha=0.9)

    _set_title("高度別温度の時系列変化")

    img = _conver_to_img(fig)

    return (img, time.perf_counter() - start)


@dataclass
class GraphDefinition:
    """グラフ定義"""

    func: Callable[..., tuple[PIL.Image.Image, float]]
    size: tuple[int, int]
    file: str


GRAPH_DEF_MAP: dict[str, GraphDefinition] = {
    "scatter_2d": GraphDefinition(func=_plot_scatter_2d, size=(2400, 1600), file="scatter_2d.png"),
    "scatter_3d": GraphDefinition(func=_plot_scatter_3d, size=(2800, 2800), file="scatter_3d.png"),
    "contour_2d": GraphDefinition(func=_plot_contour_2d, size=(2400, 1600), file="contour_2d.png"),
    "contour_3d": GraphDefinition(func=_plot_contour_3d, size=(2800, 2800), file="contour_3d.png"),
    "density": GraphDefinition(func=_plot_density, size=(2400, 1600), file="density.png"),
    "heatmap": GraphDefinition(func=_plot_heatmap, size=(2400, 1600), file="heatmap.png"),
    "temperature": GraphDefinition(func=_plot_temperature, size=(2400, 1600), file="temperature.png"),
    "wind_direction": GraphDefinition(
        func=_plot_wind_direction, size=(2400, 1600), file="wind_direction.png"
    ),
}


# =============================================================================
# キャッシュ機能
# =============================================================================
# CACHE_TTL_SECONDS, CACHE_START_TIME_TOLERANCE_SECONDS は constants.py からインポート済み


@functools.cache
def get_git_commit_hash() -> str:
    """現在の git commit ハッシュを取得する（functools.cacheでキャッシュ）"""
    try:
        revision_info = my_lib.git_util.get_revision_info()
        return revision_info.hash[:12]
    except Exception:
        logging.warning("Failed to get git commit hash")
        return "unknown"


@dataclass
class CacheFileInfo:
    """キャッシュファイルの情報"""

    path: pathlib.Path
    graph_name: GraphName
    period_seconds: int
    limit_altitude: bool
    start_ts: int
    git_commit: str
    created_at: float  # ファイル作成時刻（Unix timestamp）


def generate_cache_filename(
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
) -> str:
    """キャッシュファイル名を生成する

    形式: {graph_name}_{period_seconds}_{limit}_{start_ts}_{git}.png
    """
    git_commit = get_git_commit_hash()
    period_seconds = int((time_end - time_start).total_seconds())
    start_ts = int(time_start.timestamp())
    limit_str = "1" if limit_altitude else "0"
    return f"{graph_name}_{period_seconds}_{limit_str}_{start_ts}_{git_commit}.png"


# ETAG_TIME_ROUND_SECONDS は constants.py からインポート済み


def generate_etag_key(
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
) -> str:
    """ETag用のキーを生成する

    形式: {graph_name}_{period_seconds}_{limit}_{rounded_start_ts}_{git}
    開始時刻は10分単位に丸められる
    """
    git_commit = get_git_commit_hash()
    period_seconds = int((time_end - time_start).total_seconds())
    # 10分（600秒）単位に丸める
    rounded_start_ts = (int(time_start.timestamp()) // ETAG_TIME_ROUND_SECONDS) * ETAG_TIME_ROUND_SECONDS
    limit_str = "1" if limit_altitude else "0"
    return f"{graph_name}_{period_seconds}_{limit_str}_{rounded_start_ts}_{git_commit}"


def _generate_stable_job_id(
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
) -> str:
    """キャッシュヒット時用の安定したジョブIDを生成する

    同じパラメータからは常に同じIDが生成されるため、
    ブラウザキャッシュが効くようになる。
    """
    import hashlib

    key = generate_etag_key(graph_name, time_start, time_end, limit_altitude)
    # UUIDv5風のフォーマットにする（8-4-4-4-12）
    hash_hex = hashlib.sha256(key.encode()).hexdigest()
    return f"{hash_hex[:8]}-{hash_hex[8:12]}-{hash_hex[12:16]}-{hash_hex[16:20]}-{hash_hex[20:32]}"


def parse_cache_filename(filepath: pathlib.Path) -> CacheFileInfo | None:
    """キャッシュファイル名をパースして情報を取得する

    形式: {graph_name}_{period_seconds}_{limit}_{start_ts}_{git}.png
    """
    filename = filepath.stem  # 拡張子を除いたファイル名
    parts = filename.rsplit("_", 4)  # 後ろから4つ分割

    if len(parts) != 5:
        return None

    try:
        graph_name_str = parts[0]
        period_seconds = int(parts[1])
        limit_altitude = parts[2] == "1"
        start_ts = int(parts[3])
        git_commit = parts[4]

        # 有効なグラフ名か検証
        if graph_name_str not in GRAPH_DEF_MAP:
            return None

        # 型を明示的にキャスト（GRAPH_DEF_MAP に含まれることを検証済み）
        graph_name: GraphName = graph_name_str  # type: ignore[assignment]

        return CacheFileInfo(
            path=filepath,
            graph_name=graph_name,
            period_seconds=period_seconds,
            limit_altitude=limit_altitude,
            start_ts=start_ts,
            git_commit=git_commit,
            created_at=filepath.stat().st_mtime,
        )
    except (ValueError, OSError):
        return None


def cleanup_expired_cache(cache_dir: pathlib.Path) -> int:
    """期限切れ（作成から30分以上経過）のキャッシュファイルを削除する

    Returns
    -------
        削除したファイル数

    """
    if not cache_dir.exists():
        return 0

    deleted_count = 0
    current_time = time.time()

    for cache_file in cache_dir.glob("*.png"):
        try:
            mtime = cache_file.stat().st_mtime
            if current_time - mtime > CACHE_TTL_SECONDS:
                cache_file.unlink()
                deleted_count += 1
                logging.info(
                    "[CACHE] Deleted expired: %s (age: %.0f sec)", cache_file.name, current_time - mtime
                )
        except OSError as e:
            logging.warning("[CACHE] Failed to delete %s: %s", cache_file.name, e)

    return deleted_count


def find_matching_cache(
    cache_dir: pathlib.Path,
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
) -> CacheFileInfo | None:
    """条件に合うキャッシュファイルを検索する

    ヒット条件:
    - 同じ graph_name
    - 同じ期間（period_seconds）
    - 同じ limit_altitude
    - 同じ git_commit
    - 開始日時の差が30分以内
    - ファイル作成から30分以内（TTL）
    """
    if not cache_dir.exists():
        return None

    git_commit = get_git_commit_hash()
    request_period = int((time_end - time_start).total_seconds())
    request_start_ts = int(time_start.timestamp())
    current_time = time.time()

    for cache_file in cache_dir.glob("*.png"):
        info = parse_cache_filename(cache_file)
        if info is None:
            continue

        # 基本条件チェック
        if info.graph_name != graph_name:
            continue
        if info.period_seconds != request_period:
            continue
        if info.limit_altitude != limit_altitude:
            continue
        if info.git_commit != git_commit:
            continue

        # TTLチェック（作成から30分以内）
        if current_time - info.created_at > CACHE_TTL_SECONDS:
            continue

        # 開始日時の差が30分以内かチェック
        start_time_diff = abs(info.start_ts - request_start_ts)
        if start_time_diff <= CACHE_START_TIME_TOLERANCE_SECONDS:
            logging.info(
                "[CACHE] HIT: %s (start_diff: %d sec, age: %.0f sec)",
                cache_file.name,
                start_time_diff,
                current_time - info.created_at,
            )
            return info

    return None


def get_cached_image(
    cache_dir: pathlib.Path,
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
) -> tuple[bytes | None, str | None]:
    """キャッシュから画像を取得する

    Returns
    -------
        (画像データ, キャッシュファイル名) または (None, None)

    """
    # まず期限切れキャッシュを削除
    deleted = cleanup_expired_cache(cache_dir)
    if deleted > 0:
        logging.info("[CACHE] Cleaned up %d expired files", deleted)

    # 条件に合うキャッシュを検索
    cache_info = find_matching_cache(cache_dir, graph_name, time_start, time_end, limit_altitude)

    if cache_info is None:
        return None, None

    try:
        image_data = cache_info.path.read_bytes()
        return image_data, cache_info.path.name
    except OSError as e:
        logging.warning("[CACHE] Failed to read %s: %s", cache_info.path.name, e)
        return None, None


def save_to_cache(
    cache_dir: pathlib.Path,
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
    image_bytes: bytes,
) -> str | None:
    """画像をキャッシュに保存する

    Returns
    -------
        保存したファイル名、失敗時はNone

    """
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        filename = generate_cache_filename(graph_name, time_start, time_end, limit_altitude)
        cache_file = cache_dir / filename
        cache_file.write_bytes(image_bytes)
        logging.info("[CACHE] Saved: %s (%d bytes)", filename, len(image_bytes))
        return filename
    except OSError as e:
        logging.warning("[CACHE] Failed to save: %s", e)
        return None


def plot_in_subprocess(
    config: amdar.config.Config,
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    figsize: tuple[float, float],
    limit_altitude: bool = False,
) -> tuple[bytes, float]:
    """子プロセス内でデータ取得からグラフ描画まで一貫して実行する関数"""
    import matplotlib

    matplotlib.use("Agg")

    import matplotlib.pyplot

    # 子プロセスに渡された時間範囲を記録
    period_days = (time_end - time_start).total_seconds() / 86400
    logging.debug(
        "plot_in_subprocess() for %s: start=%s, end=%s, period=%.2f days",
        graph_name,
        time_start,
        time_end,
        period_days,
    )

    # データベース接続とデータ取得を子プロセス内で実行
    conn = _connect_database(config)

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
    # 期間が14日を超える場合は集約データを使用（パフォーマンス最適化）
    if period_days > 14:
        # 集約データを使用（期間に応じて自動的に適切なレベルを選択）
        numpy_data = amdar.database.postgresql.fetch_aggregated_numpy(
            conn,
            extended_time_start,
            extended_time_end,
            max_altitude=GRAPH_ALTITUDE_LIMIT if limit_altitude else None,
            include_wind=include_wind,
        )
    else:
        # 14日以内は生データを使用
        numpy_data = amdar.database.postgresql.fetch_by_time_numpy(
            conn,
            extended_time_start,
            extended_time_end,
            config.filter.area.distance,
            max_altitude=GRAPH_ALTITUDE_LIMIT if limit_altitude else None,
            include_wind=include_wind,
        )
    conn.close()

    # デバッグ: 取得したデータの時間範囲を確認
    if numpy_data.count > 0:
        times = numpy_data.time
        logging.info(
            "Data range for %s: %s to %s (%d rows)",
            graph_name,
            times.min(),
            times.max(),
            numpy_data.count,
        )
    else:
        logging.warning("No data fetched for %s", graph_name)

    # データ準備（高速版NumPy処理）
    data = _prepare_data_numpy(numpy_data)

    # numpy_data は data に変換済みなので解放
    del numpy_data

    if data.count < 10:
        # データがない場合の画像を生成
        del data
        gc.collect()
        try:
            img = _create_no_data_image(config, graph_name)
            bytes_io = io.BytesIO()
            img.save(bytes_io, "PNG")
            bytes_io.seek(0)
            result = bytes_io.getvalue()
            bytes_io.close()
            del img
            gc.collect()
            return result, 0
        except Exception:
            logging.exception("Failed to create no data image")
            img = _create_no_data_image(config, graph_name, "グラフの作成に失敗しました")
            bytes_io = io.BytesIO()
            img.save(bytes_io, "PNG")
            bytes_io.seek(0)
            result = bytes_io.getvalue()
            bytes_io.close()
            del img
            gc.collect()
            return result, 0

    set_font(config.font)

    try:
        # heatmapとcontourグラフの場合、元の時間範囲を渡してプロット範囲を制限
        if graph_name in ["heatmap", "contour_2d"]:
            img, elapsed = GRAPH_DEF_MAP[graph_name].func(data, figsize, time_start, time_end, limit_altitude)
        else:
            img, elapsed = GRAPH_DEF_MAP[graph_name].func(data, figsize, limit_altitude)
    except Exception as e:
        logging.warning("Failed to generate %s: %s", graph_name, str(e))
        # エラー時は「データなし」画像を生成
        try:
            img = _create_no_data_image(config, graph_name)
            elapsed = 0
        except Exception:
            logging.exception("Failed to create no data image")
            img = _create_no_data_image(config, graph_name, "グラフの作成に失敗しました")
            elapsed = 0

    # PIL.Imageを直接returnできないので、bytesに変換して返す
    bytes_io = io.BytesIO()
    img.save(bytes_io, "PNG")
    bytes_io.seek(0)
    result_bytes = bytes_io.getvalue()

    image_size = len(result_bytes)
    logging.debug(
        "plot_in_subprocess() completed for %s: elapsed=%.2f sec, image_size=%d bytes",
        graph_name,
        elapsed,
        image_size,
    )

    # メモリ解放: 大きなオブジェクトを明示的に削除してGCを実行
    del img
    del data
    bytes_io.close()
    gc.collect()

    return result_bytes, elapsed


def _calculate_timeout(time_start, time_end):
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
        return GRAPH_GEN_TIMEOUT_7DAYS_SECONDS
    elif days <= 30:
        return GRAPH_GEN_TIMEOUT_30DAYS_SECONDS
    elif days <= 90:
        return GRAPH_GEN_TIMEOUT_90DAYS_SECONDS
    else:
        return GRAPH_GEN_TIMEOUT_OVER90DAYS_SECONDS


def plot(config, graph_name, time_start, time_end, limit_altitude=False):
    """グラフを生成する（キャッシュ付き）"""
    # デバッグ: plot()に渡された時間範囲を記録
    period_days = (time_end - time_start).total_seconds() / 86400
    period_seconds = int((time_end - time_start).total_seconds())
    start_ts = int(time_start.timestamp())

    logging.debug(
        "plot() called for %s: start=%s, end=%s, period=%.2f days, limit_altitude=%s",
        graph_name,
        time_start,
        time_end,
        period_days,
        limit_altitude,
    )

    # キャッシュチェック
    cache_dir = config.webapp.cache_dir_path

    # キャッシュ判定ログ
    logging.info(
        "[CACHE] %s: checking (period=%d sec, start_ts=%d, limit=%s)",
        graph_name,
        period_seconds,
        start_ts,
        limit_altitude,
    )

    # キャッシュから取得を試みる
    cached_image, cache_filename = get_cached_image(
        cache_dir, graph_name, time_start, time_end, limit_altitude
    )
    if cached_image:
        logging.info(
            "[CACHE] Returning cached image for %s: %s (%d bytes)",
            graph_name,
            cache_filename,
            len(cached_image),
        )
        return cached_image

    # キャッシュミス
    logging.info("[CACHE] MISS: %s (no matching cache found)", graph_name)

    # グラフサイズを計算
    figsize = tuple(x / GRAPH_IMAGE_DPI for x in GRAPH_DEF_MAP[graph_name].size)

    # 期間に応じたタイムアウト値を計算
    timeout_seconds = _calculate_timeout(time_start, time_end)

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
        if image_bytes:
            save_to_cache(cache_dir, graph_name, time_start, time_end, limit_altitude, image_bytes)

        return image_bytes
    except multiprocessing.TimeoutError:
        logging.exception("Timeout in plot generation for %s (%d seconds)", graph_name, timeout_seconds)
        msg = f"Plot generation timed out for {graph_name}"
        raise RuntimeError(msg) from None
    except Exception:
        logging.exception("Error in plot generation for %s", graph_name)
        # エラー時は直接エラー画像を生成
        try:
            img = _create_no_data_image(config, graph_name, "グラフの作成に失敗しました")
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
        conn = _connect_database(config)

        # ビューを更新
        timings = amdar.database.postgresql.refresh_materialized_views(conn)

        # 統計情報を取得
        stats = amdar.database.postgresql.get_materialized_view_stats(conn)
        conn.close()

        return flask.jsonify(
            {
                "status": "success",
                "refresh_times": timings.to_dict(),
                "stats": stats.to_dict(),
            }
        )

    except Exception as e:
        logging.exception("Error refreshing materialized views")
        return flask.jsonify({"error": "Failed to refresh views", "details": str(e)}), 500


@blueprint.route("/api/aggregate-stats", methods=["GET"])
def aggregate_stats():
    """マテリアライズドビューの統計情報を取得するAPI"""
    try:
        config = flask.current_app.config["CONFIG"]
        conn = _connect_database(config)

        # ビューの存在確認
        exists = amdar.database.postgresql.check_materialized_views_exist(conn)

        # 統計情報を取得
        stats = amdar.database.postgresql.get_materialized_view_stats(conn)
        conn.close()

        return flask.jsonify(
            {
                "exists": exists,
                "stats": stats.to_dict(),
            }
        )

    except Exception as e:
        logging.exception("Error getting aggregate stats")
        return flask.jsonify({"error": "Failed to get stats", "details": str(e)}), 500


@blueprint.route("/api/data-range", methods=["GET"])
def data_range():
    """データベースの最古・最新データの日時を返すAPI"""
    try:
        config = flask.current_app.config["CONFIG"]
        conn = _connect_database(config)

        # データ範囲を取得
        result = amdar.database.postgresql.fetch_data_range(conn)
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


@blueprint.route("/api/last-received", methods=["GET"])
def last_received():
    """受信方式別の最終受信時刻を返すAPI"""
    try:
        config = flask.current_app.config["CONFIG"]
        conn = _connect_database(config)

        result = amdar.database.postgresql.fetch_last_received_by_method(conn)
        conn.close()

        def format_datetime(dt: datetime.datetime | None) -> str | None:
            if dt is None:
                return None
            # タイムゾーン情報がない場合はJSTとして扱う
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=my_lib.time.get_zoneinfo())
            return dt.isoformat()

        response_data = {
            "mode_s": format_datetime(result.mode_s),
            "vdl2": format_datetime(result.vdl2),
        }

        return flask.jsonify(response_data)

    except Exception as e:
        logging.exception("Error fetching last received times")
        return flask.jsonify({"error": "最終受信時刻の取得に失敗しました", "details": str(e)}), 500


@blueprint.route("/api/graph/<path:graph_name>", methods=["GET"])
def graph(graph_name):
    # デフォルト値を設定
    default_time_end = my_lib.time.now()
    default_time_start = default_time_end - datetime.timedelta(days=1)

    # パラメータから時間を取得（JSON文字列として）
    time_end_str = flask.request.args.get("end", None)
    time_start_str = flask.request.args.get("start", None)
    limit_altitude_str = flask.request.args.get("limit_altitude", "false")  # デフォルトでfalse

    # 受信したパラメータを記録
    logging.debug(
        "Raw params for %s: start_str=%r, end_str=%r, limit_altitude_str=%r",
        graph_name,
        time_start_str,
        time_end_str,
        limit_altitude_str,
    )

    # 文字列をUTC時間のdatetimeに変換してからローカルタイムに変換
    if time_end_str:
        try:
            parsed_end = json.loads(time_end_str)
            logging.debug("Parsed end JSON: %r", parsed_end)
            time_end = datetime.datetime.fromisoformat(parsed_end)
            time_end = time_end.astimezone(my_lib.time.get_zoneinfo())
        except Exception:
            logging.debug("Failed to parse end time", exc_info=True)
            time_end = default_time_end
    else:
        logging.debug("No end param, using default: %s", default_time_end)
        time_end = default_time_end

    if time_start_str:
        try:
            parsed_start = json.loads(time_start_str)
            logging.debug("Parsed start JSON: %r", parsed_start)
            time_start = datetime.datetime.fromisoformat(parsed_start)
            time_start = time_start.astimezone(my_lib.time.get_zoneinfo())
        except Exception:
            logging.debug("Failed to parse start time", exc_info=True)
            time_start = default_time_start
    else:
        logging.debug("No start param, using default: %s", default_time_start)
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

    # キャッシュ用の ETag を生成（開始時刻は10分単位に丸める）
    etag_key = generate_etag_key(graph_name, time_start, time_end, limit_altitude)
    etag = f'"{etag_key}"'

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
        res.headers["Cache-Control"] = f"private, max-age={CACHE_CONTROL_MAX_AGE_RESULT}"
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
        plt.savefig(buf, format="png", dpi=GRAPH_IMAGE_DPI, bbox_inches="tight", facecolor="white")
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
def debug_date_parse():
    """デバッグ用：日付パース処理をテストするAPI"""
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
            time_end = datetime.datetime.fromisoformat(parsed_end)
            time_end = time_end.astimezone(my_lib.time.get_zoneinfo())
            result["parsed"]["end"] = {
                "json_parsed": parsed_end,
                "datetime": str(time_end),
                "utc": str(time_end.astimezone(datetime.UTC)),
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
            time_start = datetime.datetime.fromisoformat(parsed_start)
            time_start = time_start.astimezone(my_lib.time.get_zoneinfo())
            result["parsed"]["start"] = {
                "json_parsed": parsed_start,
                "datetime": str(time_start),
                "utc": str(time_start.astimezone(datetime.UTC)),
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
    level = amdar.database.postgresql.get_aggregation_level(period_days)
    result["aggregation"] = {
        "table": level.table,
        "time_interval": level.time_interval,
        "altitude_bin": level.altitude_bin,
    }

    # データサンプル取得
    try:
        config = flask.current_app.config["CONFIG"]
        conn = _connect_database(config)

        # マテリアライズドビューの存在確認
        view_exists = amdar.database.postgresql.check_materialized_views_exist(conn)
        result["views_exist"] = view_exists

        # データ取得テスト（最初の10件のみ）
        if period_days > 14:
            raw_data = amdar.database.postgresql.fetch_aggregated_by_time(
                conn, time_start, time_end, max_altitude=None
            )
        else:
            raw_data = amdar.database.postgresql.fetch_by_time(
                conn, time_start, time_end, distance=DEFAULT_DISTANCE_KM
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
        dt = datetime.datetime.fromisoformat(date_str)
        # JSTに変換
        return dt.astimezone(my_lib.time.get_zoneinfo())
    except Exception:
        logging.exception("Failed to parse datetime: %s", date_str)
        return None


def _start_job_async(
    config: amdar.config.Config,
    job_id: str,
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
    cache_dir: pathlib.Path,
) -> None:
    """プロセスプールを使用してジョブを非同期実行（ポーリング方式）"""
    _job_manager.update_status(job_id, JobStatus.PROCESSING, progress=10, stage="開始中...")

    pool = _pool_manager.get_pool()
    figsize = tuple(x / GRAPH_IMAGE_DPI for x in GRAPH_DEF_MAP[graph_name].size)

    # ポーリングスレッドを起動（まだ起動していない場合）
    _start_result_checker_thread()

    # コールバックを使わずにAsyncResultを直接取得
    async_result = pool.apply_async(
        plot_in_subprocess,
        (config, graph_name, time_start, time_end, figsize, limit_altitude),
    )

    # 保留中の結果リストに追加（ポーリングスレッドが監視）
    with _async_results_lock:
        _pending_async_results[job_id] = (async_result, graph_name, cache_dir)

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
        cache_dir = config.webapp.cache_dir_path

        jobs = []

        for graph_name in graphs:
            if graph_name not in GRAPH_DEF_MAP:
                logging.warning("Unknown graph name: %s", graph_name)
                continue

            # キャッシュチェック（先にチェックしてジョブIDを決定）
            cached_image, cache_filename = get_cached_image(
                cache_dir, graph_name, time_start, time_end, limit_altitude
            )

            if cached_image:
                # キャッシュヒット: 安定したジョブIDを使用（ブラウザキャッシュが効く）
                stable_job_id = _generate_stable_job_id(graph_name, time_start, time_end, limit_altitude)
                job_id = _job_manager.create_job(
                    graph_name, time_start, time_end, limit_altitude, job_id=stable_job_id
                )
                logging.info(
                    "[CACHE] HIT for %s: %s (%d bytes, stable_id=%s)",
                    graph_name,
                    cache_filename,
                    len(cached_image),
                    job_id,
                )
                _job_manager.update_status(job_id, JobStatus.COMPLETED, result=cached_image, progress=100)
            else:
                # キャッシュミス: 新規ジョブIDで作成
                job_id = _job_manager.create_job(graph_name, time_start, time_end, limit_altitude)
                logging.info("[CACHE] MISS for %s, starting job %s", graph_name, job_id)
                _start_job_async(config, job_id, graph_name, time_start, time_end, limit_altitude, cache_dir)

            jobs.append({"job_id": job_id, "graph_name": graph_name})

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
    res.headers["Cache-Control"] = f"private, max-age={CACHE_CONTROL_MAX_AGE_STATUS}"
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
        data = _prepare_data(raw_data)

        if data is None:
            logging.warning("プロット用のデータがありません")
            return

        set_font(config.font)

        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
            futures: dict[str, concurrent.futures.Future] = {}
            for graph_name, graph_def in GRAPH_DEF_MAP.items():
                figsize = tuple(x / GRAPH_IMAGE_DPI for x in graph_def.size)
                futures[graph_name] = executor.submit(graph_def.func, data, figsize)

            for graph_name, graph_def in GRAPH_DEF_MAP.items():
                img, elapsed = futures[graph_name].result()
                img.save(graph_def.file)

                logging.info("elapsed time: %s = %.3f sec", graph_name, elapsed)

    import docopt
    import my_lib.logger
    import my_lib.time

    import amdar.database.postgresql

    assert __doc__ is not None  # noqa: S101
    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    period_days = int(args["-p"])
    debug_mode = args["-D"]

    my_lib.logger.init("modes sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = amdar.config.load_config(config_file)

    conn = _connect_database(config)
    time_end = my_lib.time.now()
    time_start = time_end - datetime.timedelta(days=period_days)

    plot_local(
        amdar.database.postgresql.fetch_by_time(
            conn,
            time_start,
            time_end,
            config.filter.area.distance,
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
