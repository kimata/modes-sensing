"""グラフ生成の進捗推定とタイムアウト計算。"""

from __future__ import annotations

import datetime
import time

import amdar.viewer.api.progress_estimation
from amdar.constants import (
    GRAPH_GEN_TIMEOUT_7DAYS_SECONDS,
    GRAPH_GEN_TIMEOUT_30DAYS_SECONDS,
    GRAPH_GEN_TIMEOUT_90DAYS_SECONDS,
    GRAPH_GEN_TIMEOUT_OVER90DAYS_SECONDS,
    GRAPH_JOB_TIMEOUT_BUFFER_SECONDS,
    GraphName,
)
from amdar.viewer.api.job_manager import Job

# 進捗計算用バッファ（推定時間の不確実性を吸収）
_ESTIMATION_BUFFER_SECONDS = 3.0


def calculate_timeout(time_start: datetime.datetime, time_end: datetime.datetime) -> int:
    """期間に応じたグラフ生成のタイムアウト値（秒）を返す。"""
    days = (time_end - time_start).total_seconds() / 86400
    if days <= 7:
        return GRAPH_GEN_TIMEOUT_7DAYS_SECONDS
    if days <= 30:
        return GRAPH_GEN_TIMEOUT_30DAYS_SECONDS
    if days <= 90:
        return GRAPH_GEN_TIMEOUT_90DAYS_SECONDS
    return GRAPH_GEN_TIMEOUT_OVER90DAYS_SECONDS


def calculate_polling_timeout(time_start: datetime.datetime, time_end: datetime.datetime) -> int:
    """ポーリング側で「ハング」判定するためのタイムアウト値（秒）。

    実行用タイムアウト + バッファ。
    """
    return calculate_timeout(time_start, time_end) + GRAPH_JOB_TIMEOUT_BUFFER_SECONDS


def estimate_progress_and_stage(job: Job) -> tuple[int, str]:
    """ジョブの推定進捗（0-100）と現在ステージを返す。

    開始からの経過時間と履歴ベースの推定総時間から進捗を線形補間する。
    """
    if not job.started_at:
        return 10, "開始中..."

    elapsed = time.time() - job.started_at

    duration_hours = (job.time_end - job.time_start).total_seconds() / 3600

    history = amdar.viewer.api.progress_estimation.generation_time_history
    estimated_total = (
        history.get_estimated_time(job.graph_name, duration_hours, job.limit_altitude)
        + _ESTIMATION_BUFFER_SECONDS
    )

    # 進捗は 10-95% の範囲に収める
    progress = min(95, 10 + int((elapsed / estimated_total) * 85))

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


def record_generation_time(
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
    elapsed: float,
) -> None:
    """生成時間を履歴に記録（次回以降の推定精度向上）。"""
    duration_hours = (time_end - time_start).total_seconds() / 3600
    amdar.viewer.api.progress_estimation.generation_time_history.record(
        graph_name, duration_hours, limit_altitude, elapsed
    )
