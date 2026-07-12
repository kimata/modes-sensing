"""グラフ生成のサブプロセスワーカー。

このモジュールの :func:`generate_graph_image` は **必ず子プロセス内**
（:class:`amdar.viewer.graph.pool.ProcessPoolManager` 経由）で実行する。
メインプロセスから直接呼び出してはならない。理由は二つ:

1. matplotlib (pyplot) はスレッド安全ではないため、複数スレッドから同時に
   触れるとロックや無音失敗を起こす。
2. DB 接続もプロセス毎に独立させたいため。

Config はタスク毎に pickle 転送せず、Pool の initializer
（:func:`init_worker`）で一度だけ受け取る。
"""

from __future__ import annotations

import datetime
import gc
import io
import logging
import time
from collections.abc import MutableMapping

import matplotlib

matplotlib.use("Agg")  # pyplot を import する前に設定する必要がある

import matplotlib.pyplot

import amdar.config
import amdar.database.postgresql
from amdar.constants import GRAPH_ALTITUDE_LIMIT, VERTICAL_PROFILE_WINDOW_HOURS, GraphName
from amdar.viewer.graph.definitions import GRAPH_DEF_MAP
from amdar.viewer.graph.plotting.data_prep import prepare_data_numpy
from amdar.viewer.graph.plotting.figure import create_no_data_image

# データが少なすぎる場合にデータなし画像を返す閾値
_MIN_DATA_POINTS = 10
# heatmap / contour_2d でデータ取得範囲を拡張する割合
_GRID_EXTENSION_RATIO = 0.1
# 集約データを使う期間の境界（日）
_AGGREGATION_THRESHOLD_DAYS = 14

# Pool initializer で設定されるワーカープロセス毎の Config
_worker_config: amdar.config.Config | None = None


def init_worker(config: amdar.config.Config) -> None:
    """プロセスプールのワーカー初期化（Pool の initializer として実行される）。"""
    global _worker_config
    _worker_config = config


def _get_worker_config() -> amdar.config.Config:
    if _worker_config is None:
        msg = "Worker is not initialized. generate_graph_image must run in a pool worker."
        raise RuntimeError(msg)
    return _worker_config


def _connect_database(config: amdar.config.Config):
    # スキーマ適用（DDL）は収集側で実施済みのため、閲覧側の接続では省略する
    return amdar.database.postgresql.open(
        config.database.host,
        config.database.port,
        config.database.name,
        config.database.user,
        config.database.password,
        apply_schema=False,
    )


def _image_to_bytes(img) -> bytes:
    bytes_io = io.BytesIO()
    img.save(bytes_io, "PNG")
    bytes_io.seek(0)
    data = bytes_io.getvalue()
    bytes_io.close()
    return data


def _no_data_bytes(config: amdar.config.Config, graph_name: GraphName, text: str | None = None) -> bytes:
    """データなし画像を bytes で返す。"""
    size = GRAPH_DEF_MAP[graph_name].size
    img = create_no_data_image(config, size) if text is None else create_no_data_image(config, size, text)
    try:
        return _image_to_bytes(img)
    finally:
        del img
        gc.collect()


def _fetch_numpy_data(
    config: amdar.config.Config,
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
):
    """DB からグラフ用データを取得する。"""
    # 鉛直プロファイル系は要求期間の末尾ウィンドウのみ使うため取得範囲を狭める
    if graph_name in ("temperature_profile", "hodograph"):
        time_start = max(time_start, time_end - datetime.timedelta(hours=VERTICAL_PROFILE_WINDOW_HOURS))

    period_days = (time_end - time_start).total_seconds() / 86400

    # heatmap / contour_2d は端の描画を改善するため取得範囲を 10% 拡張
    if graph_name in ("heatmap", "contour_2d"):
        time_range = time_end - time_start
        extension = time_range * _GRID_EXTENSION_RATIO
        extended_time_start = time_start - extension
        extended_time_end = time_end + extension
    else:
        extended_time_start = time_start
        extended_time_end = time_end

    include_wind = graph_name in ("wind_direction", "hodograph")
    max_altitude = GRAPH_ALTITUDE_LIMIT if limit_altitude else None

    conn = _connect_database(config)
    try:
        if period_days > _AGGREGATION_THRESHOLD_DAYS:
            # 14 日超は集約データを使う
            return amdar.database.postgresql.fetch_aggregated_numpy(
                conn,
                extended_time_start,
                extended_time_end,
                max_altitude=max_altitude,
                include_wind=include_wind,
            )
        return amdar.database.postgresql.fetch_by_time_numpy(
            conn,
            extended_time_start,
            extended_time_end,
            config.filter.area.distance,
            max_altitude=max_altitude,
            include_wind=include_wind,
        )
    finally:
        conn.close()


def generate_graph_image(
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    figsize: tuple[float, float],
    limit_altitude: bool = False,
    job_id: str | None = None,
    start_times: MutableMapping[str, float] | None = None,
) -> tuple[bytes, float]:
    """グラフ画像を生成する（サブプロセスで実行されることを前提とする）。

    Args:
        graph_name: グラフ種別
        time_start: データ取得開始日時
        time_end: データ取得終了日時
        figsize: matplotlib の figsize（インチ）
        limit_altitude: 高度制限フラグ
        job_id: 非同期ジョブの ID（監視用、同期実行時は None）
        start_times: 実行開始時刻を通知する共有 dict（job_id とセットで渡す）

    Returns:
        ``(PNG バイト列, 描画所要秒)``。データなし時の代替画像でも空 bytes は返さない。
    """
    if job_id is not None and start_times is not None:
        # 監視側にキュー待ち終了（実行開始）を通知する
        start_times[job_id] = time.time()

    config = _get_worker_config()

    period_days = (time_end - time_start).total_seconds() / 86400
    logging.debug(
        "generate_graph_image() for %s: start=%s, end=%s, period=%.2f days",
        graph_name,
        time_start,
        time_end,
        period_days,
    )

    try:
        return _generate_graph_image_impl(config, graph_name, time_start, time_end, figsize, limit_altitude)
    finally:
        # どの経路（例外含む）でも figure を確実に解放する
        matplotlib.pyplot.close("all")
        gc.collect()


def _generate_graph_image_impl(
    config: amdar.config.Config,
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    figsize: tuple[float, float],
    limit_altitude: bool,
) -> tuple[bytes, float]:
    numpy_data = _fetch_numpy_data(config, graph_name, time_start, time_end, limit_altitude)

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

    data = prepare_data_numpy(numpy_data)
    del numpy_data

    if data.count < _MIN_DATA_POINTS:
        del data
        return _no_data_bytes(config, graph_name), 0

    # フォント設定はサブプロセス毎に必要
    from amdar.viewer.graph.plotting.styles import set_font  # 遅延 import

    set_font(config.font)

    graph_def = GRAPH_DEF_MAP[graph_name]
    try:
        # heatmap / contour_2d は元の時間範囲をプロット範囲として渡す。
        # 鉛直プロファイル系は末尾ウィンドウの決定に時間範囲を使う
        if graph_name in ("heatmap", "contour_2d", "temperature_profile", "hodograph"):
            img, elapsed = graph_def.func(data, figsize, time_start, time_end, limit_altitude)
        else:
            img, elapsed = graph_def.func(data, figsize, limit_altitude)
    except Exception as e:
        logging.warning("Failed to generate %s: %s", graph_name, str(e))
        # 描画失敗時はデータなし画像にフォールバック
        return _no_data_bytes(config, graph_name, "グラフの作成に失敗しました"), 0

    try:
        result_bytes = _image_to_bytes(img)
    finally:
        del img
        del data

    logging.debug(
        "generate_graph_image() completed for %s: elapsed=%.2f sec, image_size=%d bytes",
        graph_name,
        elapsed,
        len(result_bytes),
    )

    return result_bytes, elapsed
