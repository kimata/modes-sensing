"""
キャッシュ事前生成モジュール

デフォルト7日間表示用のグラフを25分ごとに自動生成し、
キャッシュが常に存在する状態を維持する。
"""

from __future__ import annotations

import datetime
import logging
import threading
import time
from typing import TYPE_CHECKING

import my_lib.time

from amdar.constants import (
    DEFAULT_PREGENERATION_DAYS,
    GRAPH_IMAGE_DPI,
    PREGENERATION_INTERVAL_SECONDS,
    GraphName,
)

if TYPE_CHECKING:
    import pathlib

    import amdar.config

# 事前生成対象のグラフ
_PREGENERATION_GRAPHS: list[GraphName] = [
    "scatter_2d",
    "contour_2d",
    "density",
    "heatmap",
    "temperature",
    "wind_direction",
    "scatter_3d",
    "contour_3d",
]


class CachePregenerator:
    """キャッシュ事前生成スケジューラ

    シングルトンパターンで実装。
    25分ごとにデフォルト7日間表示用のグラフを事前生成する。
    """

    _instance: CachePregenerator | None = None
    _lock = threading.Lock()

    # インスタンス属性の型宣言
    _config: amdar.config.Config | None
    _cache_dir: pathlib.Path | None
    _timer: threading.Timer | None
    _running: bool
    _initialized: bool

    def __new__(cls) -> CachePregenerator:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = object.__new__(cls)
                    instance._config = None
                    instance._cache_dir = None
                    instance._timer = None
                    instance._running = False
                    instance._initialized = False
                    cls._instance = instance
        assert cls._instance is not None  # noqa: S101 (シングルトンパターン)
        return cls._instance

    def initialize(self, config: amdar.config.Config, cache_dir: pathlib.Path) -> None:
        """初期化して事前生成を開始する"""
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            self._config = config
            self._cache_dir = cache_dir
            self._initialized = True

            # 初回は少し遅延してから開始（アプリ起動完了を待つ）
            self._schedule_next(delay=10)
            logging.info(
                "CachePregenerator initialized: interval=%d sec, graphs=%d",
                PREGENERATION_INTERVAL_SECONDS,
                len(_PREGENERATION_GRAPHS),
            )

    def stop(self) -> None:
        """事前生成を停止する"""
        with self._lock:
            self._running = False
            if self._timer:
                self._timer.cancel()
                self._timer = None
            logging.info("CachePregenerator stopped")

    def _schedule_next(self, delay: float | None = None) -> None:
        """次回の事前生成をスケジュール"""
        if not self._initialized:
            return

        interval = delay if delay is not None else PREGENERATION_INTERVAL_SECONDS
        self._timer = threading.Timer(interval, self._run_pregeneration)
        self._timer.daemon = True
        self._timer.start()

    def _run_pregeneration(self) -> None:
        """事前生成を実行"""
        if not self._config or not self._cache_dir:
            return

        try:
            self._running = True
            start_time = time.perf_counter()

            # 時間範囲を計算（現在時刻から7日前まで）
            # JSTで統一し、分単位で正規化（ユーザーリクエストと一致させる）
            now = my_lib.time.now()
            time_end = now.replace(second=0, microsecond=0)
            time_start = time_end - datetime.timedelta(days=DEFAULT_PREGENERATION_DAYS)

            logging.info(
                "[PREGEN] Starting pregeneration: %s to %s",
                time_start.isoformat(),
                time_end.isoformat(),
            )

            # 各グラフを生成
            # limit_altitude=False（デフォルト表示）のみ
            generated = self._generate_graphs(time_start, time_end, limit_altitude=False)

            elapsed = time.perf_counter() - start_time
            logging.info(
                "[PREGEN] Completed: %d/%d graphs in %.1f sec",
                generated,
                len(_PREGENERATION_GRAPHS),
                elapsed,
            )

        except Exception:
            logging.exception("[PREGEN] Error during pregeneration")
        finally:
            self._running = False
            # 次回をスケジュール
            self._schedule_next()

    def _generate_graphs(
        self,
        time_start: datetime.datetime,
        time_end: datetime.datetime,
        limit_altitude: bool,
    ) -> int:
        """グラフを生成してキャッシュに保存

        Returns
        -------
            生成成功したグラフ数

        """
        # 型の狭小化（_run_pregenerationで既にチェック済み）
        assert self._config is not None  # noqa: S101
        assert self._cache_dir is not None  # noqa: S101

        # 循環インポートを避けるため関数内でインポート
        import amdar.viewer.api.graph as graph_module

        generated = 0

        for graph_name in _PREGENERATION_GRAPHS:
            try:
                # キャッシュの有効性をチェック（TTL残り時間も考慮）
                cache_info = graph_module.find_matching_cache(
                    self._cache_dir,
                    graph_name,
                    time_start,
                    time_end,
                    limit_altitude,
                )

                if cache_info:
                    # TTL残り時間を計算
                    current_time = time.time()
                    cache_age = current_time - cache_info.created_at
                    ttl_remaining = graph_module.CACHE_TTL_SECONDS - cache_age

                    # 次回の事前生成までTTLが持つ場合はスキップ
                    if ttl_remaining > PREGENERATION_INTERVAL_SECONDS:
                        logging.debug(
                            "[PREGEN] Cache valid for %s: %s (TTL remaining: %.0f sec)",
                            graph_name,
                            cache_info.path.name,
                            ttl_remaining,
                        )
                        generated += 1
                        continue

                    logging.info(
                        "[PREGEN] Cache expiring soon for %s (TTL remaining: %.0f sec), regenerating",
                        graph_name,
                        ttl_remaining,
                    )

                # グラフを生成
                logging.info("[PREGEN] Generating %s...", graph_name)

                graph_def = graph_module.GRAPH_DEF_MAP.get(graph_name)
                if not graph_def:
                    logging.warning("[PREGEN] Unknown graph: %s", graph_name)
                    continue

                figsize = tuple(x / GRAPH_IMAGE_DPI for x in graph_def.size)

                # 同期的にグラフを生成
                result = graph_module.plot_in_subprocess(
                    self._config,
                    graph_name,
                    time_start,
                    time_end,
                    figsize,
                    limit_altitude,
                )

                if result:
                    image_bytes, elapsed = result
                    if image_bytes:
                        # キャッシュに保存
                        graph_module.save_to_cache(
                            self._cache_dir,
                            graph_name,
                            time_start,
                            time_end,
                            limit_altitude,
                            image_bytes,
                        )
                        logging.info("[PREGEN] Generated %s in %.1f sec", graph_name, elapsed)
                        generated += 1

            except Exception:
                logging.exception("[PREGEN] Failed to generate %s", graph_name)

        return generated

    @property
    def is_running(self) -> bool:
        """事前生成が実行中かどうか"""
        return self._running


# グローバルインスタンス
cache_pregenerator = CachePregenerator()
