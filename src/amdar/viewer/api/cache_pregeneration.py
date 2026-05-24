"""キャッシュ事前生成スケジューラ。

デフォルト7日間表示用のグラフを定期的に自動生成し、キャッシュが常に
新鮮な状態を維持する。

実装上のポイント:
    - グラフ生成は :class:`amdar.viewer.graph.service.GraphService` に委譲。
      matplotlib はサブプロセスで実行されるため、タイマースレッドから安全に呼べる。
    - キャッシュが TTL の残り時間で十分カバーできる場合は再生成をスキップする。
"""

from __future__ import annotations

import datetime
import logging
import threading
import time
from typing import ClassVar

import my_lib.time

from amdar.constants import (
    CACHE_TTL_SECONDS,
    DEFAULT_PREGENERATION_DAYS,
    PREGENERATION_INTERVAL_SECONDS,
    GraphName,
)
from amdar.viewer.graph import cache
from amdar.viewer.graph.service import graph_service

# 事前生成対象のグラフ（デフォルト表示で使う 8 種）
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

# 初回実行までの遅延（アプリ起動完了を待つ）
_INITIAL_DELAY_SECONDS = 10


class CachePregenerator:
    """キャッシュ事前生成スケジューラ（シングルトン）。"""

    _instance: ClassVar[CachePregenerator | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    _timer: threading.Timer | None
    _running: bool
    _initialized: bool

    def __new__(cls) -> CachePregenerator:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = object.__new__(cls)
                    instance._timer = None
                    instance._running = False
                    instance._initialized = False
                    cls._instance = instance
        assert cls._instance is not None  # noqa: S101 (シングルトンパターン)
        return cls._instance

    def initialize(self) -> None:
        """事前生成を開始する（多重呼び出しは無視）。

        前提: :func:`amdar.viewer.graph.service.graph_service.initialize` が
        事前に呼ばれていること。
        """
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            self._initialized = True
            self._schedule_next(delay=_INITIAL_DELAY_SECONDS)
            logging.info(
                "CachePregenerator initialized: interval=%d sec, graphs=%d",
                PREGENERATION_INTERVAL_SECONDS,
                len(_PREGENERATION_GRAPHS),
            )

    def stop(self) -> None:
        """事前生成を停止する。"""
        with self._lock:
            self._running = False
            if self._timer:
                self._timer.cancel()
                self._timer = None
            logging.info("CachePregenerator stopped")

    @property
    def is_running(self) -> bool:
        """事前生成が実行中かどうか。"""
        return self._running

    # ------------------------------------------------------------------
    # 内部メソッド
    # ------------------------------------------------------------------

    def _schedule_next(self, delay: float | None = None) -> None:
        if not self._initialized:
            return
        interval = delay if delay is not None else PREGENERATION_INTERVAL_SECONDS
        self._timer = threading.Timer(interval, self._run_pregeneration)
        self._timer.daemon = True
        self._timer.start()

    def _run_pregeneration(self) -> None:
        try:
            self._running = True
            start_time = time.perf_counter()

            # 分単位で正規化（ユーザリクエストと一致させる）
            now = my_lib.time.now()
            time_end = now.replace(second=0, microsecond=0)
            time_start = time_end - datetime.timedelta(days=DEFAULT_PREGENERATION_DAYS)

            logging.info(
                "[PREGEN] Starting pregeneration: %s to %s",
                time_start.isoformat(),
                time_end.isoformat(),
            )

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
            self._schedule_next()

    def _generate_graphs(
        self,
        time_start: datetime.datetime,
        time_end: datetime.datetime,
        limit_altitude: bool,
    ) -> int:
        """対象グラフを生成し、成功件数を返す。"""
        cache_dir = graph_service.cache_dir

        generated = 0
        for graph_name in _PREGENERATION_GRAPHS:
            try:
                if self._cache_still_fresh(cache_dir, graph_name, time_start, time_end, limit_altitude):
                    generated += 1
                    continue

                logging.info("[PREGEN] Generating %s...", graph_name)
                graph_service.generate_sync(graph_name, time_start, time_end, limit_altitude)
                generated += 1
            except Exception:
                logging.exception("[PREGEN] Failed to generate %s", graph_name)

        return generated

    def _cache_still_fresh(
        self,
        cache_dir,
        graph_name: GraphName,
        time_start: datetime.datetime,
        time_end: datetime.datetime,
        limit_altitude: bool,
    ) -> bool:
        """次回事前生成までキャッシュが TTL 内に収まるか。"""
        cache_info = cache.find_matching_cache(cache_dir, graph_name, time_start, time_end, limit_altitude)
        if cache_info is None:
            return False

        current_time = time.time()
        cache_age = current_time - cache_info.created_at
        ttl_remaining = CACHE_TTL_SECONDS - cache_age

        if ttl_remaining > PREGENERATION_INTERVAL_SECONDS:
            logging.debug(
                "[PREGEN] Cache valid for %s: %s (TTL remaining: %.0f sec)",
                graph_name,
                cache_info.path.name,
                ttl_remaining,
            )
            return True

        logging.info(
            "[PREGEN] Cache expiring soon for %s (TTL remaining: %.0f sec), regenerating",
            graph_name,
            ttl_remaining,
        )
        return False


cache_pregenerator = CachePregenerator()
