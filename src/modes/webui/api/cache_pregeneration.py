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
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    import pathlib

    import modes.config

# 事前生成の間隔（秒）
PREGENERATION_INTERVAL_SECONDS = 25 * 60  # 25分

# デフォルト表示期間（日）
DEFAULT_PERIOD_DAYS = 7

# 事前生成対象のグラフ
PREGENERATION_GRAPHS = [
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
    _config: modes.config.Config | None
    _cache_dir: pathlib.Path | None
    _timer: threading.Timer | None
    _running: bool
    _initialized: bool

    def __new__(cls) -> Self:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = object.__new__(cls)
                    instance._config = None  # noqa: SLF001
                    instance._cache_dir = None  # noqa: SLF001
                    instance._timer = None  # noqa: SLF001
                    instance._running = False  # noqa: SLF001
                    instance._initialized = False  # noqa: SLF001
                    cls._instance = instance
        return cls._instance  # type: ignore[return-value]

    def initialize(self, config: modes.config.Config, cache_dir: pathlib.Path) -> None:
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
                len(PREGENERATION_GRAPHS),
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
            now = datetime.datetime.now(tz=datetime.UTC)
            time_end = now.replace(second=0, microsecond=0)
            time_start = time_end - datetime.timedelta(days=DEFAULT_PERIOD_DAYS)

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
                len(PREGENERATION_GRAPHS),
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
        import modes.webui.api.graph as graph_module

        generated = 0

        for graph_name in PREGENERATION_GRAPHS:
            try:
                # キャッシュが既にあるかチェック
                cached_image, cache_filename = graph_module.get_cached_image(
                    self._cache_dir,
                    graph_name,
                    time_start,
                    time_end,
                    limit_altitude,
                )

                if cached_image:
                    logging.debug("[PREGEN] Cache exists for %s: %s", graph_name, cache_filename)
                    generated += 1
                    continue

                # グラフを生成
                logging.info("[PREGEN] Generating %s...", graph_name)

                graph_def = graph_module.GRAPH_DEF_MAP.get(graph_name)
                if not graph_def:
                    logging.warning("[PREGEN] Unknown graph: %s", graph_name)
                    continue

                figsize = tuple(x / graph_module.IMAGE_DPI for x in graph_def.size)

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
