"""
マテリアライズドビュー定期リフレッシュモジュール

集約テーブル（halfhourly_altitude_grid, threehour_altitude_grid）を
定期的にリフレッシュし、長期間グラフが最新データを反映するようにする。
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

import amdar.constants
import amdar.database.postgresql

if TYPE_CHECKING:
    import amdar.config


class MaterializedViewRefresher:
    """マテリアライズドビュー定期リフレッシュスケジューラ

    シングルトンパターンで実装。
    30分ごとにマテリアライズドビューをリフレッシュする。
    """

    _instance: MaterializedViewRefresher | None = None
    _lock = threading.Lock()

    _config: amdar.config.Config | None
    _timer: threading.Timer | None
    _running: bool
    _initialized: bool

    def __new__(cls) -> MaterializedViewRefresher:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = object.__new__(cls)
                    instance._config = None
                    instance._timer = None
                    instance._running = False
                    instance._initialized = False
                    cls._instance = instance
        assert cls._instance is not None  # noqa: S101 (シングルトンパターン)
        return cls._instance

    def initialize(self, config: amdar.config.Config) -> None:
        """初期化してリフレッシュを開始する"""
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            self._config = config
            self._initialized = True

            self._schedule_next(delay=amdar.constants.MATERIALIZED_VIEW_REFRESH_INITIAL_DELAY_SECONDS)
            logging.info(
                "MaterializedViewRefresher initialized: interval=%d sec",
                amdar.constants.MATERIALIZED_VIEW_REFRESH_INTERVAL_SECONDS,
            )

    def stop(self) -> None:
        """リフレッシュを停止する"""
        with self._lock:
            self._running = False
            if self._timer:
                self._timer.cancel()
                self._timer = None
            logging.info("MaterializedViewRefresher stopped")

    def _schedule_next(self, delay: float | None = None) -> None:
        """次回のリフレッシュをスケジュール"""
        if not self._initialized:
            return

        interval = delay if delay is not None else amdar.constants.MATERIALIZED_VIEW_REFRESH_INTERVAL_SECONDS
        self._timer = threading.Timer(interval, self._run_refresh)
        self._timer.daemon = True
        self._timer.start()

    def _run_refresh(self) -> None:
        """リフレッシュを実行"""
        if not self._config:
            return

        try:
            self._running = True
            start_time = time.perf_counter()

            conn = amdar.database.postgresql.open(
                self._config.database.host,
                self._config.database.port,
                self._config.database.name,
                self._config.database.user,
                self._config.database.password,
            )

            try:
                timings = amdar.database.postgresql.refresh_materialized_views(conn)
                elapsed = time.perf_counter() - start_time
                logging.info(
                    "[MV_REFRESH] Completed: halfhourly=%.1f sec, threehour=%.1f sec, total=%.1f sec",
                    timings.halfhourly_altitude_grid,
                    timings.threehour_altitude_grid,
                    elapsed,
                )
            finally:
                conn.close()

        except Exception:
            logging.exception("[MV_REFRESH] Error during refresh")
        finally:
            self._running = False
            self._schedule_next()

    @property
    def is_running(self) -> bool:
        """リフレッシュが実行中かどうか"""
        return self._running


# グローバルインスタンス
materialized_view_refresher = MaterializedViewRefresher()
