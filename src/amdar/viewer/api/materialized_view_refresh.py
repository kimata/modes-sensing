"""
集約テーブル定期更新モジュール

集約テーブル（halfhourly_altitude_grid, threehour_altitude_grid）を
定期的に増分更新し、長期間グラフが最新データを反映するようにする。
（旧実装ではマテリアライズドビューを REFRESH していたため、モジュール名・
クラス名は互換性のため維持している）
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, ClassVar

import amdar.constants
import amdar.database.postgresql

if TYPE_CHECKING:
    import amdar.config


class MaterializedViewRefresher:
    """集約テーブル定期更新スケジューラ

    シングルトンパターンで実装。
    30分ごとに集約テーブルを増分更新する。
    stop() 後は再スケジュールされず、再度 initialize() することで再開できる。
    """

    _instance: ClassVar[MaterializedViewRefresher | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    _config: amdar.config.Config | None
    _timer: threading.Timer | None
    _running: bool
    _initialized: bool
    _stop_requested: bool

    def __new__(cls) -> MaterializedViewRefresher:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = object.__new__(cls)
                    instance._config = None
                    instance._timer = None
                    instance._running = False
                    instance._initialized = False
                    instance._stop_requested = False
                    cls._instance = instance
        assert cls._instance is not None  # noqa: S101 (シングルトンパターン)
        return cls._instance

    def initialize(self, config: amdar.config.Config) -> None:
        """初期化して定期更新を開始する

        stop() 後に再度呼び出すことで更新を再開できる。
        """
        with self._lock:
            if self._initialized:
                return

            self._config = config
            self._initialized = True
            self._stop_requested = False

            self._schedule_next_locked(delay=amdar.constants.MATERIALIZED_VIEW_REFRESH_INITIAL_DELAY_SECONDS)
            logging.info(
                "MaterializedViewRefresher initialized: interval=%d sec",
                amdar.constants.MATERIALIZED_VIEW_REFRESH_INTERVAL_SECONDS,
            )

    def stop(self) -> None:
        """定期更新を停止する

        更新が実行中でも、完了後の再スケジュールは行われない。
        """
        with self._lock:
            self._stop_requested = True
            self._initialized = False
            if self._timer:
                self._timer.cancel()
                self._timer = None
            logging.info("MaterializedViewRefresher stopped")

    def _schedule_next_locked(self, delay: float | None = None) -> None:
        """次回の更新をスケジュールする（呼び出し側で _lock を保持していること）"""
        if self._stop_requested or not self._initialized:
            return

        interval = delay if delay is not None else amdar.constants.MATERIALIZED_VIEW_REFRESH_INTERVAL_SECONDS
        self._timer = threading.Timer(interval, self._run_refresh)
        self._timer.daemon = True
        self._timer.start()

    def _run_refresh(self) -> None:
        """集約テーブルの増分更新を実行する"""
        config = self._config
        if config is None:
            return

        try:
            self._running = True
            start_time = time.perf_counter()

            conn = amdar.database.postgresql.open(
                config.database.host,
                config.database.port,
                config.database.name,
                config.database.user,
                config.database.password,
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
            # stop() 済みの場合は再スケジュールしない（停止フラグをロック下で確認）
            with self._lock:
                self._schedule_next_locked()

    @property
    def is_running(self) -> bool:
        """更新が実行中かどうか"""
        return self._running


# グローバルインスタンス
materialized_view_refresher = MaterializedViewRefresher()
