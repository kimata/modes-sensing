"""グラフ生成用プロセスプール。

matplotlib はスレッドセーフではないため、グラフ生成はメインスレッドから
切り離してサブプロセスで実行する必要がある。本モジュールはそのための
プロセスプールをシングルトンとして管理する。
"""

from __future__ import annotations

import atexit
import logging
import multiprocessing
import multiprocessing.pool
import threading
from typing import ClassVar


class ProcessPoolManager:
    """グラフ生成用プロセスプールのシングルトン管理。"""

    _instance: ClassVar[ProcessPoolManager | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    _pool: multiprocessing.pool.Pool | None

    def __new__(cls) -> ProcessPoolManager:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = object.__new__(cls)
                    instance._pool = None
                    cls._instance = instance
        assert cls._instance is not None  # noqa: S101 (シングルトンパターン)
        return cls._instance

    def get_pool(self) -> multiprocessing.pool.Pool:
        """プロセスプールを取得（必要に応じて遅延作成）。"""
        if self._pool is None:
            with self._lock:
                if self._pool is None:
                    # CPU コア数の半分（最小 2、最大 10）を採用
                    max_workers = min(max(multiprocessing.cpu_count() // 2, 2), 10)
                    self._pool = multiprocessing.Pool(processes=max_workers)
                    atexit.register(self.cleanup)
                    logging.info("Created global process pool with %d workers", max_workers)
        assert self._pool is not None  # noqa: S101 (上のロックブロックで保証)
        return self._pool

    def cleanup(self) -> None:
        """プロセスプールを停止する。"""
        if self._pool is not None:
            try:
                self._pool.close()
                self._pool.join()
                self._pool = None
                logging.info("Cleaned up global process pool")
            except Exception as e:
                logging.warning("Error cleaning up process pool: %s", e)


process_pool = ProcessPoolManager()
