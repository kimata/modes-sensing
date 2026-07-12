"""グラフ生成用プロセスプール。

matplotlib はスレッドセーフではないため、グラフ生成はメインスレッドから
切り離してサブプロセスで実行する必要がある。本モジュールはそのための
プロセスプールを管理する。アプリ全体ではモジュールレベルの
:data:`process_pool` インスタンスを共有する。

Config はタスク毎に pickle 転送せず、Pool の initializer で各ワーカーに
一度だけ渡す（:func:`amdar.viewer.graph.worker.init_worker`）。
"""

from __future__ import annotations

import atexit
import logging
import multiprocessing
import multiprocessing.pool
import threading

import amdar.config
from amdar.constants import PROCESS_POOL_MAX_TASKS_PER_CHILD


class ProcessPoolManager:
    """グラフ生成用プロセスプールの管理。"""

    def __init__(self) -> None:
        self._pool: multiprocessing.pool.Pool | None = None
        self._config: amdar.config.Config | None = None
        self._lock = threading.Lock()

    def configure(self, config: amdar.config.Config) -> None:
        """ワーカー初期化に使う設定を登録する（プール生成前に呼ぶこと）。"""
        with self._lock:
            self._config = config

    def get_pool(self) -> multiprocessing.pool.Pool:
        """プロセスプールを取得（必要に応じて遅延作成）。"""
        if self._pool is None:
            with self._lock:
                if self._pool is None:
                    if self._config is None:
                        msg = "ProcessPoolManager is not configured. Call configure(config) first."
                        raise RuntimeError(msg)

                    # 循環 import を避けるため遅延 import
                    from amdar.viewer.graph.worker import init_worker

                    # CPU コア数の半分（最小 2、最大 10）を採用
                    max_workers = min(max(multiprocessing.cpu_count() // 2, 2), 10)
                    self._pool = multiprocessing.Pool(
                        processes=max_workers,
                        initializer=init_worker,
                        initargs=(self._config,),
                        maxtasksperchild=PROCESS_POOL_MAX_TASKS_PER_CHILD,
                    )
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
            except (OSError, ValueError) as e:
                logging.warning("Error cleaning up process pool: %s", e)


# モジュールレベルの共有インスタンス
process_pool = ProcessPoolManager()
