"""グラフ生成サービス。

HTTP 層と pregeneration 層から呼ばれる唯一のエントリポイント。
キャッシュ確認 → プロセスプール経由でのグラフ生成 → キャッシュ保存 を一括で行う。

**重要**: 本サービスは matplotlib を直接呼ばない。matplotlib を触るのは
:mod:`amdar.viewer.graph.worker` の :func:`generate_graph_image` のみで、
それは必ずサブプロセスで実行される。
"""

from __future__ import annotations

import datetime
import logging
import multiprocessing
import multiprocessing.pool
import pathlib
import threading
import time
from dataclasses import dataclass
from typing import ClassVar

import amdar.config
from amdar.constants import GRAPH_IMAGE_DPI, GraphName
from amdar.viewer.api.job_manager import JobManager, JobStatus
from amdar.viewer.graph import cache, progress
from amdar.viewer.graph.definitions import GRAPH_DEF_MAP
from amdar.viewer.graph.pool import process_pool
from amdar.viewer.graph.worker import generate_graph_image

# 非同期結果ポーリングの間隔
_POLLING_INTERVAL_SECONDS = 0.5


@dataclass
class _PendingJob:
    """ポーリングスレッドが追跡している非同期ジョブ。"""

    async_result: multiprocessing.pool.AsyncResult
    graph_name: GraphName
    cache_dir: pathlib.Path


class GraphService:
    """グラフ生成のオーケストレーション層（シングルトン）。"""

    _instance: ClassVar[GraphService | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    _config: amdar.config.Config | None
    _cache_dir: pathlib.Path | None
    _job_manager: JobManager
    _pending_jobs: dict[str, _PendingJob]
    _pending_lock: threading.Lock
    _checker_started: bool
    _initialized: bool

    def __new__(cls) -> GraphService:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = object.__new__(cls)
                    instance._config = None
                    instance._cache_dir = None
                    instance._job_manager = JobManager()
                    instance._pending_jobs = {}
                    instance._pending_lock = threading.Lock()
                    instance._checker_started = False
                    instance._initialized = False
                    cls._instance = instance
        assert cls._instance is not None  # noqa: S101 (シングルトンパターン)
        return cls._instance

    def initialize(self, config: amdar.config.Config, cache_dir: pathlib.Path) -> None:
        """設定とキャッシュディレクトリを設定する（多重呼び出しは無視）。"""
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            self._config = config
            self._cache_dir = cache_dir
            self._initialized = True
            logging.info("GraphService initialized: cache_dir=%s", cache_dir)

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    @property
    def cache_dir(self) -> pathlib.Path:
        """キャッシュディレクトリ（初期化前アクセスはエラー）。"""
        self._ensure_initialized()
        assert self._cache_dir is not None  # noqa: S101
        return self._cache_dir

    # ------------------------------------------------------------------
    # 同期 API: pregeneration や CLI から使う
    # ------------------------------------------------------------------

    def generate_sync(
        self,
        graph_name: GraphName,
        time_start: datetime.datetime,
        time_end: datetime.datetime,
        limit_altitude: bool = False,
    ) -> bytes:
        """グラフを同期生成する（キャッシュヒット時は即返却）。

        プロセスプール経由で生成するため、matplotlib は必ずサブプロセスで動く。
        本メソッドはブロッキング呼び出しなので、HTTP リクエストから直接は呼ばない。

        Returns:
            PNG バイト列。データなし時の代替画像でも空にはならない。
        """
        self._ensure_initialized()
        assert self._config is not None and self._cache_dir is not None  # noqa: S101

        cached, cache_filename = cache.get_cached_image(
            self._cache_dir, graph_name, time_start, time_end, limit_altitude
        )
        if cached:
            logging.info(
                "[CACHE] HIT (sync) for %s: %s (%d bytes)",
                graph_name,
                cache_filename,
                len(cached),
            )
            return cached

        figsize = self._figsize_for(graph_name)
        timeout_sec = progress.calculate_timeout(time_start, time_end)

        pool = process_pool.get_pool()
        try:
            async_result = pool.apply_async(
                generate_graph_image,
                (self._config, graph_name, time_start, time_end, figsize, limit_altitude),
            )
            image_bytes, elapsed = async_result.get(timeout=timeout_sec)
        except multiprocessing.TimeoutError:
            logging.warning("Graph generation timed out (sync) for %s (%d sec)", graph_name, timeout_sec)
            raise

        if image_bytes:
            cache.save_to_cache(
                self._cache_dir, graph_name, time_start, time_end, limit_altitude, image_bytes
            )
            progress.record_generation_time(graph_name, time_start, time_end, limit_altitude, elapsed)

        return image_bytes

    # ------------------------------------------------------------------
    # 非同期 API: HTTP リクエストハンドラから使う
    # ------------------------------------------------------------------

    def submit_async(
        self,
        graph_name: GraphName,
        time_start: datetime.datetime,
        time_end: datetime.datetime,
        limit_altitude: bool = False,
    ) -> str:
        """ジョブを登録して job_id を返す。

        - キャッシュヒット: 即座に COMPLETED 状態のジョブを安定 ID で返す
          （ブラウザキャッシュが効きやすくなる）
        - ミス: 新規ジョブを作成しプロセスプールに投入。ポーリングスレッドが
          完了を検知して JobManager を更新する
        """
        self._ensure_initialized()
        assert self._config is not None and self._cache_dir is not None  # noqa: S101

        cached, cache_filename = cache.get_cached_image(
            self._cache_dir, graph_name, time_start, time_end, limit_altitude
        )

        if cached:
            stable_id = cache.generate_stable_job_id(graph_name, time_start, time_end, limit_altitude)
            job_id = self._job_manager.create_job(
                graph_name, time_start, time_end, limit_altitude, job_id=stable_id
            )
            logging.info(
                "[CACHE] HIT (async) for %s: %s (%d bytes, job=%s)",
                graph_name,
                cache_filename,
                len(cached),
                job_id,
            )
            self._job_manager.update_status(
                job_id, JobStatus.COMPLETED, result=cached, progress=100, stage="完了"
            )
            return job_id

        job_id = self._job_manager.create_job(graph_name, time_start, time_end, limit_altitude)
        logging.info("[CACHE] MISS for %s, starting job %s", graph_name, job_id)
        self._dispatch_job(job_id, graph_name, time_start, time_end, limit_altitude)
        return job_id

    # ------------------------------------------------------------------
    # 内部メソッド
    # ------------------------------------------------------------------

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            msg = "GraphService is not initialized. Call initialize(config, cache_dir) first."
            raise RuntimeError(msg)

    def _figsize_for(self, graph_name: GraphName) -> tuple[float, float]:
        size = GRAPH_DEF_MAP[graph_name].size
        return (size[0] / GRAPH_IMAGE_DPI, size[1] / GRAPH_IMAGE_DPI)

    def _dispatch_job(
        self,
        job_id: str,
        graph_name: GraphName,
        time_start: datetime.datetime,
        time_end: datetime.datetime,
        limit_altitude: bool,
    ) -> None:
        """ジョブをプロセスプールに投げ、ポーリング監視に登録する。"""
        assert self._config is not None and self._cache_dir is not None  # noqa: S101

        self._job_manager.update_status(job_id, JobStatus.PROCESSING, progress=10, stage="開始中...")

        figsize = self._figsize_for(graph_name)
        pool = process_pool.get_pool()

        self._ensure_checker_thread()

        async_result = pool.apply_async(
            generate_graph_image,
            (self._config, graph_name, time_start, time_end, figsize, limit_altitude),
        )

        with self._pending_lock:
            self._pending_jobs[job_id] = _PendingJob(
                async_result=async_result,
                graph_name=graph_name,
                cache_dir=self._cache_dir,
            )

        logging.info("Started async job %s for %s", job_id, graph_name)

    def _ensure_checker_thread(self) -> None:
        if self._checker_started:
            return
        with self._lock:
            if self._checker_started:
                return
            thread = threading.Thread(
                target=self._checker_loop,
                daemon=True,
                name="GraphServiceChecker",
            )
            thread.start()
            self._checker_started = True
            logging.info("Started GraphService checker thread")

    def _checker_loop(self) -> None:
        while True:
            time.sleep(_POLLING_INTERVAL_SECONDS)
            try:
                self._poll_pending_jobs()
            except Exception:
                logging.exception("Error in GraphService checker loop")

    def _poll_pending_jobs(self) -> None:
        """保留中のジョブをチェックし、完了/タイムアウトを処理する。"""
        with self._pending_lock:
            pending_snapshot = list(self._pending_jobs.items())

        completed: list[str] = []
        for job_id, pending in pending_snapshot:
            if self._check_job(job_id, pending):
                completed.append(job_id)

        if completed:
            with self._pending_lock:
                for job_id in completed:
                    self._pending_jobs.pop(job_id, None)

    def _check_job(self, job_id: str, pending: _PendingJob) -> bool:
        """単一ジョブをチェックし、完了 (success/failure/timeout) なら True を返す。"""
        try:
            if not pending.async_result.ready():
                return self._handle_unfinished_job(job_id, pending.graph_name)
            return self._handle_finished_job(job_id, pending)
        except Exception:
            logging.exception("Error checking job %s", job_id)
            return True

    def _handle_unfinished_job(self, job_id: str, graph_name: GraphName) -> bool:
        """未完了ジョブの進捗更新とタイムアウト判定。"""
        job = self._job_manager.get_job(job_id)
        if job and job.started_at:
            elapsed = time.time() - job.started_at
            max_timeout = progress.calculate_polling_timeout(job.time_start, job.time_end)
            if elapsed > max_timeout:
                logging.warning(
                    "Job %s for %s timed out after %.1f sec (max: %d sec)",
                    job_id,
                    graph_name,
                    elapsed,
                    max_timeout,
                )
                self._job_manager.update_status(
                    job_id,
                    JobStatus.TIMEOUT,
                    error=f"ジョブがタイムアウトしました（{int(elapsed)}秒経過）",
                    stage="タイムアウト",
                )
                return True

        if job is not None:
            est_progress, stage = progress.estimate_progress_and_stage(job)
            self._job_manager.update_status(job_id, JobStatus.PROCESSING, progress=est_progress, stage=stage)
        return False

    def _handle_finished_job(self, job_id: str, pending: _PendingJob) -> bool:
        """完了ジョブの結果を取得し、キャッシュ保存と履歴記録を行う。"""
        try:
            image_bytes, elapsed = pending.async_result.get(timeout=1)
        except Exception:
            logging.exception("Job %s failed for %s", job_id, pending.graph_name)
            self._job_manager.update_status(
                job_id, JobStatus.FAILED, error="Job execution failed", stage="エラー"
            )
            return True

        self._job_manager.update_status(
            job_id, JobStatus.COMPLETED, result=image_bytes, progress=100, stage="完了"
        )
        logging.info("Job %s completed for %s (%.2f sec)", job_id, pending.graph_name, elapsed)

        job = self._job_manager.get_job(job_id)
        if job and image_bytes:
            cache.save_to_cache(
                pending.cache_dir,
                pending.graph_name,
                job.time_start,
                job.time_end,
                job.limit_altitude,
                image_bytes,
            )
            progress.record_generation_time(
                pending.graph_name, job.time_start, job.time_end, job.limit_altitude, elapsed
            )
        return True


# モジュールレベルのシングルトンインスタンス
graph_service = GraphService()
