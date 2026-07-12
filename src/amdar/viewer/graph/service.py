"""グラフ生成サービス。

HTTP 層と pregeneration 層から呼ばれる唯一のエントリポイント。
キャッシュ確認 → プロセスプール経由でのグラフ生成 → キャッシュ保存 を一括で行う。

**重要**: 本サービスは matplotlib を直接呼ばない。matplotlib を触るのは
:mod:`amdar.viewer.graph.worker` の :func:`generate_graph_image` のみで、
それは必ずサブプロセスで実行される。

アプリ全体ではモジュールレベルの :data:`graph_service` インスタンスを共有する。
"""

from __future__ import annotations

import datetime
import logging
import multiprocessing
import multiprocessing.managers
import multiprocessing.pool
import pathlib
import threading
import time
from dataclasses import dataclass

import amdar.config
import amdar.viewer.api.job_manager
from amdar.constants import GRAPH_IMAGE_DPI, GraphName
from amdar.viewer.api.job_manager import JobStatus
from amdar.viewer.graph import cache, progress
from amdar.viewer.graph.definitions import GRAPH_DEF_MAP
from amdar.viewer.graph.pool import process_pool
from amdar.viewer.graph.worker import generate_graph_image

# 非同期結果ポーリングの間隔
_POLLING_INTERVAL_SECONDS = 0.5

# ジョブパラメータの同一性判定キー（重複排除用）
_JobParamsKey = tuple[GraphName, float, float, bool]


@dataclass
class _PendingJob:
    """ポーリングスレッドが追跡している非同期ジョブ。"""

    async_result: multiprocessing.pool.AsyncResult
    graph_name: GraphName
    cache_dir: pathlib.Path
    time_start: datetime.datetime
    time_end: datetime.datetime
    limit_altitude: bool
    params_key: _JobParamsKey
    # TIMEOUT 判定済みでも async_result は破棄せず、完走したらキャッシュ保存だけは行う
    timed_out: bool = False
    timed_out_at: float | None = None


class GraphService:
    """グラフ生成のオーケストレーション層。"""

    def __init__(self) -> None:
        self._config: amdar.config.Config | None = None
        self._cache_dir: pathlib.Path | None = None
        self._job_manager = amdar.viewer.api.job_manager.job_manager
        self._pending_jobs: dict[str, _PendingJob] = {}
        self._pending_lock = threading.Lock()
        # 実行中ジョブの (graph_name, start_ts, end_ts, limit_altitude) → job_id
        self._active_params: dict[_JobParamsKey, str] = {}
        self._checker_started = False
        self._initialized = False
        self._init_lock = threading.Lock()
        # ワーカーが実行開始時刻を通知するための共有 dict（遅延生成）
        self._sync_manager: multiprocessing.managers.SyncManager | None = None
        self._start_times: multiprocessing.managers.DictProxy[str, float] | None = None

    def initialize(self, config: amdar.config.Config, cache_dir: pathlib.Path) -> None:
        """設定とキャッシュディレクトリを設定する（多重呼び出しは無視）。"""
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            self._config = config
            self._cache_dir = cache_dir
            process_pool.configure(config)
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
                (graph_name, time_start, time_end, figsize, limit_altitude),
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
        - 同一パラメータのジョブが実行中: そのジョブの job_id をそのまま返す
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

        params_key = self._params_key(graph_name, time_start, time_end, limit_altitude)

        with self._pending_lock:
            existing_id = self._active_params.get(params_key)
            if existing_id is not None:
                existing_job = self._job_manager.get_job(existing_id)
                if existing_job and existing_job.status in (JobStatus.PENDING, JobStatus.PROCESSING):
                    logging.info("Deduplicated job request for %s: reusing job %s", graph_name, existing_id)
                    return existing_id
                # 終端状態や消失済みの参照は破棄して作り直す
                del self._active_params[params_key]

            job_id = self._job_manager.create_job(graph_name, time_start, time_end, limit_altitude)
            self._active_params[params_key] = job_id

        logging.info("[CACHE] MISS for %s, starting job %s", graph_name, job_id)
        try:
            self._dispatch_job(job_id, graph_name, time_start, time_end, limit_altitude, params_key)
        except Exception:
            logging.exception("Failed to dispatch job %s for %s", job_id, graph_name)
            self._release_active_params(params_key, job_id)
            self._job_manager.update_status(
                job_id, JobStatus.FAILED, error="Failed to dispatch job", stage="エラー"
            )
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

    def _params_key(
        self,
        graph_name: GraphName,
        time_start: datetime.datetime,
        time_end: datetime.datetime,
        limit_altitude: bool,
    ) -> _JobParamsKey:
        return (graph_name, time_start.timestamp(), time_end.timestamp(), limit_altitude)

    def _get_start_times(self) -> multiprocessing.managers.DictProxy[str, float]:
        """ワーカーの実行開始時刻を共有する dict（遅延生成）。"""
        start_times = self._start_times
        if start_times is None:
            with self._init_lock:
                start_times = self._start_times
                if start_times is None:
                    self._sync_manager = multiprocessing.Manager()
                    start_times = self._sync_manager.dict()
                    self._start_times = start_times
        return start_times

    def _release_active_params(self, params_key: _JobParamsKey, job_id: str) -> None:
        """重複排除マップから該当エントリを除去する（別ジョブに置き換わっていたら触らない）。"""
        with self._pending_lock:
            if self._active_params.get(params_key) == job_id:
                del self._active_params[params_key]

    def _dispatch_job(
        self,
        job_id: str,
        graph_name: GraphName,
        time_start: datetime.datetime,
        time_end: datetime.datetime,
        limit_altitude: bool,
        params_key: _JobParamsKey,
    ) -> None:
        """ジョブをプロセスプールに投げ、ポーリング監視に登録する。"""
        assert self._config is not None and self._cache_dir is not None  # noqa: S101

        self._job_manager.update_status(job_id, JobStatus.PROCESSING, progress=5, stage="キュー待機中...")

        figsize = self._figsize_for(graph_name)
        pool = process_pool.get_pool()
        start_times = self._get_start_times()

        self._ensure_checker_thread()

        async_result = pool.apply_async(
            generate_graph_image,
            (graph_name, time_start, time_end, figsize, limit_altitude, job_id, start_times),
        )

        with self._pending_lock:
            self._pending_jobs[job_id] = _PendingJob(
                async_result=async_result,
                graph_name=graph_name,
                cache_dir=self._cache_dir,
                time_start=time_start,
                time_end=time_end,
                limit_altitude=limit_altitude,
                params_key=params_key,
            )

        logging.info("Started async job %s for %s", job_id, graph_name)

    def _ensure_checker_thread(self) -> None:
        if self._checker_started:
            return
        with self._init_lock:
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

        finished: list[tuple[str, _PendingJob]] = []
        for job_id, pending in pending_snapshot:
            if self._check_job(job_id, pending):
                finished.append((job_id, pending))

        for job_id, pending in finished:
            with self._pending_lock:
                self._pending_jobs.pop(job_id, None)
                if self._active_params.get(pending.params_key) == job_id:
                    del self._active_params[pending.params_key]
            # 実行開始時刻の共有 dict エントリを削除
            try:
                self._get_start_times().pop(job_id, None)
            except Exception:
                logging.exception("Failed to remove start time entry for job %s", job_id)

    def _check_job(self, job_id: str, pending: _PendingJob) -> bool:
        """単一ジョブをチェックし、追跡終了 (success/failure/放棄) なら True を返す。"""
        try:
            if not pending.async_result.ready():
                return self._handle_unfinished_job(job_id, pending)
            return self._handle_finished_job(job_id, pending)
        except Exception:
            logging.exception("Error checking job %s", job_id)
            # 監視から外す前に FAILED に落とす（PROCESSING のまま放置しない）
            if not pending.timed_out:
                self._job_manager.update_status(
                    job_id, JobStatus.FAILED, error="Job monitoring failed", stage="エラー"
                )
            return True

    def _mark_timed_out(self, job_id: str, pending: _PendingJob, elapsed: float, reason: str) -> None:
        """ジョブを TIMEOUT にする。async_result は破棄せず追跡を続ける。"""
        pending.timed_out = True
        pending.timed_out_at = time.time()
        self._job_manager.update_status(
            job_id,
            JobStatus.TIMEOUT,
            error=f"ジョブがタイムアウトしました（{reason}、{int(elapsed)}秒経過）",
            stage="タイムアウト",
        )
        # 終端状態になったので、同一パラメータの新規リクエストを受け付けられるようにする
        self._release_active_params(pending.params_key, job_id)

    def _handle_unfinished_job(self, job_id: str, pending: _PendingJob) -> bool:
        """未完了ジョブの進捗更新とタイムアウト判定。"""
        now = time.time()

        if pending.timed_out:
            # TIMEOUT 済み: 完走すればキャッシュ保存するため待つが、待ちすぎたら放棄する
            assert pending.timed_out_at is not None  # noqa: S101
            abandon_after = progress.calculate_queue_wait_timeout(pending.time_start, pending.time_end)
            if now - pending.timed_out_at > abandon_after:
                logging.warning("Abandoning timed-out job %s for %s", job_id, pending.graph_name)
                return True
            return False

        job = self._job_manager.get_job(job_id)
        if job is None:
            # クリーンアップ等でジョブが消えた場合は追跡をやめる
            logging.warning("Job %s disappeared from JobManager, dropping", job_id)
            return True

        exec_started_at = self._get_start_times().get(job_id)

        if exec_started_at is None:
            # まだキュー待ち（ワーカーが実行を開始していない）
            queue_elapsed = now - job.created_at
            queue_limit = progress.calculate_queue_wait_timeout(job.time_start, job.time_end)
            if queue_elapsed > queue_limit:
                logging.warning(
                    "Job %s for %s timed out in queue after %.1f sec (max: %d sec)",
                    job_id,
                    pending.graph_name,
                    queue_elapsed,
                    queue_limit,
                )
                self._mark_timed_out(job_id, pending, queue_elapsed, "キュー待ち上限超過")
                return False
            self._job_manager.update_status(job_id, JobStatus.PROCESSING, progress=5, stage="キュー待機中...")
            return False

        # 実行中: タイムアウト起点は実行開始時刻
        elapsed = now - exec_started_at
        max_timeout = progress.calculate_polling_timeout(job.time_start, job.time_end)
        if elapsed > max_timeout:
            logging.warning(
                "Job %s for %s timed out after %.1f sec of execution (max: %d sec)",
                job_id,
                pending.graph_name,
                elapsed,
                max_timeout,
            )
            self._mark_timed_out(job_id, pending, elapsed, "生成時間超過")
            return False

        est_progress, stage = progress.estimate_progress_and_stage(job, exec_started_at)
        self._job_manager.update_status(job_id, JobStatus.PROCESSING, progress=est_progress, stage=stage)
        return False

    def _handle_finished_job(self, job_id: str, pending: _PendingJob) -> bool:
        """完了ジョブの結果を取得し、キャッシュ保存と履歴記録を行う。"""
        try:
            image_bytes, elapsed = pending.async_result.get(timeout=1)
        except Exception:
            logging.exception("Job %s failed for %s", job_id, pending.graph_name)
            if not pending.timed_out:
                self._job_manager.update_status(
                    job_id, JobStatus.FAILED, error="Job execution failed", stage="エラー"
                )
            return True

        if image_bytes:
            cache.save_to_cache(
                pending.cache_dir,
                pending.graph_name,
                pending.time_start,
                pending.time_end,
                pending.limit_altitude,
                image_bytes,
            )
            progress.record_generation_time(
                pending.graph_name, pending.time_start, pending.time_end, pending.limit_altitude, elapsed
            )

        if pending.timed_out:
            # ジョブステータスは TIMEOUT のまま（キャッシュ保存と履歴記録のみ実施）
            logging.info(
                "Timed-out job %s for %s finished late (%.2f sec); cached result only",
                job_id,
                pending.graph_name,
                elapsed,
            )
            return True

        self._job_manager.update_status(
            job_id, JobStatus.COMPLETED, result=image_bytes, progress=100, stage="完了"
        )
        logging.info("Job %s completed for %s (%.2f sec)", job_id, pending.graph_name, elapsed)
        return True


# モジュールレベルの共有インスタンス
graph_service = GraphService()
