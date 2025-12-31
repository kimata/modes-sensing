"""
メモリ内ジョブ管理システム

グラフ生成の非同期処理を管理するためのシングルトンクラス。
外部依存（Redis等）なしで動作する。

機能:
- ジョブの登録・状態管理
- 結果の一時保存
- 古いジョブの自動クリーンアップ
"""

from __future__ import annotations

import datetime  # noqa: TC003
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class JobStatus(Enum):
    """ジョブの状態"""

    PENDING = "pending"  # キューに入った状態
    PROCESSING = "processing"  # 処理中
    COMPLETED = "completed"  # 完了
    FAILED = "failed"  # 失敗
    TIMEOUT = "timeout"  # タイムアウト


@dataclass
class Job:
    """ジョブ情報"""

    job_id: str
    graph_name: str
    time_start: datetime.datetime
    time_end: datetime.datetime
    limit_altitude: bool
    status: JobStatus = JobStatus.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    completed_at: float | None = None
    result: bytes | None = None  # PNG画像データ
    error: str | None = None
    progress: int = 0  # 0-100


class JobManager:
    """
    スレッドセーフなジョブ管理クラス（シングルトン）

    使用例:
        manager = JobManager()
        job_id = manager.create_job("scatter_2d", start, end, False)
        manager.update_status(job_id, JobStatus.PROCESSING, progress=50)
        job = manager.get_job(job_id)
    """

    _instance: JobManager | None = None
    _lock = threading.Lock()

    # 設定
    JOB_EXPIRY_SECONDS = 600  # 10分後に結果を削除
    CLEANUP_INTERVAL = 60  # 1分ごとにクリーンアップ

    def __new__(cls) -> JobManager:  # noqa: PYI034
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._jobs: dict[str, Job] = {}
                    instance._jobs_lock = threading.RLock()
                    instance._cleanup_started = False
                    cls._instance = instance
        return cls._instance

    def _ensure_cleanup_thread(self) -> None:
        """クリーンアップスレッドが起動していなければ起動"""
        if not self._cleanup_started:
            with self._lock:
                if not self._cleanup_started:
                    self._start_cleanup_thread()
                    self._cleanup_started = True

    def create_job(
        self,
        graph_name: str,
        time_start: datetime.datetime,
        time_end: datetime.datetime,
        limit_altitude: bool,
    ) -> str:
        """
        新しいジョブを作成してIDを返す

        Args:
            graph_name: グラフ名（例: "scatter_2d"）
            time_start: データ取得開始時刻
            time_end: データ取得終了時刻
            limit_altitude: 高度制限フラグ

        Returns:
            作成されたジョブのID（UUID）
        """
        self._ensure_cleanup_thread()

        job_id = str(uuid.uuid4())
        job = Job(
            job_id=job_id,
            graph_name=graph_name,
            time_start=time_start,
            time_end=time_end,
            limit_altitude=limit_altitude,
        )
        with self._jobs_lock:
            self._jobs[job_id] = job

        logging.info("Created job %s for graph %s", job_id, graph_name)
        return job_id

    def get_job(self, job_id: str) -> Job | None:
        """
        ジョブを取得

        Args:
            job_id: ジョブID

        Returns:
            ジョブ情報、見つからない場合はNone
        """
        with self._jobs_lock:
            return self._jobs.get(job_id)

    def update_status(
        self,
        job_id: str,
        status: JobStatus,
        result: bytes | None = None,
        error: str | None = None,
        progress: int | None = None,
    ) -> None:
        """
        ジョブステータスを更新

        Args:
            job_id: ジョブID
            status: 新しいステータス
            result: 完了時の結果（PNG画像データ）
            error: エラー時のメッセージ
            progress: 進捗率（0-100）
        """
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if job:
                job.status = status
                if status == JobStatus.PROCESSING and job.started_at is None:
                    job.started_at = time.time()
                if status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.TIMEOUT):
                    job.completed_at = time.time()
                if result is not None:
                    job.result = result
                if error is not None:
                    job.error = error
                if progress is not None:
                    job.progress = progress

                logging.debug(
                    "Updated job %s: status=%s, progress=%d",
                    job_id,
                    status.value,
                    job.progress,
                )

    def get_job_status_dict(self, job_id: str) -> dict[str, Any] | None:
        """
        ジョブステータスを辞書形式で取得（API用）

        Args:
            job_id: ジョブID

        Returns:
            ステータス辞書、見つからない場合はNone
        """
        job = self.get_job(job_id)
        if not job:
            return None

        elapsed = None
        if job.started_at:
            end_time = job.completed_at or time.time()
            elapsed = end_time - job.started_at

        return {
            "job_id": job.job_id,
            "status": job.status.value,
            "progress": job.progress,
            "graph_name": job.graph_name,
            "error": job.error,
            "elapsed_seconds": elapsed,
        }

    def _cleanup_old_jobs(self) -> None:
        """古いジョブを削除"""
        current_time = time.time()
        with self._jobs_lock:
            expired_ids = [
                job_id
                for job_id, job in self._jobs.items()
                if job.completed_at and (current_time - job.completed_at) > self.JOB_EXPIRY_SECONDS
            ]
            for job_id in expired_ids:
                del self._jobs[job_id]

            if expired_ids:
                logging.info("Cleaned up %d expired jobs", len(expired_ids))

    def _start_cleanup_thread(self) -> None:
        """バックグラウンドクリーンアップスレッドを開始"""

        def cleanup_loop() -> None:
            while True:
                time.sleep(self.CLEANUP_INTERVAL)
                try:
                    self._cleanup_old_jobs()
                except Exception:
                    logging.exception("Error in cleanup thread")

        thread = threading.Thread(target=cleanup_loop, daemon=True, name="JobManagerCleanup")
        thread.start()
        logging.info("Started job cleanup thread")

    def get_stats(self) -> dict[str, int]:
        """
        ジョブ統計情報を取得（デバッグ用）

        Returns:
            ステータス別のジョブ数
        """
        with self._jobs_lock:
            stats: dict[str, int] = {}
            for job in self._jobs.values():
                status_name = job.status.value
                stats[status_name] = stats.get(status_name, 0) + 1
            stats["total"] = len(self._jobs)
            return stats
