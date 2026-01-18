#!/usr/bin/env python3
# ruff: noqa: S101
"""
job_manager.py のテスト
"""

import datetime
import time

import pytest

import amdar.viewer.api.job_manager as job_manager
from amdar.constants import JOB_EXPIRY_SECONDS, JOB_TIMEOUT_SECONDS


class TestJobStatus:
    """JobStatus のテスト"""

    def test_job_status_values(self):
        """ジョブステータスの値"""
        assert job_manager.JobStatus.PENDING.value == "pending"
        assert job_manager.JobStatus.PROCESSING.value == "processing"
        assert job_manager.JobStatus.COMPLETED.value == "completed"
        assert job_manager.JobStatus.FAILED.value == "failed"
        assert job_manager.JobStatus.TIMEOUT.value == "timeout"


class TestJob:
    """Job のテスト"""

    def test_job_creation(self):
        """ジョブ作成"""
        job = job_manager.Job(
            job_id="test-123",
            graph_name="scatter_2d",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=False,
        )

        assert job.job_id == "test-123"
        assert job.graph_name == "scatter_2d"
        assert job.status == job_manager.JobStatus.PENDING
        assert job.progress == 0
        assert job.result is None
        assert job.error is None


class TestJobManager:
    """JobManager のテスト"""

    @pytest.fixture
    def manager(self):
        """JobManager インスタンス"""
        manager = job_manager.JobManager()
        # 既存のジョブをクリア
        with manager._jobs_lock:
            manager._jobs.clear()
        yield manager

    def test_singleton(self):
        """シングルトンパターン"""
        manager1 = job_manager.JobManager()
        manager2 = job_manager.JobManager()
        assert manager1 is manager2

    def test_create_job(self, manager):
        """ジョブ作成"""
        job_id = manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=False,
        )

        assert job_id is not None
        job = manager.get_job(job_id)
        assert job is not None
        assert job.graph_name == "scatter_2d"
        assert job.status == job_manager.JobStatus.PENDING

    def test_create_job_with_id(self, manager):
        """カスタムIDでジョブ作成"""
        job_id = manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=False,
            job_id="custom-id-123",
        )

        assert job_id == "custom-id-123"

    def test_reuse_completed_job(self, manager):
        """完了済みジョブの再利用"""
        # ジョブ作成
        job_id = manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=False,
            job_id="reuse-test-123",
        )

        # 完了としてマーク
        manager.update_status(job_id, job_manager.JobStatus.COMPLETED, result=b"test")

        # 同じIDで再作成を試みる
        reused_id = manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=False,
            job_id="reuse-test-123",
        )

        assert reused_id == "reuse-test-123"

    def test_get_nonexistent_job(self, manager):
        """存在しないジョブの取得"""
        job = manager.get_job("nonexistent-job")
        assert job is None

    def test_update_status(self, manager):
        """ステータス更新"""
        job_id = manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=False,
        )

        manager.update_status(
            job_id,
            job_manager.JobStatus.PROCESSING,
            progress=50,
            stage="データ取得中",
        )

        job = manager.get_job(job_id)
        assert job.status == job_manager.JobStatus.PROCESSING
        assert job.progress == 50
        assert job.stage == "データ取得中"
        assert job.started_at is not None

    def test_update_status_completed(self, manager):
        """完了ステータス更新"""
        job_id = manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=False,
        )

        manager.update_status(
            job_id,
            job_manager.JobStatus.COMPLETED,
            result=b"PNG data",
            progress=100,
        )

        job = manager.get_job(job_id)
        assert job.status == job_manager.JobStatus.COMPLETED
        assert job.result == b"PNG data"
        assert job.completed_at is not None

    def test_update_status_failed(self, manager):
        """失敗ステータス更新"""
        job_id = manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=False,
        )

        manager.update_status(
            job_id,
            job_manager.JobStatus.FAILED,
            error="テストエラー",
        )

        job = manager.get_job(job_id)
        assert job.status == job_manager.JobStatus.FAILED
        assert job.error == "テストエラー"
        assert job.completed_at is not None

    def test_update_status_nonexistent_job(self, manager):
        """存在しないジョブのステータス更新"""
        # 例外なく完了
        manager.update_status(
            "nonexistent-job",
            job_manager.JobStatus.COMPLETED,
        )

    def test_get_job_status_dict(self, manager):
        """ステータス辞書取得"""
        job_id = manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=False,
        )

        manager.update_status(job_id, job_manager.JobStatus.PROCESSING, progress=50)

        status_dict = manager.get_job_status_dict(job_id)

        assert status_dict is not None
        assert status_dict["job_id"] == job_id
        assert status_dict["status"] == "processing"
        assert status_dict["progress"] == 50
        assert status_dict["graph_name"] == "scatter_2d"
        assert status_dict["elapsed_seconds"] is not None

    def test_get_job_status_dict_nonexistent(self, manager):
        """存在しないジョブのステータス辞書取得"""
        status_dict = manager.get_job_status_dict("nonexistent-job")
        assert status_dict is None

    def test_get_stats(self, manager):
        """統計情報取得"""
        # ジョブを作成
        job_id1 = manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=False,
        )
        manager.create_job(
            graph_name="heatmap",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=True,
        )

        # 1つを完了にする
        manager.update_status(job_id1, job_manager.JobStatus.COMPLETED)

        stats = manager.get_stats()

        assert stats["total"] == 2
        assert stats.get("pending", 0) == 1
        assert stats.get("completed", 0) == 1

    def test_cleanup_old_jobs(self, manager):
        """古いジョブのクリーンアップ"""
        job_id = manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=False,
        )

        # 完了として即座にクリーンアップ対象にする
        manager.update_status(job_id, job_manager.JobStatus.COMPLETED)
        job = manager.get_job(job_id)
        # 時間を過去に設定
        job.completed_at = time.time() - JOB_EXPIRY_SECONDS - 1

        manager._cleanup_old_jobs()

        # ジョブが削除されていることを確認
        assert manager.get_job(job_id) is None

    def test_cleanup_timeout_jobs(self, manager):
        """タイムアウトしたジョブのクリーンアップ"""
        job_id = manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime.now(),
            time_end=datetime.datetime.now() + datetime.timedelta(hours=1),
            limit_altitude=False,
        )

        # 作成時間を過去に設定
        job = manager.get_job(job_id)
        job.created_at = time.time() - JOB_TIMEOUT_SECONDS - 1

        manager._cleanup_old_jobs()

        # タイムアウトステータスになっていることを確認
        job = manager.get_job(job_id)
        assert job.status == job_manager.JobStatus.TIMEOUT
        assert "timed out" in job.error
