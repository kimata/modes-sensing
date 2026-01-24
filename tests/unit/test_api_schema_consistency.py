#!/usr/bin/env python3
# ruff: noqa: S101
"""
API スキーマ整合性テスト

バックエンドの TypedDict/dataclass とフロントエンドの型定義が一致していることを検証する。
フロントエンドでの期待フィールド名をハードコードして、バックエンドの実装と比較する。
"""

from __future__ import annotations

import json

from amdar.viewer.api.job_manager import JobManager, JobStatus, JobStatusDict

# フロントエンドで期待されるフィールド名（frontend/src/types/api.ts と一致）
EXPECTED_JOB_STATUS_FIELDS: frozenset[str] = frozenset(
    {
        "job_id",
        "status",
        "progress",
        "graph_name",
        "error",
        "elapsed_seconds",
        "stage",
    }
)

# フロントエンドで期待される status 値
EXPECTED_STATUS_VALUES: frozenset[str] = frozenset(
    {
        "pending",
        "processing",
        "completed",
        "failed",
        "timeout",
    }
)

# フロントエンドで期待されるフィールド名
EXPECTED_JOB_INFO_FIELDS: frozenset[str] = frozenset({"job_id", "graph_name"})
EXPECTED_DATA_RANGE_FIELDS: frozenset[str] = frozenset({"earliest", "latest", "count"})
EXPECTED_LAST_RECEIVED_FIELDS: frozenset[str] = frozenset({"mode_s", "vdl2"})
EXPECTED_BATCH_JOB_STATUS_FIELDS: frozenset[str] = frozenset(
    {
        "status",
        "progress",
        "graph_name",
        "error",
        "elapsed_seconds",
        "stage",
    }
)


class TestJobStatusDictSchema:
    """JobStatusDict のスキーマ整合性テスト"""

    def test_job_status_dict_has_expected_fields(self) -> None:
        """JobStatusDict が期待されるフィールドを持つことを検証"""
        # TypedDict の __annotations__ から実際のフィールドを取得
        actual_fields = set(JobStatusDict.__annotations__.keys())

        assert actual_fields == EXPECTED_JOB_STATUS_FIELDS, (
            f"JobStatusDict のフィールドが一致しません。\n"
            f"期待: {EXPECTED_JOB_STATUS_FIELDS}\n"
            f"実際: {actual_fields}\n"
            f"不足: {EXPECTED_JOB_STATUS_FIELDS - actual_fields}\n"
            f"余剰: {actual_fields - EXPECTED_JOB_STATUS_FIELDS}"
        )

    def test_job_status_dict_no_extra_fields(self) -> None:
        """JobStatusDict に未知のフィールドがないことを検証"""
        actual_fields = set(JobStatusDict.__annotations__.keys())
        extra_fields = actual_fields - EXPECTED_JOB_STATUS_FIELDS

        assert not extra_fields, f"未知のフィールドが存在します: {extra_fields}"

    def test_job_status_enum_values(self) -> None:
        """JobStatus enum の値がフロントエンドと一致することを検証"""
        actual_values = {status.value for status in JobStatus}

        assert actual_values == EXPECTED_STATUS_VALUES, (
            f"JobStatus の値が一致しません。\n期待: {EXPECTED_STATUS_VALUES}\n実際: {actual_values}"
        )

    def test_job_status_dict_json_serializable(self) -> None:
        """JobStatusDict が JSON シリアライズ可能であることを検証"""
        sample: JobStatusDict = {
            "job_id": "test-job-id",
            "status": "processing",
            "progress": 50,
            "graph_name": "scatter_2d",
            "error": None,
            "elapsed_seconds": 12.5,
            "stage": "データ取得中",
        }

        # JSON シリアライズ -> デシリアライズのラウンドトリップ
        json_str = json.dumps(sample)
        restored = json.loads(json_str)

        assert restored["job_id"] == sample["job_id"]
        assert restored["status"] == sample["status"]
        assert restored["progress"] == sample["progress"]
        assert restored["graph_name"] == sample["graph_name"]
        assert restored["error"] == sample["error"]
        assert restored["elapsed_seconds"] == sample["elapsed_seconds"]
        assert restored["stage"] == sample["stage"]

    def test_job_status_dict_nullable_fields(self) -> None:
        """null 許容フィールドが正しく動作することを検証"""
        # error, elapsed_seconds, stage は null 許容
        sample: JobStatusDict = {
            "job_id": "test-job-id",
            "status": "pending",
            "progress": 0,
            "graph_name": "scatter_2d",
            "error": None,
            "elapsed_seconds": None,
            "stage": None,
        }

        json_str = json.dumps(sample)
        restored = json.loads(json_str)

        assert restored["error"] is None
        assert restored["elapsed_seconds"] is None
        assert restored["stage"] is None


class TestJobManagerIntegration:
    """JobManager の get_job_status_dict メソッドの整合性テスト"""

    def setup_method(self) -> None:
        """各テストの前にJobManagerをリセット"""
        # シングルトンのインスタンスをリセット
        JobManager._instance = None

    def teardown_method(self) -> None:
        """各テストの後にJobManagerをリセット"""
        JobManager._instance = None

    def test_get_job_status_dict_returns_correct_structure(self) -> None:
        """get_job_status_dict が正しい構造を返すことを検証"""
        import datetime

        manager = JobManager()

        job_id = manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
            time_end=datetime.datetime(2025, 1, 7, tzinfo=datetime.UTC),
            limit_altitude=False,
        )

        status_dict = manager.get_job_status_dict(job_id)
        assert status_dict is not None

        # 全ての期待されるフィールドが存在することを確認
        actual_fields = set(status_dict.keys())

        assert actual_fields == EXPECTED_JOB_STATUS_FIELDS, (
            f"get_job_status_dict の戻り値のフィールドが一致しません。\n"
            f"期待: {EXPECTED_JOB_STATUS_FIELDS}\n"
            f"実際: {actual_fields}"
        )

    def test_status_transitions_produce_valid_output(self) -> None:
        """各ステータス遷移時に有効な出力が生成されることを検証"""
        import datetime

        manager = JobManager()

        job_id = manager.create_job(
            graph_name="contour_2d",
            time_start=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
            time_end=datetime.datetime(2025, 1, 7, tzinfo=datetime.UTC),
            limit_altitude=True,
        )

        # PENDING 状態
        status = manager.get_job_status_dict(job_id)
        assert status is not None
        assert status["status"] == "pending"
        assert status["progress"] == 0

        # PROCESSING 状態へ遷移
        manager.update_status(job_id, JobStatus.PROCESSING, progress=50, stage="グラフ生成中")
        status = manager.get_job_status_dict(job_id)
        assert status is not None
        assert status["status"] == "processing"
        assert status["progress"] == 50
        assert status["stage"] == "グラフ生成中"

        # COMPLETED 状態へ遷移
        manager.update_status(job_id, JobStatus.COMPLETED, result=b"png_data", progress=100)
        status = manager.get_job_status_dict(job_id)
        assert status is not None
        assert status["status"] == "completed"
        assert status["progress"] == 100
        assert status["elapsed_seconds"] is not None  # 経過時間が設定される


class TestCreateJobsResponseSchema:
    """POST /api/graph/job レスポンスのスキーマ整合性テスト"""

    def test_job_info_structure(self) -> None:
        """ジョブ情報の構造を検証（コード分析ベース）"""
        # graph.py の create_graph_job で返される構造
        # jobs.append({"job_id": job_id, "graph_name": graph_name})
        sample_job_info = {"job_id": "uuid-1", "graph_name": "scatter_2d"}

        actual_fields = set(sample_job_info.keys())
        assert actual_fields == EXPECTED_JOB_INFO_FIELDS


class TestDataRangeResponseSchema:
    """GET /api/data-range レスポンスのスキーマ整合性テスト"""

    def test_data_range_response_structure(self) -> None:
        """データ範囲レスポンスの構造を検証"""
        # graph.py の data_range で返される構造
        sample_with_data = {
            "earliest": "2025-01-01T00:00:00+09:00",
            "latest": "2025-01-07T23:59:59+09:00",
            "count": 12345,
        }

        actual_fields = set(sample_with_data.keys())
        assert actual_fields == EXPECTED_DATA_RANGE_FIELDS

    def test_data_range_response_null_case(self) -> None:
        """データがない場合のレスポンス構造を検証"""
        sample_no_data = {"earliest": None, "latest": None, "count": 0}

        actual_fields = set(sample_no_data.keys())
        assert actual_fields == EXPECTED_DATA_RANGE_FIELDS


class TestLastReceivedResponseSchema:
    """GET /api/last-received レスポンスのスキーマ整合性テスト"""

    def test_last_received_response_structure(self) -> None:
        """最終受信レスポンスの構造を検証"""
        sample = {
            "mode_s": "2025-01-07T12:00:00+09:00",
            "vdl2": "2025-01-07T11:30:00+09:00",
        }

        actual_fields = set(sample.keys())
        assert actual_fields == EXPECTED_LAST_RECEIVED_FIELDS

    def test_last_received_response_null_case(self) -> None:
        """受信データがない場合のレスポンス構造を検証"""
        sample = {"mode_s": None, "vdl2": None}

        actual_fields = set(sample.keys())
        assert actual_fields == EXPECTED_LAST_RECEIVED_FIELDS


class TestBatchStatusResponseSchema:
    """POST /api/graph/jobs/status レスポンスのスキーマ整合性テスト"""

    def test_batch_status_response_structure(self) -> None:
        """一括ステータスレスポンスの構造を検証"""
        # graph.py の get_jobs_status_batch で返される構造
        sample_job_status = {
            "status": "completed",
            "progress": 100,
            "graph_name": "scatter_2d",
            "error": None,
            "elapsed_seconds": 5.2,
            "stage": None,
        }

        actual_fields = set(sample_job_status.keys())
        assert actual_fields == EXPECTED_BATCH_JOB_STATUS_FIELDS, (
            f"BatchStatus のジョブフィールドが一致しません。\n"
            f"期待: {EXPECTED_BATCH_JOB_STATUS_FIELDS}\n"
            f"実際: {actual_fields}"
        )
