#!/usr/bin/env python3
# ruff: noqa: S101
"""
Flask API スキーマ検証テスト

Flask テストクライアントを使用して、実際の API レスポンスのスキーマを検証する。
"""

from __future__ import annotations

import json
import unittest.mock

import pytest

import amdar.cli.webui as webui
import amdar.config

# フロントエンドで期待されるフィールド名
EXPECTED_CREATE_JOB_RESPONSE_FIELDS: frozenset[str] = frozenset({"jobs"})
EXPECTED_JOB_INFO_FIELDS: frozenset[str] = frozenset({"job_id", "graph_name"})
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
EXPECTED_STATUS_VALUES: frozenset[str] = frozenset(
    {"pending", "processing", "completed", "failed", "timeout"}
)
EXPECTED_BATCH_RESPONSE_FIELDS: frozenset[str] = frozenset({"jobs"})
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
EXPECTED_DATA_RANGE_FIELDS: frozenset[str] = frozenset({"earliest", "latest", "count"})
EXPECTED_LAST_RECEIVED_FIELDS: frozenset[str] = frozenset({"mode_s", "vdl2"})
EXPECTED_AGGREGATE_STATS_FIELDS: frozenset[str] = frozenset({"exists", "stats"})


@pytest.fixture(scope="module")
def app(config: amdar.config.Config):
    """Flask テストアプリケーションを作成"""
    app = webui.create_app(config)
    app.config["TESTING"] = True
    return app


@pytest.fixture(scope="module")
def client(app):
    """Flask テストクライアントを返す"""
    return app.test_client()


class TestCreateGraphJobEndpoint:
    """POST /api/graph/job エンドポイントのスキーマテスト"""

    def test_create_job_response_structure(self, client) -> None:
        """ジョブ作成レスポンスの構造を検証"""
        # グラフ生成はモックして、レスポンス構造のみ検証
        with unittest.mock.patch(
            "amdar.viewer.api.graph.get_cached_image",
            return_value=(b"fake_png_data", "cached_file.png"),
        ):
            response = client.post(
                "/modes-sensing/api/graph/job",
                data=json.dumps(
                    {
                        "graphs": ["scatter_2d"],
                        "start": "2025-01-01T00:00:00Z",
                        "end": "2025-01-07T00:00:00Z",
                        "limit_altitude": False,
                    }
                ),
                content_type="application/json",
            )

        assert response.status_code == 200
        data = response.get_json()
        assert data is not None

        # トップレベルのフィールド検証
        assert set(data.keys()) == EXPECTED_CREATE_JOB_RESPONSE_FIELDS

        # jobs 配列の検証
        assert isinstance(data["jobs"], list)
        if len(data["jobs"]) > 0:
            job_info = data["jobs"][0]
            assert set(job_info.keys()) == EXPECTED_JOB_INFO_FIELDS

    def test_create_job_error_response(self, client) -> None:
        """エラーレスポンスの構造を検証"""
        # 必須パラメータなしでリクエスト
        response = client.post(
            "/modes-sensing/api/graph/job",
            data=json.dumps({}),
            content_type="application/json",
        )

        assert response.status_code == 400
        data = response.get_json()
        assert data is not None
        assert "error" in data


class TestJobStatusEndpoint:
    """GET /api/graph/job/{id}/status エンドポイントのスキーマテスト"""

    def test_job_status_response_structure(self, client) -> None:
        """ジョブステータスレスポンスの構造を検証"""
        # まずジョブを作成
        with unittest.mock.patch(
            "amdar.viewer.api.graph.get_cached_image",
            return_value=(b"fake_png_data", "cached_file.png"),
        ):
            create_response = client.post(
                "/modes-sensing/api/graph/job",
                data=json.dumps(
                    {
                        "graphs": ["scatter_2d"],
                        "start": "2025-01-01T00:00:00Z",
                        "end": "2025-01-07T00:00:00Z",
                        "limit_altitude": False,
                    }
                ),
                content_type="application/json",
            )

        assert create_response.status_code == 200
        create_data = create_response.get_json()
        job_id = create_data["jobs"][0]["job_id"]

        # ステータス取得
        status_response = client.get(f"/modes-sensing/api/graph/job/{job_id}/status")

        assert status_response.status_code == 200
        status_data = status_response.get_json()
        assert status_data is not None

        # フィールド検証
        assert set(status_data.keys()) == EXPECTED_JOB_STATUS_FIELDS

        # status 値の検証
        assert status_data["status"] in EXPECTED_STATUS_VALUES

        # 型検証
        assert isinstance(status_data["job_id"], str)
        assert isinstance(status_data["progress"], int)
        assert isinstance(status_data["graph_name"], str)
        # error, elapsed_seconds, stage は null 許容
        assert status_data["error"] is None or isinstance(status_data["error"], str)
        assert status_data["elapsed_seconds"] is None or isinstance(status_data["elapsed_seconds"], float)
        assert status_data["stage"] is None or isinstance(status_data["stage"], str)

    def test_job_status_not_found(self, client) -> None:
        """存在しないジョブのエラーレスポンスを検証"""
        response = client.get("/modes-sensing/api/graph/job/nonexistent-job-id/status")

        assert response.status_code == 404
        data = response.get_json()
        assert data is not None
        assert "error" in data


class TestBatchStatusEndpoint:
    """POST /api/graph/jobs/status エンドポイントのスキーマテスト"""

    def test_batch_status_response_structure(self, client) -> None:
        """一括ステータスレスポンスの構造を検証"""
        # まずジョブを作成
        with unittest.mock.patch(
            "amdar.viewer.api.graph.get_cached_image",
            return_value=(b"fake_png_data", "cached_file.png"),
        ):
            create_response = client.post(
                "/modes-sensing/api/graph/job",
                data=json.dumps(
                    {
                        "graphs": ["scatter_2d", "contour_2d"],
                        "start": "2025-01-01T00:00:00Z",
                        "end": "2025-01-07T00:00:00Z",
                        "limit_altitude": False,
                    }
                ),
                content_type="application/json",
            )

        assert create_response.status_code == 200
        create_data = create_response.get_json()
        job_ids = [job["job_id"] for job in create_data["jobs"]]

        # 一括ステータス取得
        batch_response = client.post(
            "/modes-sensing/api/graph/jobs/status",
            data=json.dumps({"job_ids": job_ids}),
            content_type="application/json",
        )

        assert batch_response.status_code == 200
        batch_data = batch_response.get_json()
        assert batch_data is not None

        # トップレベルのフィールド検証
        assert set(batch_data.keys()) == EXPECTED_BATCH_RESPONSE_FIELDS

        # jobs は辞書（job_id -> status）
        assert isinstance(batch_data["jobs"], dict)

        # 各ジョブのステータスフィールド検証
        for job_id, status in batch_data["jobs"].items():
            assert isinstance(job_id, str)
            assert set(status.keys()) == EXPECTED_BATCH_JOB_STATUS_FIELDS


class TestDataRangeEndpoint:
    """GET /api/data-range エンドポイントのスキーマテスト"""

    def test_data_range_response_structure(self, client) -> None:
        """データ範囲レスポンスの構造を検証"""
        # データベース接続をモック
        mock_result = unittest.mock.MagicMock()
        mock_result.earliest = None
        mock_result.latest = None
        mock_result.count = 0

        with (
            unittest.mock.patch("amdar.viewer.api.graph._connect_database"),
            unittest.mock.patch(
                "amdar.database.postgresql.fetch_data_range",
                return_value=mock_result,
            ),
        ):
            response = client.get("/modes-sensing/api/data-range")

        assert response.status_code == 200
        data = response.get_json()
        assert data is not None

        # フィールド検証
        assert set(data.keys()) == EXPECTED_DATA_RANGE_FIELDS

    def test_data_range_with_data(self, client) -> None:
        """データがある場合のレスポンスを検証"""
        import datetime

        mock_result = unittest.mock.MagicMock()
        mock_result.earliest = datetime.datetime(2025, 1, 1, 0, 0, 0)
        mock_result.latest = datetime.datetime(2025, 1, 7, 23, 59, 59)
        mock_result.count = 12345

        with (
            unittest.mock.patch("amdar.viewer.api.graph._connect_database"),
            unittest.mock.patch(
                "amdar.database.postgresql.fetch_data_range",
                return_value=mock_result,
            ),
        ):
            response = client.get("/modes-sensing/api/data-range")

        assert response.status_code == 200
        data = response.get_json()
        assert data is not None

        # フィールド検証
        assert set(data.keys()) == EXPECTED_DATA_RANGE_FIELDS

        # データがある場合の型検証
        assert isinstance(data["earliest"], str)
        assert isinstance(data["latest"], str)
        assert isinstance(data["count"], int)


class TestLastReceivedEndpoint:
    """GET /api/last-received エンドポイントのスキーマテスト"""

    def test_last_received_response_structure(self, client) -> None:
        """最終受信レスポンスの構造を検証"""
        mock_result = unittest.mock.MagicMock()
        mock_result.mode_s = None
        mock_result.vdl2 = None

        with (
            unittest.mock.patch("amdar.viewer.api.graph._connect_database"),
            unittest.mock.patch(
                "amdar.database.postgresql.fetch_last_received_by_method",
                return_value=mock_result,
            ),
        ):
            response = client.get("/modes-sensing/api/last-received")

        assert response.status_code == 200
        data = response.get_json()
        assert data is not None

        # フィールド検証
        assert set(data.keys()) == EXPECTED_LAST_RECEIVED_FIELDS

    def test_last_received_with_data(self, client) -> None:
        """データがある場合のレスポンスを検証"""
        import datetime

        mock_result = unittest.mock.MagicMock()
        mock_result.mode_s = datetime.datetime(2025, 1, 7, 12, 0, 0)
        mock_result.vdl2 = datetime.datetime(2025, 1, 7, 11, 30, 0)

        with (
            unittest.mock.patch("amdar.viewer.api.graph._connect_database"),
            unittest.mock.patch(
                "amdar.database.postgresql.fetch_last_received_by_method",
                return_value=mock_result,
            ),
        ):
            response = client.get("/modes-sensing/api/last-received")

        assert response.status_code == 200
        data = response.get_json()
        assert data is not None

        # フィールド検証
        assert set(data.keys()) == EXPECTED_LAST_RECEIVED_FIELDS

        # データがある場合の型検証
        assert isinstance(data["mode_s"], str)
        assert isinstance(data["vdl2"], str)


class TestAggregateStatsEndpoint:
    """GET /api/aggregate-stats エンドポイントのスキーマテスト"""

    def test_aggregate_stats_response_has_exists_field(self, client) -> None:
        """集約統計レスポンスに exists フィールドがあることを検証"""
        mock_stats = unittest.mock.MagicMock()
        mock_stats.to_dict.return_value = {"count": 100}

        with (
            unittest.mock.patch("amdar.viewer.api.graph._connect_database"),
            unittest.mock.patch(
                "amdar.database.postgresql.check_materialized_views_exist",
                return_value=True,
            ),
            unittest.mock.patch(
                "amdar.database.postgresql.get_materialized_view_stats",
                return_value=mock_stats,
            ),
        ):
            response = client.get("/modes-sensing/api/aggregate-stats")

        assert response.status_code == 200
        data = response.get_json()
        assert data is not None

        # トップレベルに exists と stats フィールドがあることを確認
        assert set(data.keys()) == EXPECTED_AGGREGATE_STATS_FIELDS
