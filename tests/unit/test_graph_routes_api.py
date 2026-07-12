#!/usr/bin/env python3
# ruff: noqa: S101
"""graph_routes / data_routes の HTTP レベルテスト。

Flask テストクライアントで SSE・レート制限・入力検証（400 系）の動作を検証する。
"""

from __future__ import annotations

import datetime
import io
import json
import threading
import typing
import unittest.mock

import PIL.Image
import pytest

import amdar.cli.webui as webui
import amdar.viewer.api.data_routes as data_routes
import amdar.viewer.api.graph_routes as graph_routes
from amdar.viewer.api.job_manager import JobStatus, job_manager


@pytest.fixture(scope="module")
def app(config):
    app = webui.create_app(config)
    app.config["TESTING"] = True
    return app


@pytest.fixture(scope="module")
def client(app):
    return app.test_client()


def _create_completed_job(graph_name="scatter_2d") -> str:
    job_id = job_manager.create_job(
        graph_name=graph_name,
        time_start=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
        time_end=datetime.datetime(2025, 1, 7, tzinfo=datetime.UTC),
        limit_altitude=False,
    )
    job_manager.update_status(job_id, JobStatus.COMPLETED, result=b"png", progress=100, stage="完了")
    return job_id


class TestCreateGraphJobValidation:
    """POST /api/graph/job の入力検証テスト。"""

    _BODY_BASE: typing.ClassVar[dict] = {
        "start": "2025-01-01T00:00:00Z",
        "end": "2025-01-07T00:00:00Z",
        "limit_altitude": False,
    }

    def _post(self, client, body):
        return client.post(
            "/modes-sensing/api/graph/job",
            data=json.dumps(body),
            content_type="application/json",
        )

    def test_unknown_graph_name_is_rejected(self, client):
        res = self._post(client, {**self._BODY_BASE, "graphs": ["nonexistent_graph"]})
        assert res.status_code == 400
        assert "Unknown graph names" in res.get_json()["error"]

    def test_too_many_graphs_rejected(self, client):
        graphs = [f"graph_{i}" for i in range(20)]
        res = self._post(client, {**self._BODY_BASE, "graphs": graphs})
        assert res.status_code == 400
        assert "Too many graphs" in res.get_json()["error"]

    def test_start_after_end_rejected(self, client):
        res = self._post(
            client,
            {
                "graphs": ["scatter_2d"],
                "start": "2025-01-07T00:00:00Z",
                "end": "2025-01-01T00:00:00Z",
            },
        )
        assert res.status_code == 400
        assert "start must be before end" in res.get_json()["error"]

    def test_invalid_datetime_rejected(self, client):
        res = self._post(client, {"graphs": ["scatter_2d"], "start": "garbage", "end": "garbage"})
        assert res.status_code == 400

    def test_duplicate_graphs_are_deduplicated(self, client):
        with unittest.mock.patch(
            "amdar.viewer.graph.cache.get_cached_image",
            return_value=(b"fake_png_data", "cached_file.png"),
        ):
            res = self._post(client, {**self._BODY_BASE, "graphs": ["scatter_2d", "scatter_2d"]})

        assert res.status_code == 200
        jobs = res.get_json()["jobs"]
        assert len(jobs) == 1
        assert jobs[0]["graph_name"] == "scatter_2d"


class TestOldGraphApiValidation:
    """GET /api/graph/<name> の日時パース検証。"""

    def test_invalid_datetime_returns_400(self, client):
        res = client.get("/modes-sensing/api/graph/scatter_2d?start=garbage")
        assert res.status_code == 400
        assert "Invalid datetime" in res.get_json()["error"]


class TestBatchStatus:
    """GET/POST /api/graph/jobs/status のテスト。"""

    def test_unknown_job_id_included_as_unknown(self, client):
        job_id = _create_completed_job()

        res = client.post(
            "/modes-sensing/api/graph/jobs/status",
            data=json.dumps({"job_ids": [job_id, "no-such-job"]}),
            content_type="application/json",
        )

        assert res.status_code == 200
        jobs = res.get_json()["jobs"]
        assert jobs[job_id]["status"] == "completed"
        assert jobs["no-such-job"] == {"status": "unknown"}

    def test_get_variant_with_query_param(self, client):
        job_id = _create_completed_job()

        res = client.get(f"/modes-sensing/api/graph/jobs/status?job_ids={job_id},no-such-job")

        assert res.status_code == 200
        jobs = res.get_json()["jobs"]
        assert jobs[job_id]["status"] == "completed"
        assert jobs["no-such-job"] == {"status": "unknown"}

    def test_get_variant_requires_job_ids(self, client):
        res = client.get("/modes-sensing/api/graph/jobs/status")
        assert res.status_code == 400


class TestJobEventsSse:
    """GET /api/graph/job/events (SSE) のテスト。"""

    def test_empty_job_ids_rejected(self, client):
        res = client.get("/modes-sensing/api/graph/job/events")
        assert res.status_code == 400

    def test_too_many_job_ids_rejected(self, client):
        ids = ",".join(f"job-{i}" for i in range(33))
        res = client.get(f"/modes-sensing/api/graph/job/events?job_ids={ids}")
        assert res.status_code == 400

    def test_unknown_jobs_immediately_done(self, client):
        res = client.get("/modes-sensing/api/graph/job/events?job_ids=aaa,bbb")

        assert res.status_code == 200
        assert res.mimetype == "text/event-stream"
        assert res.headers["Cache-Control"] == "no-cache"
        assert res.headers["X-Accel-Buffering"] == "no"

        body = res.get_data(as_text=True)
        assert "event: status" in body
        assert '"aaa": {"status": "unknown"}' in body
        assert "event: done" in body

    def test_completed_job_snapshot_then_done(self, client):
        job_id = _create_completed_job()

        res = client.get(f"/modes-sensing/api/graph/job/events?job_ids={job_id}")

        body = res.get_data(as_text=True)
        events = [block for block in body.split("\n\n") if block]
        assert events[0].startswith("event: status")
        payload = json.loads(events[0].split("\ndata: ", 1)[1])
        assert payload["jobs"][job_id]["status"] == "completed"
        assert payload["jobs"][job_id]["job_id"] == job_id
        assert events[-1].startswith("event: done")

    def test_status_transition_to_done(self, client):
        """PROCESSING → COMPLETED の遷移で status イベント → done が届く"""
        job_id = job_manager.create_job(
            graph_name="scatter_2d",
            time_start=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
            time_end=datetime.datetime(2025, 1, 7, tzinfo=datetime.UTC),
            limit_altitude=False,
        )
        job_manager.update_status(job_id, JobStatus.PROCESSING, progress=50, stage="生成中...")

        timer = threading.Timer(
            1.0,
            lambda: job_manager.update_status(
                job_id, JobStatus.COMPLETED, result=b"png", progress=100, stage="完了"
            ),
        )
        timer.start()
        try:
            res = client.get(f"/modes-sensing/api/graph/job/events?job_ids={job_id}")
            body = res.get_data(as_text=True)
        finally:
            timer.cancel()

        assert '"status": "processing"' in body
        assert '"status": "completed"' in body
        assert "event: done" in body


class TestRefreshAggregatesRateLimit:
    """POST /api/refresh-aggregates のレート制限テスト。"""

    def test_second_request_within_interval_is_429(self, client):
        data_routes._last_refresh_time = 0.0

        mock_timings = unittest.mock.MagicMock()
        mock_timings.to_dict.return_value = {"halfhourly": 1.0}
        mock_stats = unittest.mock.MagicMock()
        mock_stats.to_dict.return_value = {"count": 1}

        with (
            unittest.mock.patch("amdar.viewer.api.data_routes._connect_database"),
            unittest.mock.patch(
                "amdar.database.postgresql.refresh_materialized_views",
                return_value=mock_timings,
            ),
            unittest.mock.patch(
                "amdar.database.postgresql.get_materialized_view_stats",
                return_value=mock_stats,
            ),
        ):
            first = client.post("/modes-sensing/api/refresh-aggregates")
            second = client.post("/modes-sensing/api/refresh-aggregates")

        assert first.status_code == 200
        assert second.status_code == 429
        assert "retry_after_seconds" in second.get_json()

        # 後続テストへの影響を避ける
        data_routes._last_refresh_time = 0.0


class TestDebugEndpoint:
    """GET /api/debug/date-parse のテスト。"""

    def test_returns_404_when_not_debug(self, client):
        res = client.get("/modes-sensing/api/debug/date-parse")
        assert res.status_code == 404


class TestErrorImage:
    """Pillow ベースのエラー画像生成のテスト。"""

    def test_render_error_image_returns_png(self):
        data = graph_routes._render_error_image("Graph generation failed\ntest", (600, 400), None)

        with PIL.Image.open(io.BytesIO(data)) as img:
            assert img.format == "PNG"
            assert img.size == (600, 400)

    def test_render_error_image_is_cached(self):
        a = graph_routes._render_error_image("same message", (600, 400), None)
        b = graph_routes._render_error_image("same message", (600, 400), None)
        assert a is b
