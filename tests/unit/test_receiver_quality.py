#!/usr/bin/env python3
# ruff: noqa: S101
"""
受信品質関連のテスト

- fetch_observation_counts_by_method / fetch_receiver_quality（DB モック）
- GET /api/receiver-quality / GET /api/metrics（Flask テストクライアント）
"""

from __future__ import annotations

import datetime
import unittest.mock

import flask
import pytest

import amdar.database.postgresql as db
import amdar.viewer.api.data_routes


@pytest.fixture(autouse=True)
def _clear_quality_cache():
    """各テスト前後で受信品質キャッシュをクリアする"""
    db._clear_receiver_quality_cache()
    yield
    db._clear_receiver_quality_cache()


def _mock_conn(rows):
    mock_conn = unittest.mock.MagicMock()
    mock_cursor = unittest.mock.MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = rows
    return mock_conn, mock_cursor


def _sample_quality() -> db.ReceiverQualityResult:
    return db.ReceiverQualityResult(
        last_hour=db.MethodObservationCounts(mode_s=12, vdl2=3),
        last_24h=db.MethodObservationCounts(mode_s=345, vdl2=67),
        last_received=db.MethodLastReceived(
            mode_s=datetime.datetime(2026, 7, 12, 10, 0, tzinfo=datetime.UTC),
            vdl2=None,
        ),
        aggregate_rows=db.AggregateRowCounts(
            halfhourly_altitude_grid=100,
            threehour_altitude_grid=50,
        ),
    )


class TestFetchObservationCountsByMethod:
    """fetch_observation_counts_by_method のテスト"""

    def test_returns_both_methods(self):
        mock_conn, _cursor = _mock_conn(
            [
                {"method": "mode-s", "count": 42},
                {"method": "vdl2", "count": 7},
            ]
        )

        result = db.fetch_observation_counts_by_method(mock_conn, hours=24)

        assert result.mode_s == 42
        assert result.vdl2 == 7

    def test_returns_zero_for_missing_method(self):
        mock_conn, _cursor = _mock_conn([{"method": "mode-s", "count": 5}])

        result = db.fetch_observation_counts_by_method(mock_conn, hours=1)

        assert result.mode_s == 5
        assert result.vdl2 == 0

    def test_empty_result(self):
        mock_conn, _cursor = _mock_conn([])

        result = db.fetch_observation_counts_by_method(mock_conn, hours=1)

        assert result.mode_s == 0
        assert result.vdl2 == 0

    def test_query_structure(self):
        mock_conn, mock_cursor = _mock_conn([])

        db.fetch_observation_counts_by_method(mock_conn, hours=24)

        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        assert "time >= %s" in query
        assert "GROUP BY method" in query
        assert "meteorological_data" in query


class TestFetchReceiverQuality:
    """fetch_receiver_quality のテスト"""

    def _patch_sources(self):
        quality = _sample_quality()
        stats = db.AllMaterializedViewStats(
            halfhourly_altitude_grid=db.MaterializedViewStats(row_count=100, earliest=None, latest=None),
            threehour_altitude_grid=db.MaterializedViewStats(row_count=50, earliest=None, latest=None),
        )
        return (
            unittest.mock.patch(
                "amdar.database.postgresql.fetch_observation_counts_by_method",
                side_effect=[quality.last_hour, quality.last_24h],
            ),
            unittest.mock.patch(
                "amdar.database.postgresql.fetch_last_received_by_method",
                return_value=quality.last_received,
            ),
            unittest.mock.patch(
                "amdar.database.postgresql.get_materialized_view_stats",
                return_value=stats,
            ),
        )

    def test_combines_all_sources(self):
        counts_patch, last_received_patch, stats_patch = self._patch_sources()
        with counts_patch, last_received_patch, stats_patch:
            result = db.fetch_receiver_quality(unittest.mock.MagicMock())

        assert result.last_hour.mode_s == 12
        assert result.last_24h.vdl2 == 67
        assert result.last_received.vdl2 is None
        assert result.aggregate_rows.halfhourly_altitude_grid == 100
        assert result.aggregate_rows.to_dict() == {
            "halfhourly_altitude_grid": 100,
            "threehour_altitude_grid": 50,
        }

    def test_result_is_cached(self):
        """TTL 内の 2 回目の呼び出しはクエリを実行しない"""
        counts_patch, last_received_patch, stats_patch = self._patch_sources()
        with counts_patch as counts_mock, last_received_patch, stats_patch:
            first = db.fetch_receiver_quality(unittest.mock.MagicMock())
            second = db.fetch_receiver_quality(unittest.mock.MagicMock())

        assert first is second
        assert counts_mock.call_count == 2  # 1回目の last_hour / last_24h のみ

    def test_cache_expires(self):
        """TTL 経過後は再クエリする"""
        counts_patch, last_received_patch, stats_patch = self._patch_sources()
        with counts_patch as counts_mock, last_received_patch, stats_patch:
            counts_mock.side_effect = [_sample_quality().last_hour, _sample_quality().last_24h] * 2
            db.fetch_receiver_quality(unittest.mock.MagicMock())

            # キャッシュ時刻を TTL 超過に偽装
            db._receiver_quality_cache_time = 0.0

            db.fetch_receiver_quality(unittest.mock.MagicMock())

        assert counts_mock.call_count == 4


@pytest.fixture
def client(config):
    """data_routes のみを登録した軽量 Flask アプリのテストクライアント"""
    app = flask.Flask("test-receiver-quality")
    app.config["CONFIG"] = config
    app.config["TESTING"] = True
    app.register_blueprint(amdar.viewer.api.data_routes.blueprint, url_prefix="/modes-sensing")
    return app.test_client()


class TestReceiverQualityEndpoint:
    """GET /api/receiver-quality のテスト"""

    def test_returns_quality_json(self, client):
        with unittest.mock.patch(
            "amdar.viewer.api.data_routes._fetch_receiver_quality",
            return_value=_sample_quality(),
        ):
            res = client.get("/modes-sensing/api/receiver-quality")

        assert res.status_code == 200
        body = res.get_json()

        assert body["mode_s"]["last_hour"] == 12
        assert body["mode_s"]["last_24h"] == 345
        assert body["mode_s"]["last_received"] is not None
        assert body["mode_s"]["age_seconds"] is not None
        assert body["mode_s"]["age_seconds"] >= 0

        assert body["vdl2"]["last_hour"] == 3
        assert body["vdl2"]["last_received"] is None
        assert body["vdl2"]["age_seconds"] is None

        assert body["aggregates"] == {
            "halfhourly_altitude_grid": 100,
            "threehour_altitude_grid": 50,
        }

    def test_db_error_returns_500(self, client):
        with unittest.mock.patch(
            "amdar.viewer.api.data_routes._fetch_receiver_quality",
            side_effect=RuntimeError("DB down"),
        ):
            res = client.get("/modes-sensing/api/receiver-quality")

        assert res.status_code == 500
        assert "error" in res.get_json()


class TestMetricsEndpoint:
    """GET /api/metrics のテスト"""

    def _get_metrics(self, client):
        with unittest.mock.patch(
            "amdar.viewer.api.data_routes._fetch_receiver_quality",
            return_value=_sample_quality(),
        ):
            return client.get("/modes-sensing/api/metrics")

    def test_content_type_is_prometheus(self, client):
        res = self._get_metrics(client)

        assert res.status_code == 200
        assert res.content_type.startswith("text/plain; version=0.0.4")

    def test_contains_expected_metrics(self, client):
        res = self._get_metrics(client)
        body = res.get_data(as_text=True)

        assert 'modes_sensing_observations_total{method="mode-s"} 345' in body
        assert 'modes_sensing_observations_total{method="vdl2"} 67' in body
        assert 'modes_sensing_observations_last_hour{method="mode-s"} 12' in body
        assert 'modes_sensing_observations_last_hour{method="vdl2"} 3' in body
        assert 'modes_sensing_last_observation_age_seconds{method="mode-s"}' in body
        # VDL2 は未受信のため age メトリクスは出力されない
        assert 'modes_sensing_last_observation_age_seconds{method="vdl2"}' not in body
        assert 'modes_sensing_aggregate_rows{table="halfhourly_altitude_grid"} 100' in body
        assert 'modes_sensing_aggregate_rows{table="threehour_altitude_grid"} 50' in body
        assert 'modes_sensing_jobs{status="pending"}' in body
        assert 'modes_sensing_jobs{status="completed"}' in body
        assert "modes_sensing_cache_files" in body

    def test_help_and_type_lines_present(self, client):
        res = self._get_metrics(client)
        body = res.get_data(as_text=True)

        assert "# HELP modes_sensing_observations_total" in body
        assert "# TYPE modes_sensing_observations_total gauge" in body
        assert "# TYPE modes_sensing_jobs gauge" in body

    def test_db_error_returns_500(self, client):
        with unittest.mock.patch(
            "amdar.viewer.api.data_routes._fetch_receiver_quality",
            side_effect=RuntimeError("DB down"),
        ):
            res = client.get("/modes-sensing/api/metrics")

        assert res.status_code == 500
        assert res.content_type.startswith("text/plain; version=0.0.4")
