#!/usr/bin/env python3
# ruff: noqa: S101
"""
database/postgresql.py のユニットテスト

TIMESTAMPTZ 移行、増分集約テーブル、フェッチ系ヘルパー、
データ範囲キャッシュ、定期更新スケジューラの動作を確認します。
"""

import datetime
import unittest.mock
import zoneinfo

import numpy as np
import psycopg2
import pytest

import amdar.constants
import amdar.database.postgresql as db

JST = zoneinfo.ZoneInfo("Asia/Tokyo")


@pytest.fixture
def mock_conn():
    """cursor コンテキストマネージャ付きの接続モック"""
    conn = unittest.mock.MagicMock()
    cursor = unittest.mock.MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn, cursor


class TestToNaiveDatetime:
    """_to_naive_datetime（DEPRECATED シム）のテスト"""

    def test_returns_aware_datetime_unchanged(self):
        """aware datetime をそのまま返す（naive 変換しない）"""
        dt = datetime.datetime(2026, 7, 1, 12, 0, tzinfo=JST)

        result = db._to_naive_datetime(dt)

        assert result is dt
        assert result.tzinfo is not None


class TestConvertRowsToNumpyArrays:
    """_convert_rows_to_numpy_arrays のタイムゾーン処理のテスト"""

    def test_aware_time_stored_as_jst_wall_time(self):
        """aware datetime は JST の壁時計時刻として datetime64 に格納される"""
        dt_utc = datetime.datetime(2026, 7, 1, 3, 0, tzinfo=datetime.UTC)  # JST 12:00
        rows = [(dt_utc, 1000.0, -10.0)]

        result = db._convert_rows_to_numpy_arrays(rows)

        assert result.time[0] == np.datetime64("2026-07-01T12:00:00")

    def test_naive_time_stored_unchanged(self):
        """naive datetime はそのまま格納される（移行前データとの互換）"""
        dt = datetime.datetime(2026, 7, 1, 12, 0)
        rows = [(dt, 1000.0, -10.0)]

        result = db._convert_rows_to_numpy_arrays(rows)

        assert result.time[0] == np.datetime64("2026-07-01T12:00:00")


class TestBuildRawDataFilter:
    """_build_raw_data_filter のテスト"""

    def test_applies_quality_filter(self):
        """集約テーブルと同じ品質フィルタが適用される"""
        start = datetime.datetime(2026, 7, 1, tzinfo=JST)
        end = datetime.datetime(2026, 7, 2, tzinfo=JST)

        where, params = db._build_raw_data_filter(start, end, 100.0, None)

        assert "temperature > %s" in where
        assert "altitude >= %s" in where
        assert "altitude <= %s" in where
        assert params == [
            start,
            end,
            100.0,
            amdar.constants.GRAPH_ALT_MIN,
            amdar.constants.GRAPH_ALT_MAX,
            amdar.constants.GRAPH_TEMPERATURE_THRESHOLD,
        ]

    def test_max_altitude_is_capped_by_quality_limit(self):
        """max_altitude は品質フィルタの上限（13000m）でキャップされる"""
        start = datetime.datetime(2026, 7, 1, tzinfo=JST)
        end = datetime.datetime(2026, 7, 2, tzinfo=JST)

        _, params_low = db._build_raw_data_filter(start, end, 100.0, 2000)
        _, params_high = db._build_raw_data_filter(start, end, 100.0, 99999)

        assert params_low[4] == 2000
        assert params_high[4] == amdar.constants.GRAPH_ALT_MAX

    def test_aware_datetime_passed_through(self):
        """aware datetime がそのままパラメータに渡される（naive 変換しない）"""
        start = datetime.datetime(2026, 7, 1, tzinfo=JST)
        end = datetime.datetime(2026, 7, 2, tzinfo=JST)

        _, params = db._build_raw_data_filter(start, end, 100.0, None)

        assert params[0].tzinfo is not None
        assert params[1].tzinfo is not None


class TestBuildAggregateFilter:
    """_build_aggregate_filter のテスト"""

    def test_without_max_altitude(self):
        start = datetime.datetime(2026, 7, 1, tzinfo=JST)
        end = datetime.datetime(2026, 7, 2, tzinfo=JST)

        where, params = db._build_aggregate_filter(start, end, None)

        assert where == "time_bucket >= %s AND time_bucket <= %s"
        assert params == [start, end]

    def test_with_max_altitude(self):
        start = datetime.datetime(2026, 7, 1, tzinfo=JST)
        end = datetime.datetime(2026, 7, 2, tzinfo=JST)

        where, params = db._build_aggregate_filter(start, end, 2000)

        assert "altitude <= %s" in where
        assert params == [start, end, 2000]


class TestAlignToBucketStart:
    """_align_to_bucket_start のテスト"""

    def test_halfhourly_alignment(self):
        """30分バケットへの切り下げ"""
        dt = datetime.datetime(2026, 7, 1, 12, 47, 30, tzinfo=JST)

        aligned = db._align_to_bucket_start(dt, amdar.constants.AGGREGATE_HALFHOURLY_BUCKET_SECONDS)

        assert aligned == datetime.datetime(2026, 7, 1, 12, 30, tzinfo=JST)

    def test_threehour_alignment(self):
        """3時間バケットへの切り下げ（JST 基準）"""
        dt = datetime.datetime(2026, 7, 1, 14, 10, tzinfo=JST)

        aligned = db._align_to_bucket_start(dt, amdar.constants.AGGREGATE_THREEHOUR_BUCKET_SECONDS)

        assert aligned == datetime.datetime(2026, 7, 1, 12, 0, tzinfo=JST)

    def test_utc_input_aligned_in_jst(self):
        """UTC 入力でも JST 基準のバケット境界に切り下げられる"""
        # UTC 03:10 = JST 12:10 → JST 12:00 のバケット
        dt = datetime.datetime(2026, 7, 1, 3, 10, tzinfo=datetime.UTC)

        aligned = db._align_to_bucket_start(dt, amdar.constants.AGGREGATE_THREEHOUR_BUCKET_SECONDS)

        assert aligned == datetime.datetime(2026, 7, 1, 12, 0, tzinfo=JST)

    def test_already_aligned(self):
        """既にバケット境界の場合はそのまま"""
        dt = datetime.datetime(2026, 7, 1, 12, 0, tzinfo=JST)

        aligned = db._align_to_bucket_start(dt, amdar.constants.AGGREGATE_HALFHOURLY_BUCKET_SECONDS)

        assert aligned == dt


class TestAggregateInsertSql:
    """_build_aggregate_insert_sql のテスト"""

    @pytest.mark.parametrize("spec", db._AGGREGATE_TABLE_SPECS, ids=lambda s: s.table)
    def test_contains_quality_filter_placeholders(self, spec):
        """品質フィルタがプレースホルダで埋め込まれている"""
        sql = db._build_aggregate_insert_sql(spec, with_time_filter=False)

        assert f"INSERT INTO {spec.table}" in sql
        assert "distance <= %s" in sql
        assert "temperature > %s" in sql
        assert "DISTINCT ON (time_bucket, altitude_bin)" in sql
        assert "AND time >= %s" not in sql

    @pytest.mark.parametrize("spec", db._AGGREGATE_TABLE_SPECS, ids=lambda s: s.table)
    def test_time_filter_appended(self, spec):
        """増分更新用の time フィルタが付与される"""
        sql = db._build_aggregate_insert_sql(spec, with_time_filter=True)

        assert "AND time >= %s" in sql

    def test_bucket_expr_uses_jst(self):
        """バケット計算が JST 基準になっている"""
        for spec in db._AGGREGATE_TABLE_SPECS:
            assert f"AT TIME ZONE '{amdar.constants.AGGREGATE_BUCKET_TIMEZONE}'" in spec.bucket_expr

    def test_aggregate_tables_constant(self):
        """AGGREGATE_TABLES 定数がテーブル定義と一致する"""
        assert db.AGGREGATE_TABLES == ("halfhourly_altitude_grid", "threehour_altitude_grid")


class TestRefreshMaterializedViews:
    """refresh_materialized_views（増分更新）のテスト"""

    def test_refresh_executes_delete_and_insert_in_transaction(self, mock_conn):
        """DELETE と INSERT が1トランザクションで実行される"""
        conn, cursor = mock_conn
        cursor.rowcount = 10

        result = db.refresh_materialized_views(conn)

        executed = [call.args[0] for call in cursor.execute.call_args_list]
        assert executed.count("BEGIN") == 2
        assert executed.count("COMMIT") == 2
        assert any("DELETE FROM halfhourly_altitude_grid" in sql for sql in executed)
        assert any("DELETE FROM threehour_altitude_grid" in sql for sql in executed)
        assert any("INSERT INTO halfhourly_altitude_grid" in sql for sql in executed)
        assert any("INSERT INTO threehour_altitude_grid" in sql for sql in executed)
        assert result.halfhourly_altitude_grid >= 0
        assert result.threehour_altitude_grid >= 0

    def test_failure_of_one_table_does_not_block_other(self, mock_conn):
        """片方の失敗がもう片方の更新を妨げない（エラー時 -1）"""
        conn, cursor = mock_conn
        cursor.rowcount = 10

        def _execute_side_effect(sql, *args):
            if "DELETE FROM halfhourly_altitude_grid" in sql:
                raise psycopg2.OperationalError("connection lost")

        cursor.execute.side_effect = _execute_side_effect

        result = db.refresh_materialized_views(conn)

        assert result.halfhourly_altitude_grid == -1
        assert result.threehour_altitude_grid >= 0

    def test_rebuild_uses_truncate_and_full_insert(self, mock_conn):
        """全量再構築は TRUNCATE + 全期間 INSERT"""
        conn, cursor = mock_conn
        cursor.rowcount = 10

        result = db.rebuild_aggregate_tables(conn)

        executed = [call.args[0] for call in cursor.execute.call_args_list]
        assert any("TRUNCATE halfhourly_altitude_grid" in sql for sql in executed)
        assert any("TRUNCATE threehour_altitude_grid" in sql for sql in executed)
        # 全量投入なので time フィルタなし
        insert_sqls = [sql for sql in executed if "INSERT INTO" in sql]
        assert all("AND time >= %s" not in sql for sql in insert_sqls)
        assert result.halfhourly_altitude_grid >= 0
        assert result.threehour_altitude_grid >= 0


class TestCheckMaterializedViewsExist:
    """check_materialized_views_exist のテスト"""

    def test_single_query_with_to_regclass(self, mock_conn):
        """to_regclass による1回のクエリで存在確認する"""
        conn, cursor = mock_conn
        cursor.fetchone.return_value = (True, False)

        result = db.check_materialized_views_exist(conn)

        cursor.execute.assert_called_once()
        assert "to_regclass" in cursor.execute.call_args[0][0]
        assert result.halfhourly_altitude_grid is True
        assert result.threehour_altitude_grid is False
        assert result.get("halfhourly_altitude_grid") is True
        assert result.get("unknown_table") is False

    def test_no_row_returns_all_false(self, mock_conn):
        conn, cursor = mock_conn
        cursor.fetchone.return_value = None

        result = db.check_materialized_views_exist(conn)

        assert result.halfhourly_altitude_grid is False
        assert result.threehour_altitude_grid is False


class TestFetchDataRangeCache:
    """fetch_data_range の TTL キャッシュのテスト"""

    @pytest.fixture(autouse=True)
    def _clear_cache(self):
        db._clear_data_range_cache()
        yield
        db._clear_data_range_cache()

    def _make_conn(self, earliest, latest, count):
        conn = unittest.mock.MagicMock()
        cursor = unittest.mock.MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.return_value = {"earliest": earliest, "latest": latest, "count": count}
        return conn, cursor

    def test_second_call_uses_cache(self):
        """TTL 内の2回目の呼び出しは DB にアクセスしない"""
        earliest = datetime.datetime(2026, 1, 1, tzinfo=JST)
        latest = datetime.datetime(2026, 7, 1, tzinfo=JST)
        conn, cursor = self._make_conn(earliest, latest, 12345)

        first = db.fetch_data_range(conn)
        second = db.fetch_data_range(conn)

        assert cursor.execute.call_count == 1
        assert first == second
        assert first.count == 12345

    def test_cache_expires_after_ttl(self):
        """TTL 経過後は再度クエリを実行する"""
        earliest = datetime.datetime(2026, 1, 1, tzinfo=JST)
        latest = datetime.datetime(2026, 7, 1, tzinfo=JST)
        conn, cursor = self._make_conn(earliest, latest, 12345)

        db.fetch_data_range(conn)

        # キャッシュ時刻を TTL より過去にずらす
        db._data_range_cache_time -= amdar.constants.DATA_RANGE_CACHE_TTL_SECONDS + 1

        db.fetch_data_range(conn)

        assert cursor.execute.call_count == 2

    def test_empty_result(self):
        """データがない場合は count=0"""
        conn, _ = self._make_conn(None, None, 0)

        result = db.fetch_data_range(conn)

        assert result == db.DataRangeResult(earliest=None, latest=None, count=0)


class TestFetchRowLimit:
    """生データフェッチの行数上限のテスト"""

    def test_fetch_by_time_numpy_applies_limit_and_warns(self, mock_conn, monkeypatch, caplog):
        """LIMIT が適用され、上限到達時に警告が出る"""
        monkeypatch.setattr(amdar.constants, "RAW_FETCH_ROW_LIMIT", 2)
        conn, cursor = mock_conn
        now = datetime.datetime(2026, 7, 1, 12, 0, tzinfo=JST)
        cursor.fetchall.return_value = [(now, 1000.0, -10.0), (now, 2000.0, -20.0)]

        with caplog.at_level("WARNING"):
            result = db.fetch_by_time_numpy(
                conn,
                now - datetime.timedelta(days=1),
                now,
                distance=100.0,
            )

        query, params = cursor.execute.call_args[0]
        assert "LIMIT %s" in query
        assert params[-1] == 2
        assert result.count == 2
        assert any("上限" in record.message for record in caplog.records)

    def test_fetch_by_time_numpy_no_warning_below_limit(self, mock_conn, caplog):
        """上限未満なら警告なし"""
        conn, cursor = mock_conn
        now = datetime.datetime(2026, 7, 1, 12, 0, tzinfo=JST)
        cursor.fetchall.return_value = [(now, 1000.0, -10.0)]

        with caplog.at_level("WARNING"):
            db.fetch_by_time_numpy(conn, now - datetime.timedelta(days=1), now, distance=100.0)

        assert not any("上限" in record.message for record in caplog.records)


class TestMaterializedViewRefresher:
    """MaterializedViewRefresher（定期更新スケジューラ）のテスト"""

    @pytest.fixture
    def refresher(self):
        import amdar.viewer.api.materialized_view_refresh as mvr

        instance = mvr.materialized_view_refresher
        instance.stop()
        yield instance
        instance.stop()

    def test_stop_prevents_reschedule_from_run_refresh(self, refresher):
        """stop() 後は _run_refresh の finally で再スケジュールされない"""
        config = unittest.mock.MagicMock()
        refresher.initialize(config)
        refresher.stop()

        with (
            unittest.mock.patch("amdar.database.postgresql.open") as mock_open,
            unittest.mock.patch("amdar.database.postgresql.refresh_materialized_views") as mock_refresh,
        ):
            mock_refresh.return_value = db.MaterializedViewRefreshResult(
                halfhourly_altitude_grid=0.1, threehour_altitude_grid=0.1
            )
            # stop() 後に実行中だった更新が完了したケースを模擬
            refresher._run_refresh()

        assert mock_open.called
        assert refresher._timer is None

    def test_reinitialize_after_stop(self, refresher):
        """stop() 後に再度 initialize() できる"""
        config = unittest.mock.MagicMock()

        refresher.initialize(config)
        assert refresher._timer is not None

        refresher.stop()
        assert refresher._timer is None

        refresher.initialize(config)
        assert refresher._timer is not None

    def test_initialize_is_idempotent(self, refresher):
        """初期化済みの場合は再スケジュールしない"""
        config = unittest.mock.MagicMock()

        refresher.initialize(config)
        timer = refresher._timer

        refresher.initialize(config)

        assert refresher._timer is timer
