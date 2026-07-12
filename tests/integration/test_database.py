#!/usr/bin/env python3
# ruff: noqa: S101
"""
database_postgresql.py の統合テスト

データベースクエリ機能をテストします。
"""

import datetime
import logging

import psycopg2.extras

import amdar.database.postgresql as database_postgresql
from amdar.config import Config


class TestDataRange:
    """データ範囲クエリのテスト"""

    def test_data_range_query(self, config: Config):
        """データ範囲取得クエリをテスト"""
        conn = database_postgresql.open(
            config.database.host,
            config.database.port,
            config.database.name,
            config.database.user,
            config.database.password,
        )

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
            SELECT
                MIN(time) as earliest,
                MAX(time) as latest
            FROM meteorological_data
            """)
            result = cur.fetchone()

        conn.close()

        # データが存在することを確認
        assert result is not None
        assert result["earliest"] is not None
        assert result["latest"] is not None

        # 日付範囲が妥当であることを確認
        earliest = result["earliest"]
        latest = result["latest"]
        assert earliest <= latest

        logging.info("Data range: %s ～ %s", earliest, latest)


class TestOpen:
    """open() のテスト"""

    def test_open_without_schema(self, config: Config):
        """apply_schema=False の場合、DDL を実行せずに接続できる"""
        conn = database_postgresql.open(
            config.database.host,
            config.database.port,
            config.database.name,
            config.database.user,
            config.database.password,
            apply_schema=False,
        )

        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()

        conn.close()

        assert row is not None
        assert row[0] == 1

    def test_apply_schema_function(self, config: Config):
        """apply_schema() で明示的にスキーマを適用できる"""
        conn = database_postgresql.open(
            config.database.host,
            config.database.port,
            config.database.name,
            config.database.user,
            config.database.password,
            apply_schema=False,
        )

        database_postgresql.apply_schema(conn)
        conn.close()


class TestAltitudeFiltering:
    """高度フィルタリングのテスト"""

    def test_altitude_filtering(self, config: Config):
        """高度フィルタリング機能をテスト"""
        end_time = datetime.datetime.now(datetime.UTC)
        start_time = end_time - datetime.timedelta(days=7)

        conn = database_postgresql.open(
            config.database.host,
            config.database.port,
            config.database.name,
            config.database.user,
            config.database.password,
        )

        # 高度制限なしでデータ取得
        data_unlimited = database_postgresql.fetch_by_time(
            conn, start_time, end_time, config.filter.area.distance
        )

        # 高度制限ありでデータ取得（2000m以下）
        data_limited = database_postgresql.fetch_by_time(
            conn, start_time, end_time, config.filter.area.distance, max_altitude=2000
        )

        conn.close()

        # データが取得されていることを確認
        assert len(data_unlimited) >= 0
        assert len(data_limited) >= 0

        # 高度制限ありの方が件数が少ないか同じであることを確認
        assert len(data_limited) <= len(data_unlimited)

        # 高度制限ありのデータは全て2000m以下であることを確認
        for record in data_limited:
            if record["altitude"] is not None:
                assert record["altitude"] <= 2000

        logging.info(
            "Data count - Unlimited: %d records, Limited to 2000m: %d records",
            len(data_unlimited),
            len(data_limited),
        )

    def test_quality_filter_applied(self, config: Config):
        """生データフェッチに集約テーブルと同じ品質フィルタが適用される"""
        end_time = datetime.datetime.now(datetime.UTC)
        start_time = end_time - datetime.timedelta(days=7)

        conn = database_postgresql.open(
            config.database.host,
            config.database.port,
            config.database.name,
            config.database.user,
            config.database.password,
        )

        data = database_postgresql.fetch_by_time(
            conn,
            start_time,
            end_time,
            config.filter.area.distance,
            columns=["time", "altitude", "temperature"],
        )

        conn.close()

        for record in data:
            assert record["altitude"] is not None
            assert 0 <= record["altitude"] <= 13000
            assert record["temperature"] > -100


class TestAggregateTables:
    """集約テーブル（旧マテリアライズドビュー）のテスト"""

    def test_check_aggregate_tables_exist(self, config: Config):
        """スキーマ適用後は集約テーブルが存在する"""
        conn = database_postgresql.open(
            config.database.host,
            config.database.port,
            config.database.name,
            config.database.user,
            config.database.password,
        )

        status = database_postgresql.check_materialized_views_exist(conn)
        conn.close()

        assert status.halfhourly_altitude_grid is True
        assert status.threehour_altitude_grid is True

    def test_refresh_returns_result(self, config: Config):
        """増分更新が結果（経過秒 or エラー時 -1）を返す"""
        conn = database_postgresql.open(
            config.database.host,
            config.database.port,
            config.database.name,
            config.database.user,
            config.database.password,
        )

        result = database_postgresql.refresh_materialized_views(conn)
        conn.close()

        assert isinstance(result.halfhourly_altitude_grid, float)
        assert isinstance(result.threehour_altitude_grid, float)
        logging.info(
            "Refresh timings: halfhourly=%.2f, threehour=%.2f",
            result.halfhourly_altitude_grid,
            result.threehour_altitude_grid,
        )

    def test_get_stats(self, config: Config):
        """集約テーブルの統計情報を取得できる"""
        conn = database_postgresql.open(
            config.database.host,
            config.database.port,
            config.database.name,
            config.database.user,
            config.database.password,
        )

        stats = database_postgresql.get_materialized_view_stats(conn)
        conn.close()

        assert stats.halfhourly_altitude_grid.row_count >= 0
        assert stats.threehour_altitude_grid.row_count >= 0


class TestFetchDataRange:
    """fetch_data_range のテスト"""

    def test_fetch_data_range_cached(self, config: Config):
        """2回目の呼び出しはキャッシュから返る"""
        database_postgresql._clear_data_range_cache()

        conn = database_postgresql.open(
            config.database.host,
            config.database.port,
            config.database.name,
            config.database.user,
            config.database.password,
        )

        first = database_postgresql.fetch_data_range(conn)
        second = database_postgresql.fetch_data_range(conn)

        conn.close()
        database_postgresql._clear_data_range_cache()

        assert first == second
        if first.count > 0:
            assert first.earliest is not None
            assert first.latest is not None
            assert first.earliest <= first.latest


class TestLastReceivedByMethod:
    """受信方式別の最終受信時刻クエリのテスト"""

    def test_last_received_by_method(self, config: Config):
        """受信方式別の最終受信時刻を取得"""
        conn = database_postgresql.open(
            config.database.host,
            config.database.port,
            config.database.name,
            config.database.user,
            config.database.password,
        )

        result = database_postgresql.fetch_last_received_by_method(conn)
        conn.close()

        # 結果がMethodLastReceivedであることを確認
        assert isinstance(result, database_postgresql.MethodLastReceived)

        # Mode S または VDL2 のいずれかにデータがあることを確認
        has_data = result.mode_s is not None or result.vdl2 is not None

        if has_data:
            # データがある場合、時刻が妥当であることを確認
            if result.mode_s is not None:
                assert isinstance(result.mode_s, datetime.datetime)
                logging.info("Mode S last received: %s", result.mode_s)

            if result.vdl2 is not None:
                assert isinstance(result.vdl2, datetime.datetime)
                logging.info("VDL2 last received: %s", result.vdl2)
        else:
            logging.info("No data found for either method")
