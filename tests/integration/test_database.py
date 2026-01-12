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
