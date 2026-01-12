#!/usr/bin/env python3
# ruff: noqa: S101
"""
fetch_last_received_by_method のテスト
"""

import datetime
import unittest.mock

import amdar.database.postgresql as db


class TestMethodLastReceived:
    """MethodLastReceived dataclass のテスト"""

    def test_creation(self):
        """dataclass の作成"""
        now = datetime.datetime.now()
        result = db.MethodLastReceived(mode_s=now, vdl2=None)

        assert result.mode_s == now
        assert result.vdl2 is None

    def test_both_methods(self):
        """両方のメソッドに値がある場合"""
        mode_s_time = datetime.datetime(2026, 1, 12, 15, 30)
        vdl2_time = datetime.datetime(2026, 1, 12, 12, 45)

        result = db.MethodLastReceived(mode_s=mode_s_time, vdl2=vdl2_time)

        assert result.mode_s == mode_s_time
        assert result.vdl2 == vdl2_time


class TestFetchLastReceivedByMethod:
    """fetch_last_received_by_method のテスト"""

    def test_returns_both_methods(self):
        """Mode S と VDL2 両方の時刻を返す"""
        mock_conn = unittest.mock.MagicMock()
        mock_cursor = unittest.mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # モックデータ
        mock_cursor.fetchall.return_value = [
            {"method": "mode-s", "last_received": datetime.datetime(2026, 1, 12, 15, 30)},
            {"method": "vdl2", "last_received": datetime.datetime(2026, 1, 12, 12, 45)},
        ]

        result = db.fetch_last_received_by_method(mock_conn)

        assert result.mode_s == datetime.datetime(2026, 1, 12, 15, 30)
        assert result.vdl2 == datetime.datetime(2026, 1, 12, 12, 45)

    def test_returns_only_mode_s(self):
        """Mode S のみデータがある場合"""
        mock_conn = unittest.mock.MagicMock()
        mock_cursor = unittest.mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            {"method": "mode-s", "last_received": datetime.datetime(2026, 1, 12, 15, 30)},
        ]

        result = db.fetch_last_received_by_method(mock_conn)

        assert result.mode_s == datetime.datetime(2026, 1, 12, 15, 30)
        assert result.vdl2 is None

    def test_returns_only_vdl2(self):
        """VDL2 のみデータがある場合"""
        mock_conn = unittest.mock.MagicMock()
        mock_cursor = unittest.mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            {"method": "vdl2", "last_received": datetime.datetime(2026, 1, 12, 12, 45)},
        ]

        result = db.fetch_last_received_by_method(mock_conn)

        assert result.mode_s is None
        assert result.vdl2 == datetime.datetime(2026, 1, 12, 12, 45)

    def test_returns_none_for_empty_result(self):
        """データがない場合は両方 None"""
        mock_conn = unittest.mock.MagicMock()
        mock_cursor = unittest.mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        result = db.fetch_last_received_by_method(mock_conn)

        assert result.mode_s is None
        assert result.vdl2 is None

    def test_query_structure(self):
        """正しいクエリが実行されることを確認"""
        mock_conn = unittest.mock.MagicMock()
        mock_cursor = unittest.mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        db.fetch_last_received_by_method(mock_conn)

        # execute が呼ばれたことを確認
        mock_cursor.execute.assert_called_once()

        # クエリに必要な要素が含まれていることを確認
        query = mock_cursor.execute.call_args[0][0]
        assert "method" in query
        assert "MAX(time)" in query
        assert "meteorological_data" in query
        assert "GROUP BY" in query
