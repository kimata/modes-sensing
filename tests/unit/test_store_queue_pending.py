#!/usr/bin/env python3
# ruff: noqa: S101
"""
store_queue のペンディングデータ保持機能のテスト

DBエラー発生時にデータが失われないことを確認します。
"""

import multiprocessing
import pathlib
import queue
import tempfile
from unittest.mock import MagicMock, patch

import psycopg2
import pytest

import amdar.database.postgresql as database_postgresql
from amdar.core.types import WindData
from amdar.database.postgresql import MeasurementData


@pytest.fixture
def sample_measurement_data() -> MeasurementData:
    """テスト用の測定データ"""
    return MeasurementData(
        callsign="TEST001",
        altitude=10000.0,
        latitude=35.0,
        longitude=136.0,
        temperature=-40.0,
        wind=WindData(x=10.0, y=5.0, angle=270.0, speed=11.18),
        distance=50.0,
        method="mode-s",
    )


@pytest.fixture
def temp_liveness_file() -> pathlib.Path:
    """一時的な liveness ファイル"""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        return pathlib.Path(f.name)


class TestStoreState:
    """_StoreState クラスのテスト"""

    def test_initial_pending_data_is_none(self):
        """初期状態では pending_data が None であることを確認"""
        mock_conn = MagicMock()
        state = database_postgresql._StoreState(mock_conn)

        assert state.pending_data is None

    def test_pending_data_can_be_set(self, sample_measurement_data: MeasurementData):
        """pending_data に値を設定できることを確認"""
        mock_conn = MagicMock()
        state = database_postgresql._StoreState(mock_conn)

        state.pending_data = sample_measurement_data

        assert state.pending_data is not None
        assert state.pending_data.callsign == "TEST001"


class TestProcessOneItem:
    """_process_one_item 関数のテスト"""

    def test_normal_processing_without_pending(
        self, sample_measurement_data: MeasurementData, temp_liveness_file: pathlib.Path
    ):
        """ペンディングなしの通常処理"""
        mock_conn = MagicMock()
        state = database_postgresql._StoreState(mock_conn)

        # キューにデータを追加
        measurement_queue: multiprocessing.Queue[MeasurementData] = multiprocessing.Queue()
        measurement_queue.put(sample_measurement_data)

        with patch.object(database_postgresql, "_insert") as mock_insert:
            database_postgresql._process_one_item(state, measurement_queue, temp_liveness_file)

            # INSERT が呼ばれたことを確認
            mock_insert.assert_called_once()
            # ペンディングがクリアされていることを確認
            assert state.pending_data is None
            # 処理カウントが増加していることを確認
            assert state.processed_count == 1

    def test_data_preserved_on_db_error(
        self, sample_measurement_data: MeasurementData, temp_liveness_file: pathlib.Path
    ):
        """DBエラー発生時にデータがペンディングに保持されることを確認"""
        mock_conn = MagicMock()
        state = database_postgresql._StoreState(mock_conn)

        # キューにデータを追加
        measurement_queue: multiprocessing.Queue[MeasurementData] = multiprocessing.Queue()
        measurement_queue.put(sample_measurement_data)

        with patch.object(
            database_postgresql, "_insert", side_effect=psycopg2.OperationalError("connection lost")
        ):
            with pytest.raises(psycopg2.OperationalError):
                database_postgresql._process_one_item(state, measurement_queue, temp_liveness_file)

            # ペンディングにデータが保持されていることを確認
            assert state.pending_data is not None
            assert state.pending_data.callsign == "TEST001"
            # 処理カウントは増加していないことを確認
            assert state.processed_count == 0

    def test_pending_data_processed_first(
        self, sample_measurement_data: MeasurementData, temp_liveness_file: pathlib.Path
    ):
        """ペンディングデータが優先的に処理されることを確認"""
        mock_conn = MagicMock()
        state = database_postgresql._StoreState(mock_conn)

        # ペンディングデータを設定
        pending_data = MeasurementData(
            callsign="PENDING",
            altitude=5000.0,
            latitude=35.0,
            longitude=136.0,
            temperature=-20.0,
            wind=WindData(x=5.0, y=2.0, angle=270.0, speed=5.38),
            distance=30.0,
            method="mode-s",
        )
        state.pending_data = pending_data

        # キューにも別のデータを追加
        measurement_queue: multiprocessing.Queue[MeasurementData] = multiprocessing.Queue()
        measurement_queue.put(sample_measurement_data)

        with patch.object(database_postgresql, "_insert") as mock_insert:
            database_postgresql._process_one_item(state, measurement_queue, temp_liveness_file)

            # ペンディングデータが処理されたことを確認
            mock_insert.assert_called_once()
            call_args = mock_insert.call_args[0]
            assert call_args[1].callsign == "PENDING"

            # ペンディングがクリアされていることを確認
            assert state.pending_data is None

            # キューにはまだデータが残っていることを確認
            assert not measurement_queue.empty()

    def test_retry_after_reconnect(
        self, sample_measurement_data: MeasurementData, temp_liveness_file: pathlib.Path
    ):
        """再接続後にペンディングデータがリトライされることを確認"""
        mock_conn = MagicMock()
        state = database_postgresql._StoreState(mock_conn)

        measurement_queue: multiprocessing.Queue[MeasurementData] = multiprocessing.Queue()
        measurement_queue.put(sample_measurement_data)

        # 1回目: エラー発生
        with (
            patch.object(
                database_postgresql, "_insert", side_effect=psycopg2.OperationalError("connection lost")
            ),
            pytest.raises(psycopg2.OperationalError),
        ):
            database_postgresql._process_one_item(state, measurement_queue, temp_liveness_file)

        # ペンディングにデータが保持されていることを確認
        assert state.pending_data is not None
        assert state.pending_data.callsign == "TEST001"

        # キューは空（データは取り出されたがペンディングに保持）
        # ただし、エラー前にキューから取り出しているので空になる
        # → 修正後: ペンディングがあればキューから取り出さないので、
        #   最初のエラーでキューから取り出したデータがペンディングに保持される

        # 2回目: 成功
        with patch.object(database_postgresql, "_insert") as mock_insert:
            database_postgresql._process_one_item(state, measurement_queue, temp_liveness_file)

            # ペンディングデータが処理されたことを確認
            mock_insert.assert_called_once()
            call_args = mock_insert.call_args[0]
            assert call_args[1].callsign == "TEST001"

            # ペンディングがクリアされていることを確認
            assert state.pending_data is None
            assert state.processed_count == 1

    def test_multiple_consecutive_errors_preserve_data(
        self, sample_measurement_data: MeasurementData, temp_liveness_file: pathlib.Path
    ):
        """連続エラーでもデータが失われないことを確認"""
        mock_conn = MagicMock()
        state = database_postgresql._StoreState(mock_conn)

        measurement_queue: multiprocessing.Queue[MeasurementData] = multiprocessing.Queue()
        measurement_queue.put(sample_measurement_data)

        # 3回連続でエラー
        for i in range(3):
            with (
                patch.object(
                    database_postgresql,
                    "_insert",
                    side_effect=psycopg2.OperationalError(f"error {i + 1}"),
                ),
                pytest.raises(psycopg2.OperationalError),
            ):
                database_postgresql._process_one_item(state, measurement_queue, temp_liveness_file)

            # 毎回ペンディングにデータが保持されていることを確認
            assert state.pending_data is not None
            assert state.pending_data.callsign == "TEST001"

        # 4回目: 成功
        with patch.object(database_postgresql, "_insert") as mock_insert:
            database_postgresql._process_one_item(state, measurement_queue, temp_liveness_file)

            # データが正常に処理されたことを確認
            mock_insert.assert_called_once()
            assert state.pending_data is None
            assert state.processed_count == 1

    def test_queue_empty_raises_when_no_pending(self, temp_liveness_file: pathlib.Path):
        """ペンディングなしでキューが空の場合は queue.Empty が発生"""
        mock_conn = MagicMock()
        state = database_postgresql._StoreState(mock_conn)

        # 空のキュー
        measurement_queue: multiprocessing.Queue[MeasurementData] = multiprocessing.Queue()

        with pytest.raises(queue.Empty):
            database_postgresql._process_one_item(state, measurement_queue, temp_liveness_file)
