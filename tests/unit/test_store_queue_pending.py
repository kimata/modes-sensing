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
            # NOTE: multiprocessing.Queue.empty() は信頼性が低いため、
            # get_nowait() でデータを取り出して確認する
            remaining_data = measurement_queue.get_nowait()
            assert remaining_data.callsign == "TEST001"

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


class TestErrorHandlers:
    """エラーハンドラのテスト（poison message でワーカーが停止しないこと）"""

    def test_data_error_discards_pending(self, sample_measurement_data: MeasurementData):
        """データ起因の DB エラーでは当該レコードを破棄して継続する"""
        mock_conn = MagicMock()
        state = database_postgresql._StoreState(mock_conn)
        state.pending_data = sample_measurement_data

        database_postgresql._handle_data_error(state, psycopg2.DataError("invalid input"))

        # レコードが破棄され、ワーカーは停止しない
        assert state.pending_data is None
        assert state.should_stop is False
        assert state.consecutive_errors == 0
        # トランザクションのロールバックが試行される
        mock_conn.rollback.assert_called_once()

    def test_unexpected_error_discards_pending(self, sample_measurement_data: MeasurementData):
        """予期しないエラーでも同一レコードの無限再試行にならない"""
        mock_conn = MagicMock()
        state = database_postgresql._StoreState(mock_conn)
        state.pending_data = sample_measurement_data
        slack_config = MagicMock()

        with patch("my_lib.notify.slack.error"):
            database_postgresql._handle_unexpected_error(state, slack_config)

        # レコードは破棄されるが、1回目ではワーカーは停止しない
        assert state.pending_data is None
        assert state.should_stop is False
        assert state.consecutive_errors == 1

    def test_unexpected_error_stops_after_max_consecutive(self, sample_measurement_data: MeasurementData):
        """連続エラー上限で従来どおり停止する（システム起因エラーの安全弁）"""
        mock_conn = MagicMock()
        state = database_postgresql._StoreState(mock_conn)
        slack_config = MagicMock()

        with patch("my_lib.notify.slack.error") as mock_slack:
            for _ in range(3):
                state.pending_data = sample_measurement_data
                database_postgresql._handle_unexpected_error(state, slack_config)

        assert state.should_stop is True
        mock_slack.assert_called_once()

    def test_store_queue_continues_after_data_error(
        self, sample_measurement_data: MeasurementData, temp_liveness_file: pathlib.Path
    ):
        """poison message の次のレコードが正常に処理されることを確認"""
        database_postgresql._should_terminate.clear()
        mock_conn = MagicMock()

        poison_data = MeasurementData(
            callsign="POISON",
            altitude=10000.0,
            latitude=35.0,
            longitude=136.0,
            temperature=-40.0,
            wind=WindData(x=10.0, y=5.0, angle=270.0, speed=11.18),
            distance=50.0,
            method="mode-s",
        )

        measurement_queue: multiprocessing.Queue[MeasurementData] = multiprocessing.Queue()
        measurement_queue.put(poison_data)
        measurement_queue.put(sample_measurement_data)

        db_config = database_postgresql.DatabaseConfig(
            host="localhost",
            port=5432,
            name="test",
            user="test",
            password="test",  # noqa: S106
        )

        # 1件目（POISON）は DataError、2件目は成功
        insert_calls: list[str] = []

        def _insert_side_effect(_conn, data: MeasurementData) -> None:
            insert_calls.append(data.callsign)
            if data.callsign == "POISON":
                raise psycopg2.DataError("value out of range")

        with (
            patch.object(database_postgresql, "_insert", side_effect=_insert_side_effect),
            patch("my_lib.notify.slack.error"),
        ):
            database_postgresql.store_queue(
                mock_conn, measurement_queue, temp_liveness_file, db_config, MagicMock(), count=1
            )

        # POISON は破棄され、後続レコードが処理されて count=1 で正常終了する
        assert insert_calls == ["POISON", "TEST001"]
