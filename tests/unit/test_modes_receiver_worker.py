#!/usr/bin/env python3
# ruff: noqa: S101
"""Mode S 受信ワーカーの再接続ポリシーのテスト

- 無音（接続は成功するがデータが来ない）は失敗として数えない
- 接続エラー / データ 0 行での切断のみ失敗として数え、上限でプロセス停止を要求する
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

import amdar.constants
import amdar.sources.modes.receiver as modes_receiver

_HOST = "dummy-host"
_PORT = 30002


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch: pytest.MonkeyPatch) -> None:
    modes_receiver.reset()
    # 待機・プロセス停止要求は実際には行わない
    monkeypatch.setattr(modes_receiver, "_wait_with_interrupt", lambda _delay: None)
    monkeypatch.setattr(modes_receiver, "_process_message", lambda *_args: None)


def _make_sock(*recv_results: bytes | BaseException) -> MagicMock:
    sock = MagicMock()
    sock.recv.side_effect = list(recv_results)
    return sock


class TestProcessSocketMessages:
    """_process_socket_messages が接続の終わり方を正しく分類すること"""

    def test_timeout_without_data_is_silent_timeout(self) -> None:
        sock = _make_sock(TimeoutError())

        outcome = modes_receiver._process_socket_messages(sock, MagicMock(), MagicMock())

        assert outcome is modes_receiver._ConnectionOutcome.SILENT_TIMEOUT

    def test_remote_close_without_data_is_closed_without_data(self) -> None:
        sock = _make_sock(b"")

        outcome = modes_receiver._process_socket_messages(sock, MagicMock(), MagicMock())

        assert outcome is modes_receiver._ConnectionOutcome.CLOSED_WITHOUT_DATA

    def test_os_error_without_data_is_closed_without_data(self) -> None:
        sock = _make_sock(ConnectionResetError("reset"))

        outcome = modes_receiver._process_socket_messages(sock, MagicMock(), MagicMock())

        assert outcome is modes_receiver._ConnectionOutcome.CLOSED_WITHOUT_DATA

    def test_data_then_timeout_is_received(self) -> None:
        sock = _make_sock(b"*8D861F3C99458E8DE804161B720E;\n", TimeoutError())

        outcome = modes_receiver._process_socket_messages(sock, MagicMock(), MagicMock())

        assert outcome is modes_receiver._ConnectionOutcome.RECEIVED

    def test_data_then_close_is_received(self) -> None:
        sock = _make_sock(b"*8D861F3C99458E8DE804161B720E;\n", b"")

        outcome = modes_receiver._process_socket_messages(sock, MagicMock(), MagicMock())

        assert outcome is modes_receiver._ConnectionOutcome.RECEIVED

    def test_received_line_updates_last_received_at(self) -> None:
        modes_receiver._state.last_received_at = 0.0
        sock = _make_sock(b"*8D861F3C99458E8DE804161B720E;\n", b"")

        modes_receiver._process_socket_messages(sock, MagicMock(), MagicMock())

        assert modes_receiver._state.last_received_at > 0.0


class TestSilenceLogging:
    """無音ログのスロットル"""

    def test_first_silence_is_info_then_debug_then_warning(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        clock = {"now": 1000.0}
        monkeypatch.setattr(modes_receiver.time, "time", lambda: clock["now"])
        modes_receiver._state.last_received_at = 1000.0
        interval = amdar.constants.MODES_RECEIVER_SILENCE_WARN_INTERVAL_SECONDS

        with caplog.at_level(logging.DEBUG):
            clock["now"] = 1030.0
            modes_receiver._log_silence()
            clock["now"] = 1060.0
            modes_receiver._log_silence()
            clock["now"] = 1030.0 + interval
            modes_receiver._log_silence()

        levels = [record.levelno for record in caplog.records if "受信していません" in record.getMessage()]
        assert levels == [logging.INFO, logging.WARNING]
        assert any(
            record.levelno == logging.DEBUG and "無音が継続中" in record.getMessage()
            for record in caplog.records
        )

    def test_resume_after_silence_is_logged_and_resets_throttle(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        clock = {"now": 1000.0}
        monkeypatch.setattr(modes_receiver.time, "time", lambda: clock["now"])
        modes_receiver._state.last_received_at = 1000.0
        modes_receiver._state.silence_warned_at = 1030.0

        clock["now"] = 1000.0 + amdar.constants.MODES_RECEIVER_SOCKET_TIMEOUT + 5
        with caplog.at_level(logging.INFO):
            modes_receiver._mark_data_received()

        assert any("受信を再開しました" in record.getMessage() for record in caplog.records)
        assert modes_receiver._state.silence_warned_at == 0.0
        assert modes_receiver._state.last_received_at == clock["now"]


class TestWorkerRetryPolicy:
    """_worker の失敗カウントとプロセス停止要求"""

    def _run_worker(
        self, monkeypatch: pytest.MonkeyPatch, outcomes: list[object]
    ) -> tuple[MagicMock, MagicMock]:
        """outcomes を順に返す（例外なら raise する）_handle_connection で _worker を回す

        outcomes を消費し切ったら should_terminate を立てて終了させる。
        """
        remaining = list(outcomes)
        terminate = MagicMock()

        def fake_handle_connection(*_args: object) -> modes_receiver._ConnectionOutcome:
            if not remaining:
                modes_receiver._state.should_terminate.set()
                return modes_receiver._ConnectionOutcome.RECEIVED
            item = remaining.pop(0)
            if isinstance(item, BaseException):
                raise item
            assert isinstance(item, modes_receiver._ConnectionOutcome)
            return item

        def fake_terminate(reason: str) -> None:
            terminate(reason)
            modes_receiver._state.fatal_error = True
            modes_receiver._state.should_terminate.set()

        wait = MagicMock()
        monkeypatch.setattr(modes_receiver, "_handle_connection", fake_handle_connection)
        monkeypatch.setattr(modes_receiver, "_request_process_termination", fake_terminate)
        monkeypatch.setattr(modes_receiver, "_wait_with_interrupt", wait)

        modes_receiver._worker(_HOST, _PORT, MagicMock(), MagicMock())
        return terminate, wait

    def test_silent_timeouts_never_count_as_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """上限を大きく超える回数の無音が続いてもワーカーは停止しない"""
        silent = [modes_receiver._ConnectionOutcome.SILENT_TIMEOUT] * (
            amdar.constants.MODES_RECEIVER_MAX_RETRIES * 3
        )

        terminate, wait = self._run_worker(monkeypatch, silent)

        terminate.assert_not_called()
        wait.assert_not_called()  # 無音時はバックオフせず即再接続
        assert modes_receiver.has_fatal_error() is False

    def test_closed_without_data_counts_and_backs_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        closed = modes_receiver._ConnectionOutcome.CLOSED_WITHOUT_DATA

        terminate, wait = self._run_worker(monkeypatch, [closed, closed, closed])

        terminate.assert_not_called()
        assert [call.args[0] for call in wait.call_args_list] == [2.0, 4.0, 8.0]

    def test_received_resets_failure_count(self, monkeypatch: pytest.MonkeyPatch) -> None:
        closed = modes_receiver._ConnectionOutcome.CLOSED_WITHOUT_DATA
        received = modes_receiver._ConnectionOutcome.RECEIVED
        max_retries = amdar.constants.MODES_RECEIVER_MAX_RETRIES
        # 上限直前まで失敗 → 受信 → また上限直前まで失敗、で合計は上限を超えるが停止しない
        outcomes = [closed] * max_retries + [received] + [closed] * max_retries

        terminate, _wait = self._run_worker(monkeypatch, outcomes)

        terminate.assert_not_called()

    def test_silent_timeout_resets_failure_count(self, monkeypatch: pytest.MonkeyPatch) -> None:
        closed = modes_receiver._ConnectionOutcome.CLOSED_WITHOUT_DATA
        silent = modes_receiver._ConnectionOutcome.SILENT_TIMEOUT
        max_retries = amdar.constants.MODES_RECEIVER_MAX_RETRIES
        outcomes = [closed] * max_retries + [silent] + [closed] * max_retries

        terminate, _wait = self._run_worker(monkeypatch, outcomes)

        terminate.assert_not_called()

    def test_exceeding_max_retries_requests_process_termination(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        errors: list[object] = [ConnectionRefusedError("refused")] * (
            amdar.constants.MODES_RECEIVER_MAX_RETRIES + 1
        )
        slack = MagicMock()
        monkeypatch.setattr(modes_receiver.my_lib.notify.slack, "error", slack)
        modes_receiver._state.slack_config = MagicMock()

        terminate, wait = self._run_worker(monkeypatch, errors)

        terminate.assert_called_once()
        assert "最大再接続回数" in terminate.call_args.args[0]
        assert wait.call_count == amdar.constants.MODES_RECEIVER_MAX_RETRIES
        slack.assert_called_once()
        assert modes_receiver.has_fatal_error() is True

    def test_unexpected_exception_requests_process_termination(self, monkeypatch: pytest.MonkeyPatch) -> None:
        terminate, _wait = self._run_worker(monkeypatch, [RuntimeError("boom")])

        terminate.assert_called_once()
        assert modes_receiver.has_fatal_error() is True


class TestRequestProcessTermination:
    def test_sends_sigterm_to_own_process_and_sets_flag(self, monkeypatch: pytest.MonkeyPatch) -> None:
        kill = MagicMock()
        monkeypatch.setattr(modes_receiver.os, "kill", kill)
        monkeypatch.setattr(modes_receiver.os, "getpid", lambda: 4242)

        modes_receiver._request_process_termination("test")

        kill.assert_called_once_with(4242, modes_receiver.signal.SIGTERM)
        assert modes_receiver.has_fatal_error() is True
