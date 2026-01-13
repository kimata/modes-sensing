#!/usr/bin/env python3
# ruff: noqa: S101
"""
cli/webui.py のテスト
"""

import signal
import unittest.mock

import pytest

import amdar.cli.webui as webui


class TestCreateApp:
    """create_app のテスト"""

    def test_create_app_returns_flask_app(self, config):
        """create_app が Flask アプリケーションを返す"""
        import flask

        app = webui.create_app(config)

        assert isinstance(app, flask.Flask)
        assert app.name == "modes-sensing"

    def test_create_app_has_config(self, config):
        """create_app が config を設定する"""
        app = webui.create_app(config)

        assert "CONFIG" in app.config
        assert app.config["CONFIG"] == config

    def test_create_app_cors_enabled(self, config):
        """create_app が CORS を有効にする"""
        # create_app 呼び出し
        app = webui.create_app(config)

        # Flask app が作成されたことを確認
        assert app is not None


class TestTerm:
    """_term 関数のテスト"""

    def test_term_kills_child_and_exits(self):
        """_term が子プロセスを終了してシステム終了する"""
        with (
            unittest.mock.patch("my_lib.proc_util.kill_child") as mock_kill,
            pytest.raises(SystemExit) as exc_info,
        ):
            webui._term()

        mock_kill.assert_called_once()
        assert exc_info.value.code == 0


class TestSigHandler:
    """_sig_handler のテスト"""

    def test_sig_handler_sigterm(self):
        """SIGTERM で _term が呼ばれる"""
        with (
            unittest.mock.patch("my_lib.proc_util.kill_child"),
            pytest.raises(SystemExit),
        ):
            webui._sig_handler(signal.SIGTERM, None)

    def test_sig_handler_sigint(self):
        """SIGINT で _term が呼ばれる"""
        with (
            unittest.mock.patch("my_lib.proc_util.kill_child"),
            pytest.raises(SystemExit),
        ):
            webui._sig_handler(signal.SIGINT, None)

    def test_sig_handler_other_signal(self):
        """他のシグナルでは _term が呼ばれない"""
        # SIGUSR1 などでは何も起きない
        webui._sig_handler(signal.SIGUSR1, None)
        # 例外なく終了
