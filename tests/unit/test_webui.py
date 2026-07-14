#!/usr/bin/env python3
# ruff: noqa: S101
"""
cli/webui.py のテスト
"""

import unittest.mock

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

    def test_create_app_cors_restricted_to_localhost(self, config):
        """CORS が localhost 系オリジンのみに限定されている"""
        app = webui.create_app(config)
        client = app.test_client()

        # 許可されるオリジン（開発時の Vite）
        res = client.get(
            "/modes-sensing/api/graph/jobs/stats",
            headers={"Origin": "http://localhost:5173"},
        )
        assert res.headers.get("Access-Control-Allow-Origin") == "http://localhost:5173"

        # 許可されないオリジン
        res = client.get(
            "/modes-sensing/api/graph/jobs/stats",
            headers={"Origin": "http://evil.example.com"},
        )
        assert res.headers.get("Access-Control-Allow-Origin") is None

    def test_create_app_reloader_parent_skips_background_init(self, config):
        """リローダー親プロセスではバックグラウンド初期化をスキップする"""
        with (
            unittest.mock.patch.dict("os.environ", {}, clear=False),
            unittest.mock.patch(
                "amdar.viewer.api.cache_pregeneration.cache_pregenerator.initialize"
            ) as mock_pregen,
        ):
            import os

            os.environ.pop("WERKZEUG_RUN_MAIN", None)
            app = webui.create_app(config, use_reloader=True)

        assert app is not None
        mock_pregen.assert_not_called()


class TestSpec:
    """WebAppSpec 定義のテスト

    graceful shutdown・シグナル処理は my_lib.webapp.runner 側でテストされる。
    """

    def test_logger_name(self):
        """ロガー名が modes-sensing である"""
        assert webui.SPEC.logger_name == "modes-sensing"

    def test_reloader_disabled_in_test_mode(self, monkeypatch):
        """TEST=true ではリローダーを使わない"""
        monkeypatch.setenv("TEST", "true")
        assert webui._use_reloader({"-D": True}) is False

    def test_reloader_follows_debug_flag(self, monkeypatch):
        """通常時は -D 指定時のみリローダーを使う"""
        monkeypatch.delenv("TEST", raising=False)
        assert webui._use_reloader({"-D": True}) is True
        assert webui._use_reloader({"-D": False}) is False
