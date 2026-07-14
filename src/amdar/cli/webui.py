#!/usr/bin/env python3
"""
気象データの Web UI サーバです。

Usage:
  amdar-webui [-c CONFIG] [-p PORT] [-D]

Options:
  -c CONFIG         : 通常モードで使う設定ファイルを指定します。[default: config.yaml]
  -p PORT           : WEB サーバのポートを指定します。[default: 5000]
  -D                : デバッグモードで動作します。
"""

from __future__ import annotations

import logging
import os

import flask
import flask_cors
import my_lib.webapp.runner

import amdar.config

URL_PREFIX = "/modes-sensing"


def create_app(config: amdar.config.Config, use_reloader: bool = False) -> flask.Flask:
    """Flask アプリケーションを作成する。

    Args:
        config: アプリケーション設定
        use_reloader: Werkzeug リローダーを使う場合 True。リローダー使用時は
            親プロセス（監視側）でバックグラウンド処理を初期化しない
            （二重初期化の防止）。
    """
    # NOTE: アクセスログは無効にする
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    import my_lib.webapp.base
    import my_lib.webapp.config
    import my_lib.webapp.util

    import amdar.viewer.api.cache_pregeneration
    import amdar.viewer.api.data_routes
    import amdar.viewer.api.graph_routes
    import amdar.viewer.api.materialized_view_refresh
    import amdar.viewer.api.progress_estimation
    import amdar.viewer.graph.pool
    import amdar.viewer.graph.service

    # my_lib.webapp の実行環境を構築（URL prefix と静的ファイル配信パスを束ねる）
    environment = my_lib.webapp.config.build_environment(config.webapp, url_prefix=URL_PREFIX)

    app = flask.Flask("modes-sensing")

    # 本番は同一オリジン配信のため CORS は不要。開発時の Vite からのみ許可する
    flask_cors.CORS(app, origins=[r"http://localhost:\d+", r"http://127\.0\.0\.1:\d+"])

    app.config["CONFIG"] = config

    app.register_blueprint(
        my_lib.webapp.base.create_static_blueprint(environment=environment), url_prefix=URL_PREFIX
    )
    app.register_blueprint(my_lib.webapp.base.create_root_redirect_blueprint(url_prefix=URL_PREFIX))
    app.register_blueprint(my_lib.webapp.util.blueprint, url_prefix=URL_PREFIX)
    app.register_blueprint(amdar.viewer.api.graph_routes.blueprint, url_prefix=URL_PREFIX)
    app.register_blueprint(amdar.viewer.api.data_routes.blueprint, url_prefix=URL_PREFIX)

    my_lib.webapp.config.show_handler_list(app)

    # リローダー使用時、バックグラウンド初期化は再起動後の子プロセス
    # （WERKZEUG_RUN_MAIN=true）でのみ行う（親プロセスとの二重初期化を防止）
    if not my_lib.webapp.runner.should_init(use_reloader):
        logging.info("Skipping background initialization in reloader parent process")
        return app

    # グラフ生成サービスとプロセスプールを初期化する。
    # プールはバックグラウンドスレッド開始前に生成する（スレッド起動後の
    # fork はロック保持状態を子プロセスに引き継ぐ恐れがあるため）
    cache_dir = config.webapp.cache_dir_path
    amdar.viewer.graph.service.graph_service.initialize(config, cache_dir)
    amdar.viewer.graph.pool.process_pool.get_pool()

    # マテリアライズドビューの定期リフレッシュを開始
    amdar.viewer.api.materialized_view_refresh.materialized_view_refresher.initialize(config)

    # 履歴管理・キャッシュ事前生成を初期化
    amdar.viewer.api.progress_estimation.generation_time_history.initialize(cache_dir)
    amdar.viewer.api.cache_pregeneration.cache_pregenerator.initialize(config)

    return app


def _use_reloader(args: dict) -> bool:
    # テスト環境（TEST=true）ではリローダーを無効にする
    # リローダーはマルチプロセス処理（非同期グラフ生成）と相互作用の問題を起こすため
    is_test_mode = os.environ.get("TEST", "").lower() == "true"
    if is_test_mode:
        logging.info("Test mode detected, disabling Flask reloader for multiprocessing compatibility")
    return not is_test_mode and bool(args["-D"])


SPEC = my_lib.webapp.runner.WebAppSpec(
    logger_name="modes-sensing",
    config_loader=lambda config_file, args: amdar.config.load_config(config_file),
    app_factory=lambda config, ctx: create_app(config, use_reloader=ctx.use_reloader),
    use_reloader=_use_reloader,
)


def main() -> None:
    """CLI エントリポイント"""
    if __doc__ is None:
        raise RuntimeError("__doc__ is not set")

    my_lib.webapp.runner.run(SPEC, __doc__)


if __name__ == "__main__":
    main()
