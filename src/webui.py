#!/usr/bin/env python3
"""
気象データの Web UI サーバです。

Usage:
  webui.py [-c CONFIG] [-p PORT] [-D]

Options:
  -c CONFIG         : 通常モードで使う設定ファイルを指定します。[default: config.yaml]
  -p PORT           : WEB サーバのポートを指定します。[default: 5000]
  -D                : デバッグモードで動作します。
"""

from __future__ import annotations

import logging
import pathlib
import signal
import sys
from typing import TYPE_CHECKING, Any, NoReturn

import flask
import flask_cors
import my_lib.config
import my_lib.logger
import my_lib.proc_util

if TYPE_CHECKING:
    from types import FrameType

SCHEMA_CONFIG = "config.schema"


def term() -> NoReturn:
    # 子プロセスを終了
    my_lib.proc_util.kill_child()

    # プロセス終了
    logging.info("Graceful shutdown completed")
    sys.exit(0)


def sig_handler(num: int, frame: FrameType | None) -> None:  # noqa: ARG001
    logging.warning("receive signal %d", num)

    if num in (signal.SIGTERM, signal.SIGINT):
        term()


def create_app(config: dict[str, Any]) -> flask.Flask:
    # NOTE: アクセスログは無効にする
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    import my_lib.webapp.config

    my_lib.webapp.config.URL_PREFIX = "/modes-sensing"
    my_lib.webapp.config.init(config)

    import my_lib.webapp.base
    import my_lib.webapp.util

    import modes.webui.api.graph

    app = flask.Flask("modes-sensing")

    flask_cors.CORS(app)

    app.config["CONFIG"] = config

    app.register_blueprint(my_lib.webapp.base.blueprint, url_prefix=my_lib.webapp.config.URL_PREFIX)
    app.register_blueprint(my_lib.webapp.base.blueprint_default)
    app.register_blueprint(my_lib.webapp.util.blueprint, url_prefix=my_lib.webapp.config.URL_PREFIX)
    app.register_blueprint(modes.webui.api.graph.blueprint, url_prefix=my_lib.webapp.config.URL_PREFIX)

    my_lib.webapp.config.show_handler_list(app)

    # アプリケーション起動時にキャッシュの定期更新を開始
    @app.before_request
    def init_cache():
        # 初回リクエスト時のみ実行
        if not hasattr(app, "_cache_initialized"):
            logging.info("Starting periodic cache update...")
            # Note: アクセスしているのはパブリックAPIとして設計されたグローバル変数
            modes.webui.api.graph._graph_cache.start_periodic_update(config)  # noqa: SLF001
            app._cache_initialized = True  # noqa: SLF001

    return app


if __name__ == "__main__":
    import atexit
    import contextlib
    import os

    import docopt

    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    port = args["-p"]
    debug_mode = args["-D"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = my_lib.config.load(config_file, pathlib.Path(SCHEMA_CONFIG))

    app = create_app(config)

    # プロセスグループリーダーとして実行（リローダープロセスの適切な管理のため）
    with contextlib.suppress(PermissionError):
        os.setpgrp()

    # 異常終了時のクリーンアップ処理を登録
    def cleanup_on_exit():
        try:
            current_pid = os.getpid()
            pgid = os.getpgid(current_pid)
            if current_pid == pgid:
                # プロセスグループ内の他のプロセスを終了
                os.killpg(pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass

    atexit.register(cleanup_on_exit)

    # Enhanced signal handler for process group management
    def enhanced_sig_handler(num, frame):  # noqa: ARG001
        logging.warning("receive signal %d", num)

        if num in (signal.SIGTERM, signal.SIGINT):
            # Flask reloader の子プロセスも含めて終了する
            try:
                # 現在のプロセスがプロセスグループリーダーの場合、全体を終了
                current_pid = os.getpid()
                pgid = os.getpgid(current_pid)
                if current_pid == pgid:
                    logging.info("Terminating process group %d", pgid)
                    os.killpg(pgid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                # プロセスグループ操作に失敗した場合は通常の終了処理
                pass

            term()

    signal.signal(signal.SIGTERM, enhanced_sig_handler)
    signal.signal(signal.SIGINT, enhanced_sig_handler)

    # Flaskアプリケーションを実行
    try:
        # NOTE: キャッシュ機能により初期化が重いため、開発時も自動リロードは無効化
        app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=True, debug=debug_mode)  # noqa: S104
    except KeyboardInterrupt:
        logging.info("Received KeyboardInterrupt, shutting down...")
        enhanced_sig_handler(signal.SIGINT, None)
