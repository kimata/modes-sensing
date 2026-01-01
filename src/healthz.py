#!/usr/bin/env python3
"""
Liveness のチェックを行います

Usage:
  healthz.py [-c CONFIG] [-m MODE] [-p PORT] [-D]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します。[default: config.yaml]
  -m (COL|WEB)      : 動作モード [default: COL]
  -p PORT           : WEB サーバのポートを指定します。[default: 5000]
  -D                : デバッグモードで動作します。
"""

import logging
import pathlib

import my_lib.healthz
from my_lib.healthz import HealthzTarget

SCHEMA_CONFIG = "config.schema"


def check_liveness(targets: list[HealthzTarget], port: int | None = None) -> bool:
    """複数ターゲットの liveness をチェックする"""
    for target in targets:
        if not my_lib.healthz.check_liveness(target):
            return False

    if port is not None:
        return my_lib.healthz.check_http_port(port)
    return True


if __name__ == "__main__":
    import sys

    import docopt
    import my_lib.config
    import my_lib.logger
    import my_lib.pretty

    assert __doc__ is not None  # noqa: S101
    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    mode = args["-m"]
    port = args["-p"]
    debug_mode = args["-D"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = my_lib.config.load(config_file, pathlib.Path(SCHEMA_CONFIG))

    logging.info("Mode: %s", mode)

    if mode == "COL":
        conf_list = ["collector", "receiver"]
        port = None
    else:
        conf_list = []

    targets = [
        HealthzTarget(
            name=conf,
            liveness_file=pathlib.Path(config["liveness"]["file"][conf]),
            interval=60 * 10,
        )
        for conf in conf_list
    ]

    logging.debug(my_lib.pretty.format(targets))

    if check_liveness(targets, port):
        logging.info("OK.")
        sys.exit(0)
    else:
        sys.exit(-1)
