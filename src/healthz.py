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

SCHEMA_CONFIG = "config.schema"


def check_liveness(target_list, port=None):
    for target in target_list:
        if not my_lib.healthz.check_liveness(target["name"], target["liveness_file"], target["interval"]):
            return False

    if port is not None:
        return my_lib.healthz.check_http_port(port)
    else:
        return True


if __name__ == "__main__":
    import sys

    import docopt
    import my_lib.config
    import my_lib.logger
    import my_lib.pretty

    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    mode = args["-m"]
    port = args["-p"]
    debug_mode = args["-D"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = my_lib.config.load(config_file, pathlib.Path(SCHEMA_CONFIG))

    logging.info("Mode: %s", mode)

    if mode == "COL":
        conf_list = ["collector"]
        port = None
    else:
        conf_list = []

    target_list = [
        {"name": conf, "liveness_file": config["liveness"]["file"][conf], "interval": 60 * 10}
        for conf in conf_list
    ]

    logging.debug(my_lib.pretty.format(target_list))

    if check_liveness(target_list, port):
        logging.info("OK.")
        sys.exit(0)
    else:
        sys.exit(-1)
