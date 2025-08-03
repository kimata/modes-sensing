#!/usr/bin/env python3
"""
ModeS のメッセージを PostgreSQL に保存します

Usage:
  collect.py [-c CONFIG] [-D]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
  -D                : デバッグモードで動作します．
"""

import logging
import multiprocessing
import pathlib
import signal

import my_lib.footprint

import modes.database_postgresql
import modes.receiver

SCHEMA_CONFIG = "config.schema"


def sig_handler(num, _):
    logging.warning("receive signal %d", num)

    if num in (signal.SIGTERM, signal.SIGINT):
        modes.database_postgresql.store_term()
        modes.receiver.term()


def execute(config):
    signal.signal(signal.SIGTERM, sig_handler)

    measurement_queue = multiprocessing.Queue()

    modes.receiver.start(
        config["modes"]["decoder"]["host"],
        config["modes"]["decoder"]["port"],
        measurement_queue,
        config["filter"]["area"],
    )

    conn = modes.database_postgresql.open(
        config["database"]["host"],
        config["database"]["port"],
        config["database"]["name"],
        config["database"]["user"],
        config["database"]["pass"],
    )

    try:
        modes.database_postgresql.store_queue(conn, measurement_queue)
    except Exception:
        logging.exception("Failed to store data")


######################################################################
if __name__ == "__main__":
    import docopt
    import my_lib.config
    import my_lib.logger

    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    debug_mode = args["-D"]

    my_lib.logger.init("modes sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = my_lib.config.load(config_file, pathlib.Path(SCHEMA_CONFIG))

    execute(config)
