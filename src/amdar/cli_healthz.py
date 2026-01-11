#!/usr/bin/env python3
"""
Liveness のチェックを行います

Usage:
  amdar-healthz [-c CONFIG] [-m MODE] [-p PORT] [-D]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します。[default: config.yaml]
  -m (COL|WEB)      : 動作モード [default: COL]
  -p PORT           : WEB サーバのポートを指定します。[default: 5000]
  -D                : デバッグモードで動作します。
"""

import logging
import pathlib
from datetime import datetime
from typing import Any

import my_lib.healthz
from my_lib.healthz import HealthzTarget

_SCHEMA_CONFIG = "config.schema"


def _get_timeout_for_now(schedule: dict[str, Any]) -> int:
    """現在時刻に応じたタイムアウト値を返す

    Args:
        schedule: liveness.schedule 設定辞書

    Returns:
        タイムアウト秒数
    """
    current_hour = datetime.now().hour
    daytime = schedule["daytime"]
    start_hour = daytime["start_hour"]
    end_hour = daytime["end_hour"]

    if start_hour <= current_hour < end_hour:
        return daytime["timeout_sec"]
    return schedule["nighttime"]["timeout_sec"]


def check_liveness(targets: list[HealthzTarget], port: int | None = None) -> bool:
    """複数ターゲットの liveness をチェックする"""
    for target in targets:
        if not my_lib.healthz.check_liveness(target):
            return False

    if port is not None:
        return my_lib.healthz.check_http_port(port)
    return True


def main() -> None:
    """CLI エントリポイント"""
    import sys

    import docopt
    import my_lib.config
    import my_lib.logger
    import my_lib.pretty

    if __doc__ is None:
        raise RuntimeError("__doc__ is not set")

    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    mode = args["-m"]
    port = args["-p"]
    debug_mode = args["-D"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = my_lib.config.load(config_file, pathlib.Path(_SCHEMA_CONFIG))

    logging.info("Mode: %s", mode)

    # 時間帯に応じたタイムアウト値を取得
    timeout = _get_timeout_for_now(config["liveness"]["schedule"])
    logging.info("Current timeout: %d seconds", timeout)

    targets: list[HealthzTarget] = []

    if mode == "COL":
        # collector liveness
        targets.append(
            HealthzTarget(
                name="collector",
                liveness_file=pathlib.Path(config["liveness"]["file"]["collector"]),
                interval=timeout,
            )
        )

        # modes receiver liveness
        targets.append(
            HealthzTarget(
                name="modes",
                liveness_file=pathlib.Path(config["liveness"]["file"]["receiver"]["modes"]),
                interval=timeout,
            )
        )

        # vdl2 receiver liveness (設定されている場合のみ)
        # VDL2はデータ受信頻度が低いため、長めのタイムアウト（10分）を設定
        vdl2_file = config["liveness"]["file"]["receiver"].get("vdl2")
        if vdl2_file:
            vdl2_timeout = max(timeout, 600)  # 最低10分
            targets.append(
                HealthzTarget(
                    name="vdl2",
                    liveness_file=pathlib.Path(vdl2_file),
                    interval=vdl2_timeout,
                )
            )

        port = None
    else:
        # WEB mode では liveness チェックなし
        pass

    logging.debug(my_lib.pretty.format(targets))

    if check_liveness(targets, port):
        logging.info("OK.")
        sys.exit(0)
    else:
        sys.exit(-1)


if __name__ == "__main__":
    main()
