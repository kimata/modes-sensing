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

import my_lib.container_util
import my_lib.healthz
import my_lib.notify.slack
from my_lib.healthz import HealthzTarget

import amdar.config

_SCHEMA_CONFIG = "config.schema"
_CONTAINER_STARTUP_GRACE_PERIOD = 120  # コンテナ起動後の猶予期間（秒）
_VDL2_STARTUP_GRACE_PERIOD = 10 * 60 * 60  # VDL2 用の猶予期間（10時間）


def _get_timeout_for_now(schedule: amdar.config.LivenessScheduleConfig) -> int:
    """現在時刻に応じたタイムアウト値を返す

    Args:
        schedule: liveness.schedule 設定

    Returns:
        タイムアウト秒数
    """
    current_hour = datetime.now().hour
    start_hour = schedule.daytime_start_hour
    end_hour = schedule.daytime_end_hour

    if start_hour <= current_hour < end_hour:
        return schedule.daytime_timeout_sec
    return schedule.nighttime_timeout_sec


def _notify_error(
    slack_config: my_lib.notify.slack.SlackErrorOnlyConfig | my_lib.notify.slack.SlackEmptyConfig,
    message: str,
) -> None:
    """Slack でエラー通知を送信する."""
    my_lib.notify.slack.error(
        slack_config,
        "modes-sensing Liveness Check Failed",
        message,
    )


def check_liveness(targets: list[HealthzTarget], port: int | None = None) -> tuple[bool, str | None]:
    """複数ターゲットの liveness をチェックする

    Returns:
        (成功したか, 失敗したターゲット名)
    """
    for target in targets:
        if not my_lib.healthz.check_liveness(target):
            return (False, target.name)

    if port is not None and not my_lib.healthz.check_http_port(port):
        return (False, "http_port")
    return (True, None)


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

    config_dict = my_lib.config.load(config_file, pathlib.Path(_SCHEMA_CONFIG))
    config = amdar.config.load_from_dict(config_dict, pathlib.Path.cwd())

    logging.info("Mode: %s", mode)

    # 時間帯に応じたタイムアウト値を取得
    timeout = _get_timeout_for_now(config.liveness.schedule)
    logging.info("Current timeout: %d seconds", timeout)

    targets: list[HealthzTarget] = []

    if mode == "COL":
        # collector liveness
        targets.append(
            HealthzTarget(
                name="collector",
                liveness_file=config.liveness.file.collector,
                interval=timeout,
            )
        )

        # modes receiver liveness
        targets.append(
            HealthzTarget(
                name="modes",
                liveness_file=config.liveness.file.receiver.modes,
                interval=timeout,
            )
        )

        # vdl2 receiver liveness (設定されている場合のみ)
        # VDL2はデータ受信頻度が低いため、長時間受信できない場合のみエラーにする
        vdl2_file = config.liveness.file.receiver.vdl2
        if vdl2_file:
            vdl2_timeout = 8 * 60 * 60  # 8時間
            targets.append(
                HealthzTarget(
                    name="vdl2",
                    liveness_file=vdl2_file,
                    interval=vdl2_timeout,
                )
            )

        port = None
    else:
        # WEB mode では liveness チェックなし
        pass

    logging.debug(my_lib.pretty.format(targets))

    success, failed_target = check_liveness(targets, port)

    if success:
        logging.info("OK.")
        sys.exit(0)
    else:
        # コンテナ起動後の猶予期間を過ぎている場合のみ通知
        # VDL2 はデータ受信頻度が低いため、長い猶予期間を設定
        uptime = my_lib.container_util.get_uptime()  # type: ignore[attr-defined]
        grace_period = (
            _VDL2_STARTUP_GRACE_PERIOD if failed_target == "vdl2" else _CONTAINER_STARTUP_GRACE_PERIOD
        )
        if uptime > grace_period:
            _notify_error(
                config.slack,
                f"Liveness check failed for target: {failed_target}",
            )
        else:
            logging.info("Within startup grace period (%.1f sec), skipping notification.", uptime)

        sys.exit(-1)


if __name__ == "__main__":
    main()
