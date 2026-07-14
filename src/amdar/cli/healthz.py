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

import my_lib.healthz
import my_lib.healthz.cli
import my_lib.notify.slack
import my_lib.time

import amdar.config
import amdar.constants


def _get_timeout_for_now(schedule: amdar.config.LivenessScheduleConfig) -> int:
    """現在時刻に応じたタイムアウト値を返す

    Args:
        schedule: liveness.schedule 設定

    Returns:
        タイムアウト秒数
    """
    current_hour = my_lib.time.now().hour
    start_hour = schedule.daytime_start_hour
    end_hour = schedule.daytime_end_hour

    if start_hour <= current_hour < end_hour:
        return schedule.daytime_timeout_sec
    return schedule.nighttime_timeout_sec


def _load_config(config_file, args):
    logging.info("Mode: %s", args["-m"])
    return amdar.config.load_config(config_file)


def _targets(config, args):
    if args["-m"] != "COL":
        # WEB mode では liveness チェックなし (ポートチェックのみ)
        return []

    # 時間帯に応じたタイムアウト値を取得
    timeout = _get_timeout_for_now(config.liveness.schedule)
    logging.info("Current timeout: %d seconds", timeout)

    targets = [
        my_lib.healthz.HealthzTarget(
            name="collector",
            liveness_file=config.liveness.file.collector,
            interval=timeout,
        ),
        my_lib.healthz.HealthzTarget(
            name="modes",
            liveness_file=config.liveness.file.receiver.modes,
            interval=timeout,
        ),
    ]

    # vdl2 receiver liveness (設定されている場合のみ)
    # VDL2はデータ受信頻度が低いため、長時間受信できない場合のみエラーにする
    vdl2_file = config.liveness.file.receiver.vdl2
    if vdl2_file:
        targets.append(
            my_lib.healthz.HealthzTarget(
                name="vdl2",
                liveness_file=vdl2_file,
                interval=amdar.constants.VDL2_LIVENESS_TIMEOUT_SECONDS,
            )
        )

    return targets


def _failure_handler(config, args, failed):
    """コンテナ起動後の猶予期間を過ぎている場合のみ Slack 通知する

    VDL2 はデータ受信頻度が低いため、長い猶予期間を設定。
    """
    failed_target = failed[0]
    grace_period = (
        amdar.constants.VDL2_STARTUP_GRACE_PERIOD_SECONDS
        if failed_target == "vdl2"
        else amdar.constants.CONTAINER_STARTUP_GRACE_PERIOD_SECONDS
    )
    if my_lib.healthz.cli.within_startup_grace(grace_period):
        return

    my_lib.notify.slack.error(
        config.slack,
        "modes-sensing Liveness Check Failed",
        f"Liveness check failed for target: {failed_target}",
    )


SPEC = my_lib.healthz.cli.HealthzCliSpec(
    logger_name="modes-sensing",
    config_loader=_load_config,
    targets_builder=_targets,
    use_http_port=True,
    # COL モードは WEB サーバを持たないためポートチェックしない
    http_port_enabled=lambda config, args: args["-m"] != "COL",
    failure_handler=_failure_handler,
)


def main() -> None:
    """CLI エントリポイント"""
    assert __doc__ is not None  # noqa: S101
    my_lib.healthz.cli.run(SPEC, __doc__)


if __name__ == "__main__":
    main()
