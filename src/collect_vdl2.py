#!/usr/bin/env python3
"""VDL2 気象データを受信して表示するエントリーポイント

dumpvdl2 から ACARS メッセージを受信し、気象データを抽出して表示します。
現段階では DB 保存は行いません。

Usage:
    python collect_vdl2.py [-c CONFIG] [-D] [-n COUNT]

Options:
    -c CONFIG  : 設定ファイルパス [default: config.yaml]
    -D         : デバッグモード
    -n COUNT   : 指定件数で停止
"""

from __future__ import annotations

import argparse
import logging
import pathlib
import queue
import sys
from typing import TYPE_CHECKING

import my_lib.config
import my_lib.logger

import amdar.config
import amdar.sources.vdl2.receiver as vdl2_receiver

if TYPE_CHECKING:
    import amdar.database.postgresql


def _parse_args() -> argparse.Namespace:
    """コマンドライン引数を解析する"""
    parser = argparse.ArgumentParser(description="Collect VDL2 weather data")
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="Path to config file",
    )
    parser.add_argument(
        "-D",
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )
    parser.add_argument(
        "-n",
        "--count",
        type=int,
        default=0,
        help="Stop after N data points (0 = unlimited)",
    )
    return parser.parse_args()


def _main() -> None:
    """メイン処理"""
    args = _parse_args()

    # ロガー設定
    my_lib.logger.init("vdl2-collect", level=logging.DEBUG if args.debug else logging.INFO)

    # 設定ファイルの読み込み
    config_path = pathlib.Path(args.config)
    if not config_path.exists():
        logging.error("Config file not found: %s", config_path)
        sys.exit(1)

    config_dict = my_lib.config.load(str(config_path))

    # VDL2 設定の取得
    decoder_config = config_dict.get("decoder", {})
    vdl2_config = decoder_config.get("vdl2")
    if vdl2_config is None:
        logging.error("VDL2 設定が見つかりません。config.yaml に decoder.vdl2 を設定してください。")
        sys.exit(1)

    host = vdl2_config["host"]
    port = vdl2_config["port"]

    # 基準点の取得
    filter_config = config_dict.get("filter", {}).get("area", {})
    ref_lat = filter_config.get("lat", {}).get("ref", 35.682677)
    ref_lon = filter_config.get("lon", {}).get("ref", 139.762230)

    logging.info("VDL2 host: %s:%d", host, port)
    logging.info("Reference point: %.6f, %.6f", ref_lat, ref_lon)

    # データキュー
    data_queue: queue.Queue[amdar.database.postgresql.MeasurementData] = queue.Queue()

    # 受信開始
    vdl2_receiver.start(host, port, data_queue, ref_lat, ref_lon)

    count = 0
    try:
        while True:
            try:
                data = data_queue.get(timeout=60)
                count += 1

                # データ表示
                wind_info = ""
                if data.wind.speed > 0:
                    wind_info = f", wind={data.wind.speed:.1f}m/s @ {data.wind.angle:.0f}°"

                pos_info = ""
                if data.latitude != 0 and data.longitude != 0:
                    pos_info = f", pos=({data.latitude:.3f}, {data.longitude:.3f})"

                print(
                    f"[{count}] {data.callsign}: "
                    f"alt={data.altitude:.0f}m, "
                    f"temp={data.temperature:.1f}°C"
                    f"{wind_info}{pos_info}"
                )

                # 指定件数で停止
                if args.count > 0 and count >= args.count:
                    logging.info("Reached %d data points, stopping", count)
                    break

            except queue.Empty:
                logging.debug("No data received in 60 seconds")
                continue

    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    finally:
        vdl2_receiver.term()
        logging.info("Total: %d weather data points", count)


if __name__ == "__main__":
    _main()
