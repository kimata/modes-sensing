#!/usr/bin/env python3
"""VDL2 データをキャプチャしてファイルに保存する

Usage:
    python capture_vdl2.py [-o OUTPUT] [-d DURATION] [-h HOST] [-p PORT]

Options:
    -o OUTPUT   : 出力ファイル [default: vdl2_capture.jsonl]
    -d DURATION : キャプチャ時間（秒） [default: 10800]  # 3時間
    -h HOST     : dumpvdl2 host [default: 192.168.0.20]
    -p PORT     : dumpvdl2 ZMQ port [default: 5050]
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import zmq


def capture_vdl2(
    host: str,
    port: int,
    output_file: Path,
    duration_seconds: int,
) -> int:
    """VDL2 データをキャプチャする

    Args:
        host: dumpvdl2 ホスト
        port: dumpvdl2 ZMQ ポート
        output_file: 出力ファイル
        duration_seconds: キャプチャ時間（秒）

    Returns:
        キャプチャしたメッセージ数
    """
    logging.info("VDL2 capture starting: %s:%d -> %s", host, port, output_file)
    logging.info("Duration: %d seconds (%.1f hours)", duration_seconds, duration_seconds / 3600)

    ctx = zmq.Context()
    socket = ctx.socket(zmq.SUB)
    socket.connect(f"tcp://{host}:{port}")
    socket.setsockopt(zmq.SUBSCRIBE, b"")
    socket.setsockopt(zmq.RCVTIMEO, 5000)  # 5秒タイムアウト

    logging.info("Connected to tcp://%s:%d", host, port)

    start_time = time.time()
    message_count = 0

    try:
        with output_file.open("wb") as f:
            while time.time() - start_time < duration_seconds:
                try:
                    msg = socket.recv()
                    f.write(msg + b"\n")
                    message_count += 1

                    if message_count % 1000 == 0:
                        elapsed = time.time() - start_time
                        rate = message_count / elapsed
                        logging.info(
                            "Progress: %d messages, %.2f msg/s, %.1f%% complete",
                            message_count,
                            rate,
                            (elapsed / duration_seconds) * 100,
                        )
                        f.flush()

                except zmq.Again:
                    # タイムアウト、継続
                    continue

    finally:
        socket.close()
        ctx.term()

    elapsed = time.time() - start_time
    logging.info(
        "Capture complete: %d messages in %.1f seconds (%.2f msg/s)",
        message_count,
        elapsed,
        message_count / elapsed if elapsed > 0 else 0,
    )
    return message_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture VDL2 data")
    parser.add_argument("-o", "--output", default="vdl2_capture.jsonl", help="Output file")
    parser.add_argument("-d", "--duration", type=int, default=10800, help="Duration in seconds")
    parser.add_argument("-H", "--host", default="192.168.0.20", help="dumpvdl2 host")
    parser.add_argument("-p", "--port", type=int, default=5050, help="dumpvdl2 ZMQ port")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    output_path = Path(args.output)
    try:
        capture_vdl2(args.host, args.port, output_path, args.duration)
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    except Exception as e:
        logging.error("Capture failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
