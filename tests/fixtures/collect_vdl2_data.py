#!/usr/bin/env python3
"""
VDL Mode 2 データ収集スクリプト

dumpvdl2 から VDL Mode 2 メッセージを収集し、テスト用データファイルに保存します。

Usage:
    python collect_vdl2_data.py [-d DURATION] [-o OUTPUT] [-h HOST] [-p PORT]

Options:
    -d DURATION  : 収集時間（分） [default: 60]
    -o OUTPUT    : 出力ファイルパス [default: tests/fixtures/vdl2.dat]
    -H HOST      : dumpvdl2 の ZMQ ホスト [default: 192.168.0.20]
    -p PORT      : dumpvdl2 の ZMQ ポート [default: 5050]

"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time

import zmq


def collect_messages(host: str, port: int, duration_minutes: int, output_path: pathlib.Path) -> int:
    """VDL Mode 2 メッセージを収集してファイルに保存する

    Args:
        host: dumpvdl2 の ZMQ ホスト
        port: dumpvdl2 の ZMQ ポート
        duration_minutes: 収集時間（分）
        output_path: 出力ファイルパス

    Returns:
        収集したメッセージ数

    """
    duration_seconds = duration_minutes * 60
    start_time = time.time()
    message_count = 0

    print(f"Connecting to tcp://{host}:{port}...", flush=True)
    print(f"Collecting data for {duration_minutes} minutes...", flush=True)
    print(f"Output: {output_path}", flush=True)

    ctx = zmq.Context()
    socket = ctx.socket(zmq.SUB)
    socket.connect(f"tcp://{host}:{port}")
    socket.setsockopt(zmq.SUBSCRIBE, b"")
    socket.setsockopt(zmq.RCVTIMEO, 5000)  # 5秒タイムアウト

    print("Connected!", flush=True)

    with output_path.open("wb") as f:
        while time.time() - start_time < duration_seconds:
            try:
                msg = socket.recv()
                # JSON メッセージをそのまま保存（1行1メッセージ）
                f.write(msg + b"\n")
                f.flush()  # 即座にファイルに書き込み
                message_count += 1

                # 進捗表示（100メッセージごと）
                if message_count % 100 == 0:
                    elapsed = time.time() - start_time
                    remaining = duration_seconds - elapsed
                    print(
                        f"  {message_count} messages collected, {remaining / 60:.1f} minutes remaining...",
                        flush=True,
                    )

            except zmq.Again:
                # タイムアウトは正常（データがない期間）
                continue

    socket.close()
    ctx.term()

    print("\nCollection complete!")
    print(f"Total messages: {message_count}")
    print(f"Output file: {output_path}")
    print(f"File size: {output_path.stat().st_size / 1024:.1f} KB")

    return message_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect VDL Mode 2 data for testing")
    parser.add_argument("-d", "--duration", type=int, default=60, help="Duration in minutes")
    parser.add_argument("-o", "--output", default="tests/fixtures/vdl2.dat", help="Output file path")
    parser.add_argument("-H", "--host", default="192.168.0.20", help="dumpvdl2 ZMQ host")
    parser.add_argument("-p", "--port", type=int, default=5050, help="dumpvdl2 ZMQ port")
    args = parser.parse_args()

    output_path = pathlib.Path(args.output)

    # 出力ディレクトリを作成
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        collect_messages(args.host, args.port, args.duration, output_path)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except zmq.ZMQError as e:
        print(f"ZMQ Error: {e}")
        print(f"Make sure dumpvdl2 is running and publishing to tcp://{args.host}:{args.port}")
        sys.exit(1)


if __name__ == "__main__":
    main()
