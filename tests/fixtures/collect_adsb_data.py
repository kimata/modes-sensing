#!/usr/bin/env python3
"""
ADS-B データ収集スクリプト

dump1090 から生の Mode S メッセージを収集し、テスト用データファイルに保存します。

Usage:
    python collect_adsb_data.py [-c CONFIG] [-d DURATION] [-o OUTPUT]

Options:
    -c CONFIG    : 設定ファイルパス [default: config.yaml]
    -d DURATION  : 収集時間（分） [default: 60]
    -o OUTPUT    : 出力ファイルパス [default: tests/fixtures/ads-b.dat]

"""

from __future__ import annotations

import argparse
import pathlib
import socket
import sys
import time

# プロジェクトルートをパスに追加
project_root = pathlib.Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

import my_lib.config  # noqa: E402

from amdar.config import load_from_dict  # noqa: E402


def collect_messages(host: str, port: int, duration_minutes: int, output_path: pathlib.Path) -> int:
    """Mode S メッセージを収集してファイルに保存する

    Args:
        host: dump1090 のホスト
        port: dump1090 のポート
        duration_minutes: 収集時間（分）
        output_path: 出力ファイルパス

    Returns:
        収集したメッセージ数

    """
    duration_seconds = duration_minutes * 60
    start_time = time.time()
    message_count = 0

    print(f"Connecting to {host}:{port}...")
    print(f"Collecting data for {duration_minutes} minutes...")
    print(f"Output: {output_path}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(30.0)
        sock.connect((host, port))
        print("Connected!")

        buffer = b""

        with output_path.open("w") as f:
            while time.time() - start_time < duration_seconds:
                try:
                    data = sock.recv(1024)
                    if not data:
                        print("Connection closed by remote host")
                        break

                    buffer += data

                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        message = line.decode().strip()
                        if message:
                            f.write(message + "\n")
                            message_count += 1

                            # 進捗表示（1000メッセージごと）
                            if message_count % 1000 == 0:
                                elapsed = time.time() - start_time
                                remaining = duration_seconds - elapsed
                                print(
                                    f"  {message_count} messages collected, "
                                    f"{remaining / 60:.1f} minutes remaining..."
                                )

                except TimeoutError:
                    # タイムアウトは正常（データがない期間）
                    continue

    print("\nCollection complete!")
    print(f"Total messages: {message_count}")
    print(f"Output file: {output_path}")
    print(f"File size: {output_path.stat().st_size / 1024:.1f} KB")

    return message_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect ADS-B data for testing")
    parser.add_argument("-c", "--config", default="config.yaml", help="Config file path")
    parser.add_argument("-d", "--duration", type=int, default=60, help="Duration in minutes")
    parser.add_argument("-o", "--output", default="tests/fixtures/ads-b.dat", help="Output file path")
    args = parser.parse_args()

    config_path = pathlib.Path(args.config)
    output_path = pathlib.Path(args.output)

    # 設定ファイルから接続先を取得
    if config_path.exists():
        config_dict = my_lib.config.load(str(config_path))
        config = load_from_dict(config_dict, pathlib.Path.cwd())
        host = config.decoder.modes.host
        port = config.decoder.modes.port
    else:
        print(f"Config file not found: {config_path}")
        print("Using default: localhost:30002")
        host = "localhost"
        port = 30002

    # 出力ディレクトリを作成
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        collect_messages(host, port, args.duration, output_path)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except ConnectionRefusedError:
        print(f"Connection refused: {host}:{port}")
        print("Make sure dump1090 is running")
        sys.exit(1)


if __name__ == "__main__":
    main()
