#!/usr/bin/env python3
"""Mode-S データをキャプチャしてファイルに保存する

Usage:
    python capture_modes.py [-o OUTPUT] [-d DURATION] [-h HOST] [-p PORT]

Options:
    -o OUTPUT   : 出力ファイル [default: modes_capture.txt]
    -d DURATION : キャプチャ時間（秒） [default: 10800]  # 3時間
    -h HOST     : Mode-S decoder host [default: proxy.green-rabbit.net]
    -p PORT     : Mode-S decoder port [default: 30002]
"""

import argparse
import logging
import socket
import sys
import time
from pathlib import Path


def capture_modes(
    host: str,
    port: int,
    output_file: Path,
    duration_seconds: int,
) -> int:
    """Mode-S データをキャプチャする

    Args:
        host: デコーダーホスト
        port: デコーダーポート
        output_file: 出力ファイル
        duration_seconds: キャプチャ時間（秒）

    Returns:
        キャプチャしたメッセージ数
    """
    logging.info("Mode-S capture starting: %s:%d -> %s", host, port, output_file)
    logging.info("Duration: %d seconds (%.1f hours)", duration_seconds, duration_seconds / 3600)

    start_time = time.time()
    message_count = 0
    buffer = b""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(30.0)
        sock.connect((host, port))
        logging.info("Connected to %s:%d", host, port)

        with output_file.open("w") as f:
            while time.time() - start_time < duration_seconds:
                try:
                    data = sock.recv(4096)
                    if not data:
                        logging.warning("Connection closed by remote")
                        break

                    buffer += data

                    # 改行でメッセージを分割
                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        message = line.decode().strip()

                        # Mode-S メッセージの形式チェック (*XXXX;)
                        if message.startswith("*") and message.endswith(";"):
                            f.write(message + "\n")
                            message_count += 1

                            if message_count % 10000 == 0:
                                elapsed = time.time() - start_time
                                rate = message_count / elapsed
                                logging.info(
                                    "Progress: %d messages, %.1f msg/s, %.1f%% complete",
                                    message_count,
                                    rate,
                                    (elapsed / duration_seconds) * 100,
                                )
                                f.flush()

                except TimeoutError:
                    logging.warning("Socket timeout, continuing...")
                    continue

    elapsed = time.time() - start_time
    logging.info(
        "Capture complete: %d messages in %.1f seconds (%.1f msg/s)",
        message_count,
        elapsed,
        message_count / elapsed if elapsed > 0 else 0,
    )
    return message_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture Mode-S data")
    parser.add_argument("-o", "--output", default="modes_capture.txt", help="Output file")
    parser.add_argument("-d", "--duration", type=int, default=10800, help="Duration in seconds")
    parser.add_argument("-H", "--host", default="proxy.green-rabbit.net", help="Decoder host")
    parser.add_argument("-p", "--port", type=int, default=30002, help="Decoder port")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    output_path = Path(args.output)
    try:
        capture_modes(args.host, args.port, output_path, args.duration)
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    except Exception as e:
        logging.error("Capture failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
