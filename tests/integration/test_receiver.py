#!/usr/bin/env python3
# ruff: noqa: S101
"""
receiver.py の統合テスト

Mode S レシーバーの受信機能をテストします。
"""

import queue

import modes.receiver
from modes.config import Config


class TestReceiver:
    """レシーバー統合テスト"""

    def test_receiver_start_and_receive(self, config: Config):
        """レシーバーの起動とデータ受信をテスト"""
        measurement_queue = queue.Queue()

        modes.receiver.start(config, measurement_queue)

        # データを1件受信
        data = measurement_queue.get(timeout=30)
        assert data is not None

        modes.receiver.term()
