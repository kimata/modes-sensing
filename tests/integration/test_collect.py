#!/usr/bin/env python3
# ruff: noqa: S101
"""
collect.py の統合テスト

データ収集機能をテストします。
"""

import my_lib.healthz
from my_lib.healthz import HealthzTarget

import collect
import modes.receiver
from modes.config import Config


class TestCollect:
    """データ収集統合テスト"""

    def test_collect_execute(self, config: Config):
        """データ収集の実行をテスト"""
        liveness_file = config.liveness.file.collector

        # 1件だけ処理して終了
        collect.execute(config, liveness_file, 1)

        modes.receiver.term()

        # Livenessファイルが更新されていることを確認
        target = HealthzTarget(name="collector", liveness_file=liveness_file, interval=60)
        assert my_lib.healthz.check_liveness(target)
