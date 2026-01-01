#!/usr/bin/env python3
# ruff: noqa: S101
"""
graph.py のユニットテスト

純粋な関数のテストを行います。
"""
import logging

import modes.webui.api.graph


class TestTemperatureRange:
    """温度範囲関数のテスト"""

    def test_temperature_range_limited(self):
        """高度制限ありの温度範囲をテスト"""
        temp_min, temp_max = modes.webui.api.graph.get_temperature_range(limit_altitude=True)

        # 高度制限有り: -20°C～40°C
        assert temp_min == -20
        assert temp_max == 40

    def test_temperature_range_unlimited(self):
        """高度制限なしの温度範囲をテスト"""
        temp_min, temp_max = modes.webui.api.graph.get_temperature_range(limit_altitude=False)

        # 高度制限無し: -80°C～30°C
        assert temp_min == -80
        assert temp_max == 30

    def test_temperature_range_logging(self):
        """温度範囲のログ出力を確認"""
        temp_min_limited, temp_max_limited = modes.webui.api.graph.get_temperature_range(
            limit_altitude=True
        )
        temp_min_unlimited, temp_max_unlimited = modes.webui.api.graph.get_temperature_range(
            limit_altitude=False
        )

        logging.info(
            "Temperature ranges - Limited: %d°C～%d°C, Unlimited: %d°C～%d°C",
            temp_min_limited,
            temp_max_limited,
            temp_min_unlimited,
            temp_max_unlimited,
        )
