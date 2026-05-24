#!/usr/bin/env python3
# ruff: noqa: S101
"""グラフ描画範囲 (amdar.viewer.graph.range) のユニットテスト。"""

import logging

import amdar.viewer.graph.range as graph_range


class TestTemperatureRange:
    def test_limited(self):
        temp_min, temp_max = graph_range.get_temperature_range(limit_altitude=True)
        # 高度制限有り: -20°C ～ 40°C
        assert temp_min == -20
        assert temp_max == 40

    def test_unlimited(self):
        temp_min, temp_max = graph_range.get_temperature_range(limit_altitude=False)
        # 高度制限無し: -80°C ～ 30°C
        assert temp_min == -80
        assert temp_max == 30

    def test_logging(self):
        a = graph_range.get_temperature_range(limit_altitude=True)
        b = graph_range.get_temperature_range(limit_altitude=False)
        logging.info("Temperature ranges - Limited: %s, Unlimited: %s", a, b)
