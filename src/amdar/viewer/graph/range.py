"""グラフ描画範囲の決定ロジック。"""

from __future__ import annotations

from amdar.constants import (
    GRAPH_TEMP_MAX_DEFAULT,
    GRAPH_TEMP_MAX_LIMITED,
    GRAPH_TEMP_MIN_DEFAULT,
    GRAPH_TEMP_MIN_LIMITED,
)


def get_temperature_range(limit_altitude: bool = False) -> tuple[int, int]:
    """limit_altitude に応じた温度範囲 (min, max) を返す。"""
    if limit_altitude:
        return GRAPH_TEMP_MIN_LIMITED, GRAPH_TEMP_MAX_LIMITED
    return GRAPH_TEMP_MIN_DEFAULT, GRAPH_TEMP_MAX_DEFAULT
