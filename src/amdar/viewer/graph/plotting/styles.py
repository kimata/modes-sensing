"""プロット共通のフォント・タイポグラフィ定数。"""

from __future__ import annotations

import logging

import matplotlib.font_manager
import matplotlib.pyplot
import my_lib.panel_config
import my_lib.plot_util

import amdar.config

# ティック / 軸ラベル / タイトルのフォントサイズ
TICK_LABEL_SIZE = 8
CONTOUR_SIZE = 8
ERROR_SIZE = 30
AXIS_LABEL_SIZE = 12
TITLE_SIZE = 20

TIME_AXIS_LABEL = "日時"
ALT_AXIS_LABEL = "高度 (m)"
TEMP_AXIS_LABEL = "温度 (℃)"


def to_panel_font_config(font_config: amdar.config.FontConfig) -> my_lib.panel_config.FontConfig:
    """FontConfig を my_lib.panel_config.FontConfig に変換する。"""
    return my_lib.panel_config.FontConfig(
        path=font_config.path,
        map=font_config.map,
    )


def set_font(font_config_src: amdar.config.FontConfig) -> None:
    """matplotlib のデフォルトフォントを日本語対応に設定する。"""
    try:
        font_config = to_panel_font_config(font_config_src)

        for font_file in font_config.map.values():
            matplotlib.font_manager.fontManager.addfont(font_config.path.resolve() / font_file)

        font_name = my_lib.plot_util.get_plot_font(font_config, "jp_medium", 12).get_name()

        matplotlib.pyplot.rcParams["font.family"] = [font_name, "sans-serif"]
        matplotlib.pyplot.rcParams["font.sans-serif"] = [font_name] + matplotlib.pyplot.rcParams[
            "font.sans-serif"
        ]
    except Exception:
        logging.exception("Failed to set font")
