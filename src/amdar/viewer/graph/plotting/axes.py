"""matplotlib 軸まわりの共通ヘルパ。"""

from __future__ import annotations

from typing import Any

import matplotlib.dates
import matplotlib.pyplot
import matplotlib.ticker
import numpy
from matplotlib.axes import Axes

from amdar.constants import GRAPH_ALT_MAX, GRAPH_ALT_MIN, GRAPH_ALTITUDE_LIMIT
from amdar.viewer.graph.plotting.styles import (
    ALT_AXIS_LABEL,
    AXIS_LABEL_SIZE,
    TICK_LABEL_SIZE,
    TIME_AXIS_LABEL,
    TITLE_SIZE,
)
from amdar.viewer.graph.range import get_temperature_range


def set_title(title_text: str) -> None:
    """グラフタイトルを設定する。"""
    matplotlib.pyplot.title(title_text, fontsize=TITLE_SIZE, fontweight="bold", pad=20)


def set_tick_label_size(ax: Axes, is_3d: bool = False) -> None:
    """ティックラベルのサイズを統一する。"""
    ax.tick_params(axis="x", labelsize=TICK_LABEL_SIZE)
    ax.tick_params(axis="y", labelsize=TICK_LABEL_SIZE)
    if is_3d:
        ax.tick_params(axis="z", labelsize=TICK_LABEL_SIZE)  # type: ignore[arg-type]


def set_axis_labels(
    ax: Axes,
    xlabel: str | None = None,
    ylabel: str | None = None,
    zlabel: str | None = None,
) -> None:
    """軸ラベルをまとめて設定する。"""
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=AXIS_LABEL_SIZE)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=AXIS_LABEL_SIZE)
    if zlabel:
        ax.set_zlabel(zlabel, fontsize=AXIS_LABEL_SIZE)  # type: ignore[attr-defined]


def set_temperature_range(ax: Axes, axis: str = "x", limit_altitude: bool = False) -> None:
    """温度軸の範囲を limit_altitude に応じて設定する。"""
    temp_min, temp_max = get_temperature_range(limit_altitude)
    if axis == "x":
        ax.set_xlim(temp_min, temp_max)
    else:
        ax.set_ylim(temp_min, temp_max)


def set_altitude_range(ax: Axes, axis: str = "x", limit_altitude: bool = False) -> None:
    """高度軸の範囲を limit_altitude に応じて設定する。"""
    alt_max = GRAPH_ALTITUDE_LIMIT if limit_altitude else GRAPH_ALT_MAX
    if axis == "x":
        ax.set_xlim(GRAPH_ALT_MIN, alt_max)
    else:
        ax.set_ylim(GRAPH_ALT_MIN, alt_max)


def apply_time_axis_format(ax: Axes, time_range_days: float) -> None:
    """期間に応じて時間軸のフォーマッタを切り替える（2D グラフ用）。"""
    if time_range_days <= 1:
        ax.xaxis.set_major_locator(matplotlib.dates.HourLocator(interval=3))
        ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%-H時"))
    elif time_range_days <= 3:
        ax.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=1))
        ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%-d日\n%-H時"))
    elif time_range_days <= 7:
        ax.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=2))
        ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%-m月%-d日"))
    else:
        ax.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=int(time_range_days / 5)))
        ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%-m月%-d日"))


def apply_time_axis_format_3d(ax: Any, time_numeric: numpy.ndarray) -> None:
    """3D グラフ用の時間軸フォーマット（目盛りを手動配置）。

    3D グラフでは matplotlib.dates の Locator が正しく動作しないため、
    期間に応じて目盛り間隔と位置を手動で設定する。
    """
    if len(time_numeric) == 0:
        return

    time_min = time_numeric.min()
    time_max = time_numeric.max()
    time_range_days = time_max - time_min

    if time_range_days <= 1:
        interval_days = 3 / 24
        date_format = "%-H時"
    elif time_range_days <= 3:
        interval_days = 1
        date_format = "%-d日"
    elif time_range_days <= 7:
        interval_days = 2
        date_format = "%-m月%-d日"
    elif time_range_days <= 30:
        interval_days = max(1, int(time_range_days / 5))
        date_format = "%-m月%-d日"
    elif time_range_days <= 90:
        interval_days = max(7, int(time_range_days / 6))
        date_format = "%-m月%-d日"
    else:
        interval_days = max(14, int(time_range_days / 5))
        date_format = "%-m月%-d日"

    tick_positions: list[float] = []
    tick_labels: list[str] = []
    current = time_min
    while current <= time_max:
        tick_positions.append(current)
        dt = matplotlib.dates.num2date(current)
        tick_labels.append(dt.strftime(date_format))
        current += interval_days

    # 末尾の余りを処理（重複防止）
    if tick_positions and (time_max - tick_positions[-1]) < interval_days * 0.3:
        pass
    elif time_max - tick_positions[-1] > interval_days * 0.5:
        tick_positions.append(time_max)
        dt = matplotlib.dates.num2date(time_max)
        tick_labels.append(dt.strftime(date_format))

    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels)


def set_axis_2d_default(ax: Axes, time_range, limit_altitude: bool = False) -> None:
    """2D グラフの軸を時間-高度のデフォルト構成に設定する。"""
    set_axis_labels(ax, TIME_AXIS_LABEL, ALT_AXIS_LABEL)
    set_altitude_range(ax, axis="y", limit_altitude=limit_altitude)

    if limit_altitude:
        ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(200))
    else:
        ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(2000))

    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    set_tick_label_size(ax)

    apply_time_axis_format(
        ax,
        float(matplotlib.dates.date2num(time_range[-1]) - matplotlib.dates.date2num(time_range[0])),
    )


def set_axis_3d(ax: Any, time_numeric: numpy.ndarray, limit_altitude: bool = False) -> None:
    """3D グラフの軸を時間-高度-温度の構成に設定する。"""
    from amdar.viewer.graph.plotting.styles import TEMP_AXIS_LABEL

    set_axis_labels(ax, TIME_AXIS_LABEL, ALT_AXIS_LABEL, TEMP_AXIS_LABEL)

    apply_time_axis_format_3d(ax, time_numeric)

    alt_max = GRAPH_ALTITUDE_LIMIT if limit_altitude else GRAPH_ALT_MAX

    if limit_altitude:
        ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(200))
    else:
        ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(2000))

    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    set_tick_label_size(ax, is_3d=True)

    ax.set_ylim(GRAPH_ALT_MIN, alt_max)

    temp_min, temp_max = get_temperature_range(limit_altitude)
    ax.set_zlim(temp_min, temp_max)
