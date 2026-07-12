"""高度別温度の時系列プロット。"""

from __future__ import annotations

import logging
import time

import numpy
import PIL.Image

from amdar.viewer.graph.plotting.axes import apply_time_axis_format, set_title
from amdar.viewer.graph.plotting.data_prep import PreparedData
from amdar.viewer.graph.plotting.figure import convert_figure_to_image, create_figure
from amdar.viewer.graph.range import get_temperature_range

# 30 分間隔のビン幅（matplotlib の日数単位、0.020833 日 ≒ 30 分）
_BIN_SIZE_DAYS = 0.020833


def bin_time_series(
    sorted_times: numpy.ndarray,
    sorted_temps: numpy.ndarray,
    bin_size: float = _BIN_SIZE_DAYS,
) -> tuple[numpy.ndarray, numpy.ndarray]:
    """時刻昇順のデータを可変アンカーのビンで平均化する。

    先頭の時刻をアンカーとして ``bin_size`` 以内の点を平均し、超えた点を
    次のアンカーとして繰り返す（データの隙間でビンが再アンカーされる）。
    numpy.searchsorted によりビン数分のループで処理する。

    Returns:
        (ビン中心時刻の配列, 平均温度の配列)
    """
    n = len(sorted_times)
    if n == 0:
        return numpy.array([]), numpy.array([])

    # 平均を O(1) で求めるための累積和（先頭に 0 を置く）
    temp_cumsum = numpy.concatenate(([0.0], numpy.cumsum(sorted_temps, dtype=numpy.float64)))

    bin_centers: list[float] = []
    bin_means: list[float] = []

    index = 0
    while index < n:
        anchor = sorted_times[index]
        # anchor + bin_size «以下» の点を同一ビンに含める（side="right"）
        end = int(numpy.searchsorted(sorted_times, anchor + bin_size, side="right"))
        bin_centers.append(float(anchor + bin_size / 2))
        bin_means.append(float((temp_cumsum[end] - temp_cumsum[index]) / (end - index)))
        index = end

    return numpy.array(bin_centers), numpy.array(bin_means)


def plot_temperature(
    data: PreparedData,
    figsize: tuple[float, float],
    limit_altitude: bool = False,
) -> tuple[PIL.Image.Image, float]:
    """高度範囲別の温度時系列プロット。"""
    logging.info("Starting plot temperature timeseries (limit_altitude: %s)", limit_altitude)
    start = time.perf_counter()

    fig, ax = create_figure(figsize)

    if limit_altitude:
        altitude_ranges = [
            {"min": 400, "max": 600, "label": "500±100m", "color": "blue", "marker": "o"},
            {"min": 900, "max": 1100, "label": "1000±100m", "color": "green", "marker": "s"},
            {"min": 1400, "max": 1600, "label": "1500±100m", "color": "orange", "marker": "^"},
        ]
    else:
        altitude_ranges = [
            {"min": 1400, "max": 1600, "label": "1500±100m", "color": "blue", "marker": "o"},
            {"min": 2900, "max": 3100, "label": "3000±100m", "color": "green", "marker": "s"},
            {"min": 4400, "max": 4600, "label": "4500±100m", "color": "orange", "marker": "^"},
            {"min": 5900, "max": 6100, "label": "6000±100m", "color": "red", "marker": "d"},
        ]

    for alt_range in altitude_ranges:
        mask = (data.altitudes >= alt_range["min"]) & (data.altitudes <= alt_range["max"])
        if not numpy.any(mask):
            continue

        filtered_temps = data.temperatures[mask]
        filtered_time_numeric = data.time_numeric[mask]

        sort_indices = numpy.argsort(filtered_time_numeric)
        sorted_times = filtered_time_numeric[sort_indices]
        sorted_temps = filtered_temps[sort_indices]

        # 30 分間隔でビニングして平均化
        if len(sorted_times) > 1:
            unique_times, avg_temps = bin_time_series(sorted_times, sorted_temps)

            ax.plot(
                unique_times,
                avg_temps,
                color=alt_range["color"],
                marker=alt_range["marker"],
                markersize=4,
                linewidth=2,
                label=alt_range["label"],
                alpha=0.8,
            )

    ax.set_xlabel("日時")
    ax.set_ylabel("温度 (℃)")
    ax.grid(True, alpha=0.7)

    time_range = data.time_numeric.max() - data.time_numeric.min()
    apply_time_axis_format(ax, time_range)

    temp_min, temp_max = get_temperature_range(limit_altitude)
    ax.set_ylim(temp_min, temp_max)

    ax.legend(loc="upper right", framealpha=0.9)

    set_title("高度別温度の時系列変化")

    img = convert_figure_to_image(fig)

    return (img, time.perf_counter() - start)
