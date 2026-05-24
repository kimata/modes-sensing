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

        # 30 分間隔でビニングして平均化（0.020833 日 ≒ 30 分）
        if len(sorted_times) > 1:
            bin_size = 0.020833
            unique_times: list[float] = []
            avg_temps: list[float] = []

            current_bin_start = sorted_times[0]
            current_temps: list[float] = []

            for i, time_val in enumerate(sorted_times):
                if time_val <= current_bin_start + bin_size:
                    current_temps.append(sorted_temps[i])
                else:
                    if current_temps:
                        unique_times.append(float(current_bin_start + bin_size / 2))
                        avg_temps.append(float(numpy.mean(current_temps)))
                    current_bin_start = time_val
                    current_temps = [sorted_temps[i]]

            if current_temps:
                unique_times.append(float(current_bin_start + bin_size / 2))
                avg_temps.append(float(numpy.mean(current_temps)))

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
