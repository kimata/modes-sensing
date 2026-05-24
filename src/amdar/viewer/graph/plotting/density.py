"""高度-温度の密度プロットおよびヒートマップ。"""

from __future__ import annotations

import logging
import time

import matplotlib.dates
import PIL.Image

from amdar.viewer.graph.plotting.axes import (
    set_altitude_range,
    set_axis_2d_default,
    set_axis_labels,
    set_temperature_range,
    set_tick_label_size,
    set_title,
)
from amdar.viewer.graph.plotting.colorbar import append_colorbar, create_grid
from amdar.viewer.graph.plotting.data_prep import PreparedData
from amdar.viewer.graph.plotting.figure import convert_figure_to_image, create_figure
from amdar.viewer.graph.plotting.styles import ALT_AXIS_LABEL, TEMP_AXIS_LABEL
from amdar.viewer.graph.range import get_temperature_range


def plot_density(
    data: PreparedData,
    figsize: tuple[float, float],
    limit_altitude: bool = False,
) -> tuple[PIL.Image.Image, float]:
    """高度-温度の散布密度プロット。"""
    logging.info("Starting plot density (limit_altitude: %s)", limit_altitude)
    start = time.perf_counter()

    fig, ax = create_figure(figsize)

    scatter = ax.scatter(
        data.altitudes,
        data.temperatures,
        c=data.temperatures,
        cmap="plasma",
        s=15,
        alpha=0.9,
        rasterized=True,
        edgecolors="none",
    )

    set_axis_labels(ax, ALT_AXIS_LABEL, TEMP_AXIS_LABEL)
    set_altitude_range(ax, axis="x", limit_altitude=limit_altitude)
    set_temperature_range(ax, axis="y", limit_altitude=limit_altitude)
    set_tick_label_size(ax)

    append_colorbar(scatter, shrink=1.0, pad=0.01, aspect=35, fraction=0.03, limit_altitude=limit_altitude)

    ax.grid(True, alpha=0.7)

    set_title("航空機の気象データ (高度・温度分布)")

    img = convert_figure_to_image(fig)

    return (img, time.perf_counter() - start)


def plot_heatmap(
    data: PreparedData,
    figsize: tuple[float, float],
    plot_time_start=None,
    plot_time_end=None,
    limit_altitude: bool = False,
) -> tuple[PIL.Image.Image, float]:
    """時間-高度ヒートマップ。"""
    logging.info("Starting plot heatmap (limit_altitude: %s)", limit_altitude)
    start = time.perf_counter()

    if plot_time_start and plot_time_end:
        plot_time_min = matplotlib.dates.date2num(plot_time_start)
        plot_time_max = matplotlib.dates.date2num(plot_time_end)
        if len(data.time_numeric) > 0:
            actual_min = data.time_numeric.min()
            actual_max = data.time_numeric.max()
            plot_time_min = max(plot_time_min, actual_min)
            plot_time_max = min(plot_time_max, actual_max)
        grid = create_grid(
            data.time_numeric,
            data.altitudes,
            data.temperatures,
            grid_points=80,
            time_range=(plot_time_min, plot_time_max),
            limit_altitude=limit_altitude,
        )
    else:
        grid = create_grid(
            data.time_numeric,
            data.altitudes,
            data.temperatures,
            grid_points=80,
            limit_altitude=limit_altitude,
        )

    fig, ax = create_figure(figsize)

    temp_min, temp_max = get_temperature_range(limit_altitude)
    im = ax.imshow(
        grid.temp_grid,
        extent=(grid.time_min, grid.time_max, grid.alt_min, grid.alt_max),
        aspect="auto",
        origin="lower",
        cmap="plasma",
        alpha=0.9,
        vmin=temp_min,
        vmax=temp_max,
    )

    if plot_time_start and plot_time_end:
        time_range = [plot_time_start, plot_time_end]
    else:
        time_range = [matplotlib.dates.num2date(grid.time_min), matplotlib.dates.num2date(grid.time_max)]

    set_axis_2d_default(ax, time_range, limit_altitude)
    append_colorbar(im, shrink=1.0, pad=0.01, aspect=35, fraction=0.03, limit_altitude=limit_altitude)

    set_title("航空機の気象データ (ヒートマップ)")

    img = convert_figure_to_image(fig)

    return (img, time.perf_counter() - start)
