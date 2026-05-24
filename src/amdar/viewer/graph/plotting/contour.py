"""等高線プロット（2D / 3D）。"""

from __future__ import annotations

import logging
import time

import matplotlib.dates
import numpy
import PIL.Image

from amdar.viewer.graph.plotting.axes import set_axis_2d_default, set_axis_3d, set_title
from amdar.viewer.graph.plotting.colorbar import append_colorbar, create_grid
from amdar.viewer.graph.plotting.data_prep import PreparedData
from amdar.viewer.graph.plotting.figure import (
    convert_figure_to_image,
    create_3d_figure,
    create_figure,
    setup_3d_layout,
)
from amdar.viewer.graph.plotting.styles import CONTOUR_SIZE
from amdar.viewer.graph.range import get_temperature_range


def _grid_from_data(
    data: PreparedData,
    grid_points: int,
    plot_time_start,
    plot_time_end,
    limit_altitude: bool,
):
    """指定時間範囲（あれば）でグリッドを作成する。"""
    if plot_time_start and plot_time_end:
        plot_time_min = matplotlib.dates.date2num(plot_time_start)
        plot_time_max = matplotlib.dates.date2num(plot_time_end)
        # 実データ範囲を超えないように制限
        if len(data.time_numeric) > 0:
            actual_min = data.time_numeric.min()
            actual_max = data.time_numeric.max()
            plot_time_min = max(plot_time_min, actual_min)
            plot_time_max = min(plot_time_max, actual_max)
        return create_grid(
            data.time_numeric,
            data.altitudes,
            data.temperatures,
            grid_points=grid_points,
            time_range=(plot_time_min, plot_time_max),
            limit_altitude=limit_altitude,
        )
    return create_grid(
        data.time_numeric,
        data.altitudes,
        data.temperatures,
        grid_points=grid_points,
        limit_altitude=limit_altitude,
    )


def plot_contour_2d(
    data: PreparedData,
    figsize: tuple[float, float],
    plot_time_start=None,
    plot_time_end=None,
    limit_altitude: bool = False,
) -> tuple[PIL.Image.Image, float]:
    """2D 等高線プロット。"""
    logging.info("Starting plot contour (limit_altitude: %s)", limit_altitude)
    start = time.perf_counter()

    grid = _grid_from_data(data, 80, plot_time_start, plot_time_end, limit_altitude)

    fig, ax = create_figure(figsize)

    temp_min, temp_max = get_temperature_range(limit_altitude)
    if limit_altitude:
        levels = numpy.arange(temp_min, temp_max + 1, 5)
    else:
        levels = numpy.arange(temp_min, temp_max + 1, 10)

    contour = ax.contour(
        grid.time_mesh, grid.alt_mesh, grid.temp_grid, levels=levels, colors="black", linewidths=0.5
    )
    contourf = ax.contourf(
        grid.time_mesh,
        grid.alt_mesh,
        grid.temp_grid,
        levels=levels,
        cmap="plasma",
        alpha=0.9,
    )

    ax.clabel(contour, inline=True, fontsize=CONTOUR_SIZE, fmt="%d℃")

    if plot_time_start and plot_time_end:
        time_range = [plot_time_start, plot_time_end]
    else:
        time_range = [matplotlib.dates.num2date(grid.time_min), matplotlib.dates.num2date(grid.time_max)]

    set_axis_2d_default(ax, time_range, limit_altitude)
    append_colorbar(contourf, shrink=1.0, pad=0.01, aspect=35, fraction=0.03, limit_altitude=limit_altitude)

    set_title("航空機の気象データ (等高線)")

    img = convert_figure_to_image(fig)

    return (img, time.perf_counter() - start)


def plot_contour_3d(
    data: PreparedData,
    figsize: tuple[float, float],
    limit_altitude: bool = False,
) -> tuple[PIL.Image.Image, float]:
    """3D 等高面プロット。"""
    logging.info("Starting plot contour 3d (limit_altitude: %s)", limit_altitude)
    start = time.perf_counter()

    grid = create_grid(
        data.time_numeric,
        data.altitudes,
        data.temperatures,
        grid_points=60,
        limit_altitude=limit_altitude,
    )

    fig, ax = create_3d_figure(figsize)

    temp_min, temp_max = get_temperature_range(limit_altitude)

    surf = ax.plot_surface(
        grid.time_mesh,
        grid.alt_mesh,
        grid.temp_grid,
        cmap="plasma",
        alpha=0.9,
        antialiased=True,
        rstride=1,
        cstride=1,
        linewidth=0,
        edgecolor="none",
        vmin=temp_min,
        vmax=temp_max,
    )

    levels = numpy.arange(temp_min, temp_max + 1, 10)
    ax.contour(
        grid.time_mesh,
        grid.alt_mesh,
        grid.temp_grid,
        levels=levels,
        colors="black",
        linewidths=0.5,
        alpha=0.3,
        offset=temp_min,  # 底面に等高線を投影
    )

    set_axis_3d(ax, data.time_numeric, limit_altitude)
    append_colorbar(surf, shrink=0.6, pad=0.01, aspect=35, limit_altitude=limit_altitude)
    setup_3d_layout(ax)

    set_title("航空機の気象データ (3D)")

    img = convert_figure_to_image(fig)

    return (img, time.perf_counter() - start)
