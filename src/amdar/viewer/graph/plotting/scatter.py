"""散布図プロット（2D / 3D）。"""

from __future__ import annotations

import logging
import time

import matplotlib.dates
import PIL.Image

from amdar.viewer.graph.plotting.axes import set_axis_2d_default, set_axis_3d, set_title
from amdar.viewer.graph.plotting.colorbar import append_colorbar
from amdar.viewer.graph.plotting.data_prep import PreparedData
from amdar.viewer.graph.plotting.figure import (
    convert_figure_to_image,
    create_3d_figure,
    create_figure,
    setup_3d_layout,
)


def plot_scatter_2d(
    data: PreparedData,
    figsize: tuple[float, float],
    limit_altitude: bool = False,
) -> tuple[PIL.Image.Image, float]:
    """時間-高度-温度の 2D 散布図。"""
    logging.info("Starting plot 2d scatter (limit_altitude: %s)", limit_altitude)
    start = time.perf_counter()

    fig, ax = create_figure(figsize)

    sc = ax.scatter(
        data.times,
        data.altitudes,
        c=data.temperatures,
        cmap="plasma",
        s=15,
        alpha=0.9,
        rasterized=True,
        edgecolors="none",
    )

    set_axis_2d_default(
        ax,
        [
            matplotlib.dates.num2date(data.time_numeric.min()),
            matplotlib.dates.num2date(data.time_numeric.max()),
        ],
        limit_altitude,
    )

    append_colorbar(sc, shrink=1.0, pad=0.01, aspect=35, fraction=0.03, limit_altitude=limit_altitude)

    ax.grid(True, alpha=0.7)

    set_title("航空機の気象データ")

    img = convert_figure_to_image(fig)

    return (img, time.perf_counter() - start)


def plot_scatter_3d(
    data: PreparedData,
    figsize: tuple[float, float],
    limit_altitude: bool = False,
) -> tuple[PIL.Image.Image, float]:
    """3D 散布図。"""
    logging.info("Starting plot scatter 3d (limit_altitude: %s)", limit_altitude)
    start = time.perf_counter()

    fig, ax = create_3d_figure(figsize)
    scatter = ax.scatter(
        data.time_numeric,
        data.altitudes,
        data.temperatures,
        c=data.temperatures,
        cmap="plasma",
        marker="o",
        s=15,
        alpha=0.9,
        rasterized=True,
        edgecolors="none",
    )

    set_axis_3d(ax, data.time_numeric, limit_altitude)
    append_colorbar(scatter, shrink=0.6, pad=0.01, aspect=35, limit_altitude=limit_altitude)
    setup_3d_layout(ax)

    set_title("航空機の気象データ (3D)")

    img = convert_figure_to_image(fig)

    return (img, time.perf_counter() - start)
