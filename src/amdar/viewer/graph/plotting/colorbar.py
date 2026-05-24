"""カラーバーと補間グリッドのヘルパ。"""

from __future__ import annotations

import matplotlib.pyplot
import numpy
import scipy.interpolate

from amdar.constants import GRAPH_ALT_MAX, GRAPH_ALT_MIN, GRAPH_ALTITUDE_LIMIT
from amdar.viewer.graph.plotting.axes import set_tick_label_size
from amdar.viewer.graph.plotting.data_prep import GridData
from amdar.viewer.graph.plotting.styles import AXIS_LABEL_SIZE, TEMP_AXIS_LABEL
from amdar.viewer.graph.range import get_temperature_range


def append_colorbar(
    scatter,
    shrink: float = 0.8,
    pad: float = 0.01,
    aspect: float = 35,
    fraction: float = 0.046,
    limit_altitude: bool = False,
):
    """温度カラーバーをプロットエリアに付加する。"""
    temp_min, temp_max = get_temperature_range(limit_altitude)
    scatter.set_clim(temp_min, temp_max)

    cbar = matplotlib.pyplot.colorbar(scatter, shrink=shrink, pad=pad, aspect=aspect, fraction=fraction)
    cbar.set_label(TEMP_AXIS_LABEL, fontsize=AXIS_LABEL_SIZE)
    set_tick_label_size(cbar.ax)

    return cbar


def create_grid(
    time_numeric: numpy.ndarray,
    altitudes: numpy.ndarray,
    temperatures: numpy.ndarray,
    grid_points: int = 100,
    time_range: tuple[float, float] | None = None,
    limit_altitude: bool = False,
) -> GridData:
    """補間グリッドを作成する（等高線・ヒートマップ用）。"""
    if len(time_numeric) == 0:
        time_min, time_max = 0.0, 1.0
        alt_min = float(GRAPH_ALT_MIN)
        if limit_altitude:
            alt_max = float(GRAPH_ALTITUDE_LIMIT)
            alt_grid_points = int((alt_max - alt_min) / 50) + 1
        else:
            alt_max = float(GRAPH_ALT_MAX)
            alt_grid_points = grid_points

        time_grid = numpy.linspace(time_min, time_max, grid_points)
        alt_grid = numpy.linspace(alt_min, alt_max, alt_grid_points)
        time_mesh, alt_mesh = numpy.meshgrid(time_grid, alt_grid, indexing="xy")
        temp_grid = numpy.full_like(time_mesh, numpy.nan)

        return GridData(
            time_mesh=time_mesh,
            alt_mesh=alt_mesh,
            temp_grid=temp_grid,
            time_min=time_min,
            time_max=time_max,
            alt_min=alt_min,
            alt_max=alt_max,
        )

    if time_range is not None:
        time_min, time_max = time_range
        actual_time_min, actual_time_max = float(time_numeric.min()), float(time_numeric.max())
        time_min = max(time_min, actual_time_min)
        time_max = min(time_max, actual_time_max)
    else:
        time_min, time_max = float(time_numeric.min()), float(time_numeric.max())

    alt_min = float(GRAPH_ALT_MIN)
    if limit_altitude:
        alt_max = float(GRAPH_ALTITUDE_LIMIT)
        # 50m 刻み: 2000m / 50m = 40 点
        alt_grid_points = int((alt_max - alt_min) / 50) + 1
    else:
        alt_max = float(GRAPH_ALT_MAX)
        alt_grid_points = grid_points

    time_grid = numpy.linspace(time_min, time_max, grid_points, dtype=numpy.float64)
    alt_grid = numpy.linspace(alt_min, alt_max, alt_grid_points, dtype=numpy.float64)
    time_mesh, alt_mesh = numpy.meshgrid(time_grid, alt_grid, indexing="xy")

    if time_range is not None:
        range_mask = (time_numeric >= time_min) & (time_numeric <= time_max)
        if not range_mask.any() or len(time_numeric[range_mask]) < 3:
            temp_grid = numpy.full_like(time_mesh, numpy.nan)
        else:
            filtered_time = time_numeric[range_mask]
            filtered_alt = altitudes[range_mask]
            filtered_temp = temperatures[range_mask]
            points = numpy.ascontiguousarray(numpy.column_stack((filtered_time, filtered_alt)))
            temp_values = numpy.ascontiguousarray(filtered_temp)
            temp_grid = scipy.interpolate.griddata(
                points, temp_values, (time_mesh, alt_mesh), method="linear", fill_value=numpy.nan
            )
    elif len(time_numeric) < 3:
        temp_grid = numpy.full_like(time_mesh, numpy.nan)
    else:
        points = numpy.ascontiguousarray(numpy.column_stack((time_numeric, altitudes)))
        temp_values = numpy.ascontiguousarray(temperatures)
        temp_grid = scipy.interpolate.griddata(
            points, temp_values, (time_mesh, alt_mesh), method="linear", fill_value=numpy.nan
        )

    return GridData(
        time_mesh=time_mesh,
        alt_mesh=alt_mesh,
        temp_grid=temp_grid,
        time_min=time_min,
        time_max=time_max,
        alt_min=alt_min,
        alt_max=alt_max,
    )
