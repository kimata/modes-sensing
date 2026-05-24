"""風向・風速の高度別プロット。"""

from __future__ import annotations

import logging
import time
from typing import Any

import matplotlib.dates
import matplotlib.pyplot
import numpy
import pandas
import PIL.Image

from amdar.constants import GRAPH_ALT_MAX, GRAPH_ALT_MIN, GRAPH_ALTITUDE_LIMIT
from amdar.viewer.graph.plotting.axes import set_axis_2d_default, set_tick_label_size, set_title
from amdar.viewer.graph.plotting.data_prep import PreparedData, WindFilteredData
from amdar.viewer.graph.plotting.figure import convert_figure_to_image, create_figure
from amdar.viewer.graph.plotting.styles import AXIS_LABEL_SIZE


def _validate_wind_dataframe(data: PreparedData) -> pandas.DataFrame:
    """風データの DataFrame を検証する。"""
    if len(data.dataframe) == 0:
        logging.warning("Wind data not available for wind direction plot")
        raise ValueError("Wind data not available")

    df = data.dataframe
    required_columns = ["time", "altitude", "wind_x", "wind_y", "wind_speed", "wind_angle"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logging.warning("Missing wind data columns: %s", missing_columns)
        logging.warning("Available columns: %s", list(df.columns))
        msg = f"Missing wind data columns: {missing_columns}"
        raise ValueError(msg)

    return df


def _extract_and_filter_wind_data(df: pandas.DataFrame, limit_altitude: bool = False) -> WindFilteredData:
    """風データを抽出してフィルタリングする。"""
    altitudes = df["altitude"].to_numpy()
    wind_x = df["wind_x"].to_numpy()
    wind_y = df["wind_y"].to_numpy()

    if "time_numeric" in df.columns:
        time_numeric = df["time_numeric"].to_numpy()
    else:
        time_numeric = matplotlib.dates.date2num(df["time"].to_numpy())

    # 無風データを事前除外
    wind_speed = numpy.sqrt(wind_x**2 + wind_y**2)
    valid_wind_mask = wind_speed > 0.1

    if limit_altitude:
        altitude_mask = altitudes <= GRAPH_ALTITUDE_LIMIT
        valid_wind_mask = valid_wind_mask & altitude_mask

    if not valid_wind_mask.any():
        logging.warning(
            "No valid wind vectors after filtering (speed: %s, limit_altitude: %s)",
            (wind_speed > 0.1).sum(),
            limit_altitude,
        )
        raise ValueError("No valid wind vectors after filtering")

    return WindFilteredData(
        altitudes=altitudes[valid_wind_mask],
        wind_x=wind_x[valid_wind_mask],
        wind_y=wind_y[valid_wind_mask],
        time_numeric=time_numeric[valid_wind_mask],
    )


def _prepare_wind_data(data: PreparedData, limit_altitude: bool = False) -> pandas.DataFrame:
    """高度・時間でビニングし、平均風速・風向を計算する。"""
    df = _validate_wind_dataframe(data)
    valid_data = _extract_and_filter_wind_data(df, limit_altitude)

    valid_altitudes = valid_data.altitudes
    valid_time_numeric = valid_data.time_numeric
    valid_wind_x = valid_data.wind_x
    valid_wind_y = valid_data.wind_y

    if limit_altitude:
        altitude_bins = numpy.arange(0, GRAPH_ALTITUDE_LIMIT + 100, 100)
    else:
        altitude_bins = numpy.arange(0, 13000, 200)

    altitude_bin_indices = numpy.searchsorted(altitude_bins, valid_altitudes, side="right") - 1
    altitude_bin_indices = numpy.clip(altitude_bin_indices, 0, len(altitude_bins) - 2)

    time_range = valid_time_numeric.max() - valid_time_numeric.min()
    if time_range <= 1:
        time_bins = 48  # 30 分間隔
    elif time_range <= 3:
        time_bins = 24  # 3 時間間隔
    else:
        time_bins = int(time_range * 4)  # 6 時間間隔

    time_bin_edges = numpy.linspace(valid_time_numeric.min(), valid_time_numeric.max(), time_bins + 1)
    time_bin_indices = numpy.searchsorted(time_bin_edges, valid_time_numeric, side="right") - 1
    time_bin_indices = numpy.clip(time_bin_indices, 0, time_bins - 1)

    bin_df = pandas.DataFrame(
        {
            "time_bin": time_bin_indices,
            "alt_bin_idx": altitude_bin_indices,
            "wind_x": valid_wind_x,
            "wind_y": valid_wind_y,
            "time_numeric": valid_time_numeric,
        }
    )

    grouped: Any = bin_df.groupby(["time_bin", "alt_bin_idx"], as_index=False).agg(
        {
            "wind_x": "mean",
            "wind_y": "mean",
            "time_numeric": "mean",
        }
    )

    if len(grouped) == 0:
        logging.warning("No valid wind data after binning")
        raise ValueError("No valid wind data after binning")

    alt_indices: Any = grouped["alt_bin_idx"].values
    grouped["altitude_bin"] = altitude_bins[alt_indices]

    wind_x: Any = grouped["wind_x"]
    wind_y: Any = grouped["wind_y"]
    grouped["wind_speed"] = numpy.sqrt(wind_x**2 + wind_y**2)
    grouped["wind_angle"] = (90 - numpy.degrees(numpy.arctan2(wind_y, wind_x))) % 360

    return grouped.dropna()


def plot_wind_direction(
    data: PreparedData,
    figsize: tuple[float, float],
    limit_altitude: bool = False,
) -> tuple[PIL.Image.Image, float]:
    """高度別の風向・風速プロット（quiver）。"""
    logging.info("Starting plot wind direction (limit_altitude: %s)", limit_altitude)
    start = time.perf_counter()

    if len(data.dataframe) > 0:
        df = data.dataframe
        logging.info("Available columns in dataframe: %s", list(df.columns))
        logging.info("Dataframe shape: %s", df.shape)

    grouped = _prepare_wind_data(data, limit_altitude)

    if len(grouped) == 0:
        logging.warning("No valid wind vectors after angle conversion")
        raise ValueError("No valid wind vectors after angle conversion")

    fig, ax = create_figure(figsize)

    # 軸の範囲を先に確定させてからアスペクト比を計算する必要がある
    time_min: float = float(grouped["time_numeric"].min())
    time_max: float = float(grouped["time_numeric"].max())
    alt_max = GRAPH_ALTITUDE_LIMIT if limit_altitude else GRAPH_ALT_MAX
    ax.set_xlim(time_min, time_max)
    ax.set_ylim(GRAPH_ALT_MIN, alt_max)

    fig.canvas.draw()

    # データ単位ベクトル (1, 0) / (0, 1) がピクセル空間で何ピクセルになるか計測
    transform = ax.transData
    origin = transform.transform((time_min, GRAPH_ALT_MIN))
    x_unit = transform.transform((time_min + 1, GRAPH_ALT_MIN))
    y_unit = transform.transform((time_min, GRAPH_ALT_MIN + 1))

    pixels_per_day = numpy.linalg.norm(x_unit - origin)
    pixels_per_meter = numpy.linalg.norm(y_unit - origin)

    # 北風（wind_x=0, wind_y<0）が下向きに見えるよう補正
    aspect_correction = pixels_per_day / pixels_per_meter if pixels_per_meter > 0 else 1

    time_range = time_max - time_min
    arrow_scale = time_range / 30

    gwind_x: Any = grouped["wind_x"]
    gwind_y: Any = grouped["wind_y"]
    wind_magnitude = numpy.sqrt(gwind_x**2 + gwind_y**2)
    # wind_x / wind_y は風が吹いていく方向。矢印もその方向を指す
    grouped["u_normalized"] = (gwind_x / wind_magnitude) * arrow_scale
    grouped["v_normalized"] = (gwind_y / wind_magnitude) * arrow_scale * aspect_correction
    wind_speeds: Any = grouped["wind_speed"].values
    wind_speeds_clipped = numpy.clip(wind_speeds, 0, 100)

    quiver = ax.quiver(
        grouped["time_numeric"],
        grouped["altitude_bin"],
        grouped["u_normalized"],
        grouped["v_normalized"],
        wind_speeds_clipped,
        cmap="plasma",
        scale=1,
        scale_units="xy",
        angles="xy",
        alpha=0.9,
        width=0.002,
        headwidth=3,
        headlength=5,
        minlength=0,
        pivot="middle",
    )

    quiver.set_clim(0, 100)

    set_axis_2d_default(
        ax,
        [
            matplotlib.dates.num2date(grouped["time_numeric"].min()),
            matplotlib.dates.num2date(grouped["time_numeric"].max()),
        ],
        limit_altitude,
    )

    cbar = matplotlib.pyplot.colorbar(quiver, shrink=0.8, pad=0.01, aspect=35, fraction=0.046)
    cbar.set_label("風速 (m/s)", fontsize=AXIS_LABEL_SIZE)
    set_tick_label_size(cbar.ax)

    set_title("航空機観測による風向・風速分布")

    img = convert_figure_to_image(fig)
    return (img, time.perf_counter() - start)
