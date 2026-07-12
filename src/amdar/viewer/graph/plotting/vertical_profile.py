"""大気の鉛直プロファイル（気温プロファイル + ホドグラフ）のプロット。

要求期間の末尾 :data:`amdar.constants.VERTICAL_PROFILE_WINDOW_HOURS` 時間分の
データを使い、ある時点の大気の鉛直構造をスナップショットとして描画する。

- 左パネル: 気温の鉛直プロファイル（観測点 + 250m ビン中央値 + 標準大気減率）
- 右パネル: ホドグラフ（250m ビン中央値の風ベクトル軌跡、高度で色付け）
"""

from __future__ import annotations

import datetime
import logging
import time

import matplotlib.collections
import matplotlib.colors
import matplotlib.dates
import matplotlib.patches
import matplotlib.pyplot
import my_lib.time
import numpy
import PIL.Image
from matplotlib.axes import Axes

from amdar.constants import (
    AGGREGATE_ALTITUDE_BIN_METERS,
    GRAPH_ALT_MAX,
    GRAPH_ALT_MIN,
    GRAPH_ALTITUDE_LIMIT,
    VERTICAL_PROFILE_WINDOW_HOURS,
)
from amdar.viewer.graph.plotting.axes import (
    set_altitude_range,
    set_axis_labels,
    set_temperature_range,
    set_tick_label_size,
)
from amdar.viewer.graph.plotting.data_prep import PreparedData
from amdar.viewer.graph.plotting.figure import convert_figure_to_image
from amdar.viewer.graph.plotting.styles import (
    ALT_AXIS_LABEL,
    AXIS_LABEL_SIZE,
    ERROR_SIZE,
    TEMP_AXIS_LABEL,
    TICK_LABEL_SIZE,
    TITLE_SIZE,
)

# パネル毎にデータ不足とみなす点数の閾値
_MIN_PANEL_POINTS = 10

# 標準大気の気温減率（℃/km）
_STANDARD_LAPSE_RATE_C_PER_KM = 6.5

# ホドグラフの同心円（風速、m/s）
_HODOGRAPH_SPEED_CIRCLES_MS = (10, 20, 30)

# 有効な風とみなす最小風速（m/s）。wind.py と同じ閾値
_MIN_WIND_SPEED_MS = 0.1


def _to_wall_time_num(dt: datetime.datetime) -> float:
    """aware datetime を JST 壁時計基準の matplotlib date number に変換する。

    PreparedData.time_numeric は JST 壁時計時刻（naive）から計算されているため、
    比較にはタイムゾーンを落とした壁時計時刻を使う必要がある。
    """
    wall = dt.astimezone(my_lib.time.get_zoneinfo()).replace(tzinfo=None) if dt.tzinfo else dt
    return float(matplotlib.dates.date2num(wall))


def _bin_median_by_altitude(
    altitudes: numpy.ndarray,
    value_arrays: list[numpy.ndarray],
) -> tuple[numpy.ndarray, list[numpy.ndarray]]:
    """250m 高度ビンごとの中央値を計算する。

    Returns:
        (ビン中心高度の昇順配列, 各 value 配列に対応する中央値配列のリスト)
    """
    bin_indices = numpy.floor_divide(altitudes, AGGREGATE_ALTITUDE_BIN_METERS).astype(int)
    unique_bins = numpy.unique(bin_indices)
    centers = (unique_bins + 0.5) * AGGREGATE_ALTITUDE_BIN_METERS

    medians = [
        numpy.array([float(numpy.median(values[bin_indices == b])) for b in unique_bins])
        for values in value_arrays
    ]
    return centers, medians


def _draw_insufficient_data(ax: Axes) -> None:
    """パネル中央に「データ不足」を表示する。"""
    ax.text(
        0.5,
        0.5,
        "データ不足",
        transform=ax.transAxes,
        ha="center",
        va="center",
        fontsize=ERROR_SIZE,
        color="#666",
    )


def _draw_temperature_profile(
    ax: Axes,
    altitudes: numpy.ndarray,
    temperatures: numpy.ndarray,
    limit_altitude: bool,
) -> None:
    """左パネル: 気温の鉛直プロファイルを描画する。"""
    alt_max = GRAPH_ALTITUDE_LIMIT if limit_altitude else GRAPH_ALT_MAX

    set_axis_labels(ax, TEMP_AXIS_LABEL, ALT_AXIS_LABEL)
    set_temperature_range(ax, axis="x", limit_altitude=limit_altitude)
    set_altitude_range(ax, axis="y", limit_altitude=limit_altitude)
    set_tick_label_size(ax)
    ax.grid(True, alpha=0.7)
    ax.set_title("気温プロファイル", fontsize=AXIS_LABEL_SIZE)

    if len(temperatures) < _MIN_PANEL_POINTS:
        _draw_insufficient_data(ax)
        return

    # 観測点（淡い散布）
    ax.scatter(
        temperatures,
        altitudes,
        s=10,
        alpha=0.2,
        color="tab:blue",
        rasterized=True,
        edgecolors="none",
        label="観測値",
    )

    # 250m ビン中央値の折れ線
    centers, (median_temps,) = _bin_median_by_altitude(altitudes, [temperatures])
    ax.plot(
        median_temps,
        centers,
        color="tab:blue",
        linewidth=2.5,
        marker="o",
        markersize=4,
        label=f"{AGGREGATE_ALTITUDE_BIN_METERS}m ビン中央値",
    )

    # 標準大気の気温減率（地上気温は最下層ビンの中央値から推定）
    surface_temp = float(median_temps[0]) + _STANDARD_LAPSE_RATE_C_PER_KM * float(centers[0]) / 1000
    ref_altitudes = numpy.linspace(GRAPH_ALT_MIN, alt_max, 50)
    ref_temps = surface_temp - _STANDARD_LAPSE_RATE_C_PER_KM * ref_altitudes / 1000
    ax.plot(
        ref_temps,
        ref_altitudes,
        linestyle="--",
        color="gray",
        linewidth=1.5,
        label=f"標準大気 (-{_STANDARD_LAPSE_RATE_C_PER_KM}℃/km)",
    )

    ax.legend(loc="upper right", framealpha=0.9, fontsize=TICK_LABEL_SIZE)


def _extract_hodograph_wind(
    data: PreparedData,
    window_start_num: float,
    window_end_num: float,
    limit_altitude: bool,
) -> tuple[numpy.ndarray, numpy.ndarray, numpy.ndarray]:
    """ホドグラフ用の (高度, wind_x, wind_y) を抽出する。

    風データは wind.py と同様に DataFrame 経由で取得する
    （生データ経路では PreparedData の wind 配列が空のため）。
    """
    df = data.dataframe
    if len(df) == 0 or "wind_x" not in df.columns or "wind_y" not in df.columns:
        empty = numpy.array([], dtype=numpy.float64)
        return empty, empty, empty

    if "time_numeric" in df.columns:
        time_numeric = df["time_numeric"].to_numpy(dtype=numpy.float64)
    else:
        time_numeric = matplotlib.dates.date2num(df["time"].to_numpy())

    altitudes = df["altitude"].to_numpy(dtype=numpy.float64)
    wind_x = df["wind_x"].to_numpy(dtype=numpy.float64)
    wind_y = df["wind_y"].to_numpy(dtype=numpy.float64)

    wind_speed = numpy.sqrt(wind_x**2 + wind_y**2)
    mask = (
        (time_numeric >= window_start_num)
        & (time_numeric <= window_end_num)
        & numpy.isfinite(wind_x)
        & numpy.isfinite(wind_y)
        & (wind_speed > _MIN_WIND_SPEED_MS)
    )
    if limit_altitude:
        mask &= altitudes <= GRAPH_ALTITUDE_LIMIT

    return altitudes[mask], wind_x[mask], wind_y[mask]


def _draw_hodograph(
    fig,
    ax: Axes,
    altitudes: numpy.ndarray,
    wind_x: numpy.ndarray,
    wind_y: numpy.ndarray,
    limit_altitude: bool,
) -> None:
    """右パネル: ホドグラフを描画する。"""
    alt_max = GRAPH_ALTITUDE_LIMIT if limit_altitude else GRAPH_ALT_MAX

    set_axis_labels(ax, "東西風 wind_x (m/s)", "南北風 wind_y (m/s)")
    set_tick_label_size(ax)
    ax.set_aspect("equal")
    ax.set_title("ホドグラフ", fontsize=AXIS_LABEL_SIZE)

    # 同心円グリッド（10/20/30 m/s）と十字線
    for radius in _HODOGRAPH_SPEED_CIRCLES_MS:
        ax.add_patch(
            matplotlib.patches.Circle(
                (0, 0), radius, fill=False, color="gray", linestyle="--", linewidth=0.8, alpha=0.7
            )
        )
        ax.annotate(
            f"{radius} m/s",
            xy=(0, radius),
            xytext=(0, 2),
            textcoords="offset points",
            ha="center",
            fontsize=TICK_LABEL_SIZE,
            color="gray",
        )
    ax.axhline(0, color="gray", linewidth=0.5, alpha=0.5)
    ax.axvline(0, color="gray", linewidth=0.5, alpha=0.5)

    if len(altitudes) < _MIN_PANEL_POINTS:
        axis_limit = max(_HODOGRAPH_SPEED_CIRCLES_MS) * 1.2
        ax.set_xlim(-axis_limit, axis_limit)
        ax.set_ylim(-axis_limit, axis_limit)
        _draw_insufficient_data(ax)
        return

    # 250m ビン中央値（高度昇順）を線で結ぶ
    centers, (median_x, median_y) = _bin_median_by_altitude(altitudes, [wind_x, wind_y])

    norm = matplotlib.colors.Normalize(vmin=GRAPH_ALT_MIN, vmax=alt_max)

    if len(centers) >= 2:
        points = numpy.column_stack([median_x, median_y])
        segments = list(numpy.stack([points[:-1], points[1:]], axis=1))
        line_collection = matplotlib.collections.LineCollection(
            segments,
            cmap="viridis",
            norm=norm,
            array=(centers[:-1] + centers[1:]) / 2,
            linewidth=2,
            zorder=2,
        )
        ax.add_collection(line_collection)

    scatter = ax.scatter(
        median_x,
        median_y,
        c=centers,
        cmap="viridis",
        norm=norm,
        s=35,
        zorder=3,
        edgecolors="white",
        linewidths=0.5,
    )

    max_component = float(numpy.max(numpy.abs(numpy.concatenate([median_x, median_y]))))
    axis_limit = max(max(_HODOGRAPH_SPEED_CIRCLES_MS) * 1.2, max_component * 1.15)
    ax.set_xlim(-axis_limit, axis_limit)
    ax.set_ylim(-axis_limit, axis_limit)

    cbar = fig.colorbar(scatter, ax=ax, shrink=0.9, pad=0.02, aspect=35)
    cbar.set_label(ALT_AXIS_LABEL, fontsize=AXIS_LABEL_SIZE)
    set_tick_label_size(cbar.ax)


def plot_vertical_profile(
    data: PreparedData,
    figsize: tuple[float, float],
    plot_time_start: datetime.datetime | None = None,
    plot_time_end: datetime.datetime | None = None,
    limit_altitude: bool = False,
) -> tuple[PIL.Image.Image, float]:
    """気温の鉛直プロファイルとホドグラフの 2 パネルプロット。

    要求期間の末尾 VERTICAL_PROFILE_WINDOW_HOURS 時間分のデータのみを使う。
    plot_time_start / plot_time_end が未指定の場合はデータの最新時刻を終端とする。
    """
    logging.info("Starting plot vertical profile (limit_altitude: %s)", limit_altitude)
    start = time.perf_counter()

    if data.count == 0:
        raise ValueError("No data available for vertical profile")

    # 末尾ウィンドウ [end - 3h, end] を決定する（JST 壁時計基準）
    window_end_num = _to_wall_time_num(plot_time_end) if plot_time_end else float(data.time_numeric.max())
    window_start_num = window_end_num - VERTICAL_PROFILE_WINDOW_HOURS / 24
    if plot_time_start:
        window_start_num = max(window_start_num, _to_wall_time_num(plot_time_start))

    window_mask = (data.time_numeric >= window_start_num) & (data.time_numeric <= window_end_num)
    temp_mask = window_mask & (data.altitudes <= GRAPH_ALTITUDE_LIMIT) if limit_altitude else window_mask

    fig, (ax_temp, ax_hodo) = matplotlib.pyplot.subplots(1, 2, figsize=figsize)
    fig.subplots_adjust(left=0.07, bottom=0.09, right=0.97, top=0.86, wspace=0.2)

    _draw_temperature_profile(
        ax_temp, data.altitudes[temp_mask], data.temperatures[temp_mask], limit_altitude
    )

    wind_altitudes, wind_x, wind_y = _extract_hodograph_wind(
        data, window_start_num, window_end_num, limit_altitude
    )
    _draw_hodograph(fig, ax_hodo, wind_altitudes, wind_x, wind_y, limit_altitude)

    # 対象時間帯（JST）をタイトルに明記する
    window_start_dt = matplotlib.dates.num2date(window_start_num)
    window_end_dt = matplotlib.dates.num2date(window_end_num)
    fig.suptitle(
        "大気鉛直プロファイル "
        f"({window_start_dt.strftime('%-m月%-d日 %-H:%M')}"
        f"〜{window_end_dt.strftime('%-m月%-d日 %-H:%M')} JST)",
        fontsize=TITLE_SIZE,
        fontweight="bold",
    )

    img = convert_figure_to_image(fig)

    return (img, time.perf_counter() - start)
