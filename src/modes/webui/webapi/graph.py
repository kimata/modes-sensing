#!/usr/bin/env python3
"""
気象データをグラフにプロットします．

Usage:
  graph.py [-c CONFIG] [-p DAYS] [-D]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
  -p DAYS           : グラフ化する対象区間(日数)を指定します。[default: 7]
  -D                : デバッグモードで動作します。
"""

import concurrent.futures
import datetime
import io
import logging
import pathlib
import time

import matplotlib.dates
import matplotlib.font_manager
import matplotlib.pyplot  # noqa: ICN001
import matplotlib.ticker
import mpl_toolkits.mplot3d  # noqa: F401
import my_lib.plot_util
import numpy  # noqa: ICN001
import pandas  # noqa: ICN001
import PIL.Image
import scipy.interpolate

matplotlib.use("Agg")
IMAGE_DPI = 200.0

TEMPERATURE_THRESHOLD = -100
TEMP_MIN = -80
TEMP_MAX = 30
ALT_MIN = 0
ALT_MAX = 14000
TICK_LABEL_SIZE = 8
CONTOUR_SIZE = 8

AXIS_LABEL_SIZE = 12
TITLE_SIZE = 20

# 軸ラベル定数
TIME_AXIS_LABEL = "日時"
ALT_AXIS_LABEL = "高度 (m)"
TEMP_AXIS_LABEL = "温度 (℃)"


def set_title(title_text):
    matplotlib.pyplot.title(title_text, fontsize=TITLE_SIZE, fontweight="bold", pad=16)


def set_tick_label_size(ax, is_3d=False):
    ax.tick_params(axis="x", labelsize=TICK_LABEL_SIZE)
    ax.tick_params(axis="y", labelsize=TICK_LABEL_SIZE)
    if is_3d:
        ax.tick_params(axis="z", labelsize=TICK_LABEL_SIZE)


def set_axis_labels(ax, xlabel=None, ylabel=None, zlabel=None):
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=AXIS_LABEL_SIZE)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=AXIS_LABEL_SIZE)
    if zlabel:
        ax.set_zlabel(zlabel, fontsize=AXIS_LABEL_SIZE)


def set_temperature_range(ax, axis="x"):
    if axis == "x":
        ax.set_xlim(TEMP_MIN, TEMP_MAX)
    else:
        ax.set_ylim(TEMP_MIN, TEMP_MAX)


def set_altitude_range(ax, axis="x"):
    if axis == "x":
        ax.set_xlim(ALT_MIN, ALT_MAX)
    else:
        ax.set_ylim(ALT_MIN, ALT_MAX)


def apply_time_axis_format(ax, time_range_days):
    import matplotlib.dates

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


def append_colorbar(scatter, shrink=1.0):
    scatter.set_clim(TEMP_MIN, TEMP_MAX)

    cbar = matplotlib.pyplot.colorbar(scatter, shrink=shrink)
    cbar.set_label(TEMP_AXIS_LABEL)
    set_tick_label_size(cbar.ax)

    return cbar


def create_grid(time_numeric, altitudes, temperatures, grid_points=100):
    time_min, time_max = time_numeric.min(), time_numeric.max()
    alt_min, alt_max = ALT_MIN, ALT_MAX

    time_grid = numpy.linspace(time_min, time_max, grid_points)
    alt_grid = numpy.linspace(alt_min, alt_max, grid_points)
    time_mesh, alt_mesh = numpy.meshgrid(time_grid, alt_grid)

    points = numpy.column_stack((time_numeric, altitudes))
    temp_grid = scipy.interpolate.griddata(points, temperatures, (time_mesh, alt_mesh), method="linear")

    return {
        "time_mesh": time_mesh,
        "alt_mesh": alt_mesh,
        "temp_grid": temp_grid,
        "time_min": time_min,
        "time_max": time_max,
        "alt_min": alt_min,
        "alt_max": alt_max,
    }


def create_figure(figsize=(12, 8)):
    return matplotlib.pyplot.subplots(figsize=figsize)


def set_axis_2d_default(ax, time_range):
    set_axis_labels(ax, TIME_AXIS_LABEL, ALT_AXIS_LABEL)

    set_altitude_range(ax, axis="y")

    ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(2000))
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    set_tick_label_size(ax)

    apply_time_axis_format(
        ax, matplotlib.dates.date2num(time_range[-1]) - matplotlib.dates.date2num(time_range[0])
    )


def conver_to_img(fig):
    buf = io.BytesIO()
    matplotlib.pyplot.savefig(buf, format="png", dpi=IMAGE_DPI, facecolor="white", transparent=False)

    buf.seek(0)

    img = PIL.Image.open(buf).copy()

    buf.close()

    matplotlib.pyplot.clf()
    matplotlib.pyplot.close(fig)

    return img


def prepare_data(raw_data):
    filtered_data = [d for d in raw_data if d["temperature"] > TEMPERATURE_THRESHOLD]

    if not filtered_data:
        return None

    clean_df = pandas.DataFrame(filtered_data)
    clean_df["time"] = pandas.to_datetime(clean_df["time"])

    times = clean_df["time"].to_numpy()
    altitudes = clean_df["altitude"].to_numpy()
    temperatures = clean_df["temperature"].to_numpy()

    time_numeric = numpy.array([matplotlib.dates.date2num(t) for t in times])

    return {
        "times": times,
        "time_numeric": time_numeric,
        "altitudes": altitudes,
        "temperatures": temperatures,
        "dataframe": clean_df,
    }


def set_font(font_config):
    try:
        for font_file in font_config["map"].values():
            matplotlib.font_manager.fontManager.addfont(
                pathlib.Path(font_config["path"]).resolve() / font_file
            )

        font_name = my_lib.plot_util.get_plot_font(font_config, "jp_medium", 12).get_name()

        matplotlib.pyplot.rcParams["font.family"] = [font_name, "sans-serif"]
        matplotlib.pyplot.rcParams["font.sans-serif"] = [font_name] + matplotlib.pyplot.rcParams[
            "font.sans-serif"
        ]
    except Exception:
        logging.exception("Failed to set font")


def set_axis_3d(ax, time_numeric):
    set_axis_labels(ax, TIME_AXIS_LABEL, ALT_AXIS_LABEL, TEMP_AXIS_LABEL)

    time_range = time_numeric[-1] - time_numeric[0]
    apply_time_axis_format(ax, time_range)
    ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(2000))
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

    set_tick_label_size(ax, is_3d=True)

    ax.set_ylim(ALT_MIN, ALT_MAX)
    ax.set_zlim(TEMP_MIN, TEMP_MAX)


def setup_3d_colorbar_and_layout(ax, _scatter):
    import matplotlib.pyplot  # noqa: ICN001

    ax.view_init(elev=25, azim=35)
    # プロットエリアを大きくするため余白を縮小
    matplotlib.pyplot.subplots_adjust(left=0.02, right=0.95, top=0.92, bottom=0.08)
    ax.set_position([0.02, 0.05, 0.80, 0.85])


def plot_scatter_3d(data):
    logging.info("Staring plot scatter 3d")

    start = time.perf_counter()

    fig = matplotlib.pyplot.figure(figsize=(14, 14))

    ax = fig.add_subplot(111, projection="3d")
    scatter = ax.scatter(
        data["time_numeric"],
        data["altitudes"],
        data["temperatures"],
        c=data["temperatures"],
        cmap="plasma",
        marker="o",
        s=15,
        alpha=0.9,
        rasterized=True,
        edgecolors="none",
    )

    set_axis_3d(ax, data["time_numeric"])
    append_colorbar(scatter, 0.6)
    setup_3d_colorbar_and_layout(ax, scatter)

    set_title("航空機の気象データ (3D)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_density(data):
    logging.info("Staring plot density")

    start = time.perf_counter()

    fig, ax = create_figure()

    scatter = ax.scatter(
        data["altitudes"],
        data["temperatures"],
        c=data["temperatures"],
        cmap="plasma",
        s=15,
        alpha=0.9,
        rasterized=True,
        edgecolors="none",
    )

    set_axis_labels(ax, ALT_AXIS_LABEL, TEMP_AXIS_LABEL)
    set_altitude_range(ax, axis="x")
    set_temperature_range(ax, axis="y")
    set_tick_label_size(ax)

    append_colorbar(scatter)

    ax.grid(True, alpha=0.7)

    set_title("航空機の気象データ (高度・温度分布)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_contour(data):
    logging.info("Staring plot contour")

    start = time.perf_counter()

    grid = create_grid(data["time_numeric"], data["altitudes"], data["temperatures"], grid_points=80)

    fig, ax = create_figure()

    levels = numpy.arange(TEMP_MIN, TEMP_MAX + 1, 10)
    contour = ax.contour(
        grid["time_mesh"], grid["alt_mesh"], grid["temp_grid"], levels=levels, colors="black", linewidths=0.5
    )
    contourf = ax.contourf(
        grid["time_mesh"],
        grid["alt_mesh"],
        grid["temp_grid"],
        levels=levels,
        cmap="plasma",
        alpha=0.9,
    )

    ax.clabel(contour, inline=True, fontsize=CONTOUR_SIZE, fmt="%d°C")

    set_axis_2d_default(
        ax,
        [
            matplotlib.dates.num2date(grid["time_min"]),
            matplotlib.dates.num2date(grid["time_max"]),
        ],
    )

    append_colorbar(contourf)

    set_title("航空機の気象データ (等高線)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_heatmap(data):
    logging.info("Staring plot heatmap")

    start = time.perf_counter()

    grid = create_grid(data["time_numeric"], data["altitudes"], data["temperatures"], grid_points=80)

    fig, ax = create_figure()

    im = ax.imshow(
        grid["temp_grid"],
        extent=[grid["time_min"], grid["time_max"], grid["alt_min"], grid["alt_max"]],
        aspect="auto",
        origin="lower",
        cmap="plasma",
        alpha=0.9,
        vmin=TEMP_MIN,
        vmax=TEMP_MAX,
    )

    set_axis_2d_default(
        ax,
        [
            matplotlib.dates.num2date(grid["time_min"]),
            matplotlib.dates.num2date(grid["time_max"]),
        ],
    )

    append_colorbar(im)

    set_title("航空機の気象データ (ヒートマップ)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_scatter_2d(data):
    logging.info("Staring plot 2d scatter")

    start = time.perf_counter()

    fig, ax = create_figure()

    sc = ax.scatter(
        data["times"],
        data["altitudes"],
        c=data["temperatures"],
        cmap="plasma",
        s=15,
        alpha=0.9,
        rasterized=True,
        edgecolors="none",
    )

    set_axis_2d_default(
        ax,
        [
            matplotlib.dates.num2date(data["time_numeric"].min()),
            matplotlib.dates.num2date(data["time_numeric"].max()),
        ],
    )

    append_colorbar(sc)

    ax.grid(True, alpha=0.7)

    set_title("航空機の気象データ")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


if __name__ == "__main__":

    def plot(raw_data):
        data = prepare_data(raw_data)

        if data is None:
            logging.warning("プロット用のデータがありません")
            return

        plot_def_list = [
            {"name": "scatter_2d", "func": plot_scatter_2d, "file": "scatter_2d.png"},
            {"name": "scatter_3d", "func": plot_scatter_3d, "file": "scatter_3d.png"},
            {"name": "density", "func": plot_density, "file": "density.png"},
            {"name": "contour", "func": plot_contour, "file": "contour.png"},
            {"name": "heatmap", "func": plot_heatmap, "file": "heatmap.png"},
        ]

        set_font(config["font"])

        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
            for plot_def in plot_def_list:
                plot_def["future"] = executor.submit(plot_def["func"], data)

            for plot_def in plot_def_list:
                img, elapsed = plot_def["future"].result()
                img.save(plot_def["file"])

                logging.info("elapsed time: %s = %.3f sec", plot_def["name"], elapsed)

    import docopt
    import my_lib.config
    import my_lib.logger
    import my_lib.time

    import modes.database_postgresql

    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    period_days = int(args["-p"])
    debug_mode = args["-D"]

    my_lib.logger.init("modes sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = my_lib.config.load(config_file)

    sqlite = modes.database_postgresql.open(
        config["database"]["host"],
        config["database"]["port"],
        config["database"]["name"],
        config["database"]["user"],
        config["database"]["pass"],
    )
    time_end = my_lib.time.now()
    time_start = time_end - datetime.timedelta(days=period_days)

    plot(modes.database_postgresql.fetch_by_time(sqlite, time_start, time_end))
