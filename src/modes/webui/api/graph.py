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
import hashlib
import io
import json
import logging
import multiprocessing
import pathlib
import time

import flask
import matplotlib  # noqa: ICN001

matplotlib.use("Agg")  # pyplotのimport前に設定する必要がある
import matplotlib.dates
import matplotlib.font_manager
import matplotlib.pyplot  # noqa: ICN001
import matplotlib.ticker
import mpl_toolkits.mplot3d  # noqa: F401
import my_lib.pil_util
import my_lib.plot_util
import my_lib.time
import numpy  # noqa: ICN001
import pandas  # noqa: ICN001
import PIL.Image
import PIL.ImageDraw
import PIL.ImageFont
import scipy.interpolate

import modes.database_postgresql

IMAGE_DPI = 200.0

TEMPERATURE_THRESHOLD = -100
TEMP_MIN = -80
TEMP_MAX = 30
ALT_MIN = 0
ALT_MAX = 13000
TICK_LABEL_SIZE = 8
CONTOUR_SIZE = 8
ERROR_SIZE = 30

AXIS_LABEL_SIZE = 12
TITLE_SIZE = 20

TIME_AXIS_LABEL = "日時"
ALT_AXIS_LABEL = "高度 (m)"
TEMP_AXIS_LABEL = "温度 (℃)"

blueprint = flask.Blueprint("modes-sensing-graph", __name__)


def connect_database(config):
    return modes.database_postgresql.open(
        config["database"]["host"],
        config["database"]["port"],
        config["database"]["name"],
        config["database"]["user"],
        config["database"]["pass"],
    )


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


def append_colorbar(scatter, shrink=0.8, pad=0.01, aspect=35, fraction=0.046):
    """
    カラーバーを追加（サイズを縮小してプロットエリアを拡大）

    Args:
        scatter: プロット要素
        shrink: カラーバーの高さの縮小率 (デフォルト: 0.8)
        pad: プロットエリアとカラーバーの間隔 (デフォルト: 0.01)
        aspect: カラーバーの幅の比率 (デフォルト: 35、より細く)
        fraction: カラーバーの幅の割合 (デフォルト: 0.046)

    """
    scatter.set_clim(TEMP_MIN, TEMP_MAX)

    cbar = matplotlib.pyplot.colorbar(scatter, shrink=shrink, pad=pad, aspect=aspect, fraction=fraction)
    cbar.set_label(TEMP_AXIS_LABEL, fontsize=AXIS_LABEL_SIZE)
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
    """余白を最適化した図を作成"""
    fig, ax = matplotlib.pyplot.subplots(figsize=figsize)

    # 余白を削減してプロットエリアを拡大
    fig.subplots_adjust(
        left=0.08,  # 左余白
        bottom=0.08,  # 下余白
        right=0.94,  # 右余白（カラーバーの目盛テキスト用スペースを確保）
        top=0.94,  # 上余白
    )

    return fig, ax


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


def create_no_data_image(config, graph_name, text="データがありません"):
    """データがない場合の画像を生成する"""
    # グラフサイズを取得
    size = GRAPH_DEF_MAP[graph_name]["size"]

    # 新しい画像を作成（白背景）
    img = PIL.Image.new("RGB", size, color="white")

    # フォントサイズをDPIに合わせて調整（20pt）
    font_size = int(ERROR_SIZE * IMAGE_DPI / 72)

    # my_lib.pil_utilを使用してフォントを取得
    font = my_lib.pil_util.get_font(config["font"], "jp_bold", font_size)

    pos = (size[0] // 2, size[1] // 2)

    my_lib.pil_util.draw_text(img, text, pos, font, align="center", color="#666")

    return img


def prepare_data(raw_data):
    filtered_data = [d for d in raw_data if d["temperature"] > TEMPERATURE_THRESHOLD]

    if not filtered_data:
        return {
            "count": 0,
            "times": [],
            "time_numeric": [],
            "altitudes": [],
            "temperatures": [],
            "dataframe": [],
        }

    clean_df = pandas.DataFrame(filtered_data)
    clean_df["time"] = pandas.to_datetime(clean_df["time"])

    times = clean_df["time"].to_numpy()
    altitudes = clean_df["altitude"].to_numpy()
    temperatures = clean_df["temperature"].to_numpy()

    time_numeric = numpy.array([matplotlib.dates.date2num(t) for t in times])

    return {
        "count": len(times),
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


def create_3d_figure(figsize=(12, 8)):
    """余白を最適化した3D図を作成"""
    fig = matplotlib.pyplot.figure(figsize=figsize)
    ax = fig.add_subplot(111, projection="3d")

    # 3D図の余白を削減してプロットエリアを拡大
    fig.subplots_adjust(
        left=0.02,  # 左余白
        bottom=0.05,  # 下余白
        right=0.94,  # 右余白（カラーバーをより右に、プロットエリアを拡大）
        top=0.95,  # 上余白
    )

    return fig, ax


def setup_3d_colorbar_and_layout(ax):
    """3Dプロットの余白とレイアウトを最適化"""
    ax.view_init(elev=25, azim=35)
    # 3Dプロットの位置を調整（左、下、幅、高さ）
    # プロットエリアを拡大（幅を0.82から0.86に）
    ax.set_position([0.02, 0.05, 0.86, 0.88])


def plot_scatter_3d(data, figsize):
    logging.info("Staring plot scatter 3d")

    start = time.perf_counter()

    fig, ax = create_3d_figure(figsize)
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
    append_colorbar(scatter, shrink=0.6, pad=0.01, aspect=35)
    setup_3d_colorbar_and_layout(ax)

    set_title("航空機の気象データ (3D)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_density(data, figsize):
    logging.info("Staring plot density")

    start = time.perf_counter()

    fig, ax = create_figure(figsize)

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

    append_colorbar(scatter, shrink=1.0, pad=0.01, aspect=35, fraction=0.03)

    ax.grid(True, alpha=0.7)

    set_title("航空機の気象データ (高度・温度分布)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_contour_2d(data, figsize):
    logging.info("Staring plot contour")

    start = time.perf_counter()

    grid = create_grid(data["time_numeric"], data["altitudes"], data["temperatures"], grid_points=80)

    fig, ax = create_figure(figsize)

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

    ax.clabel(contour, inline=True, fontsize=CONTOUR_SIZE, fmt="%d℃")

    set_axis_2d_default(
        ax,
        [
            matplotlib.dates.num2date(grid["time_min"]),
            matplotlib.dates.num2date(grid["time_max"]),
        ],
    )

    append_colorbar(contourf, shrink=1.0, pad=0.01, aspect=35, fraction=0.03)

    set_title("航空機の気象データ (等高線)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_heatmap(data, figsize):
    logging.info("Staring plot heatmap")

    start = time.perf_counter()

    grid = create_grid(data["time_numeric"], data["altitudes"], data["temperatures"], grid_points=80)

    fig, ax = create_figure(figsize)

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

    append_colorbar(im, shrink=1.0, pad=0.01, aspect=35, fraction=0.03)

    set_title("航空機の気象データ (ヒートマップ)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_scatter_2d(data, figsize):
    logging.info("Staring plot 2d scatter")

    start = time.perf_counter()

    fig, ax = create_figure(figsize)

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

    append_colorbar(sc, shrink=1.0, pad=0.01, aspect=35, fraction=0.03)

    ax.grid(True, alpha=0.7)

    set_title("航空機の気象データ")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_contour_3d(data, figsize):
    logging.info("Starting plot contour 3d")

    start = time.perf_counter()

    # グリッドデータを作成
    grid = create_grid(data["time_numeric"], data["altitudes"], data["temperatures"], grid_points=60)

    fig, ax = create_3d_figure(figsize)

    # 3Dサーフェスプロットを作成
    surf = ax.plot_surface(
        grid["time_mesh"],
        grid["alt_mesh"],
        grid["temp_grid"],
        cmap="plasma",
        alpha=0.9,
        antialiased=True,
        rstride=1,
        cstride=1,
        linewidth=0,
        edgecolor="none",
        vmin=TEMP_MIN,
        vmax=TEMP_MAX,
    )

    # 等高線を追加
    levels = numpy.arange(TEMP_MIN, TEMP_MAX + 1, 10)
    ax.contour(
        grid["time_mesh"],
        grid["alt_mesh"],
        grid["temp_grid"],
        levels=levels,
        colors="black",
        linewidths=0.5,
        alpha=0.3,
        offset=TEMP_MIN,  # 底面に等高線を投影
    )

    set_axis_3d(ax, data["time_numeric"])
    append_colorbar(surf, shrink=0.6, pad=0.01, aspect=35)
    setup_3d_colorbar_and_layout(ax)

    set_title("航空機の気象データ (3D)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


GRAPH_DEF_MAP = {
    "scatter_2d": {"func": plot_scatter_2d, "size": (2400, 1600), "file": "scatter_2d.png"},
    "scatter_3d": {"func": plot_scatter_3d, "size": (2800, 2800), "file": "scatter_3d.png"},
    "contour_2d": {"func": plot_contour_2d, "size": (2400, 1600), "file": "contour_2d.png"},
    "contour_3d": {"func": plot_contour_3d, "size": (2800, 2800), "file": "contour_3d.png"},
    "density": {"func": plot_density, "size": (2400, 1600), "file": "density.png"},
    "heatmap": {"func": plot_heatmap, "size": (2400, 1600), "file": "heatmap.png"},
}


def plot_in_subprocess(config, graph_name, time_start, time_end, figsize):
    """子プロセス内でデータ取得からグラフ描画まで一貫して実行する関数"""
    import matplotlib  # noqa: ICN001

    matplotlib.use("Agg")

    import matplotlib.pyplot  # noqa: ICN001

    # データベース接続とデータ取得を子プロセス内で実行
    conn = connect_database(config)
    # グラフ作成に必要な最小限のカラムのみ取得してパフォーマンス向上
    raw_data = modes.database_postgresql.fetch_by_time(
        conn,
        time_start,
        time_end,
        config["filter"]["area"]["distance"],
        columns=["time", "altitude", "temperature", "distance"],
    )
    conn.close()

    # データ準備（変換不要、直接処理）
    data = prepare_data(raw_data)

    if data["count"] < 10:
        # データがない場合の画像を生成
        try:
            img = create_no_data_image(config, graph_name)
            bytes_io = io.BytesIO()
            img.save(bytes_io, "PNG")
            bytes_io.seek(0)
            return bytes_io.getvalue(), 0
        except Exception:
            logging.exception("Failed to create no data image")
            img = create_no_data_image(config, graph_name, "グラフの作成に失敗しました")
            bytes_io = io.BytesIO()
            img.save(bytes_io, "PNG")
            bytes_io.seek(0)
            return bytes_io.getvalue(), 0

    set_font(config["font"])

    img, elapsed = GRAPH_DEF_MAP[graph_name]["func"](data, figsize)

    # PIL.Imageを直接returnできないので、bytesに変換して返す
    bytes_io = io.BytesIO()
    img.save(bytes_io, "PNG")
    bytes_io.seek(0)

    return bytes_io.getvalue(), elapsed


def plot(config, graph_name, time_start, time_end):
    # グラフサイズを計算
    figsize = tuple(x / IMAGE_DPI for x in GRAPH_DEF_MAP[graph_name]["size"])

    # データ取得から描画まで全て子プロセスで実行
    with multiprocessing.Pool(processes=1) as pool:
        result = pool.apply(plot_in_subprocess, (config, graph_name, time_start, time_end, figsize))

    image_bytes, elapsed = result

    if elapsed > 0:
        logging.info("elapsed time: %s = %.3f sec", graph_name, elapsed)
    else:
        logging.info("No data available for %s", graph_name)

    return image_bytes


@blueprint.route("/api/graph/<path:graph_name>", methods=["GET"])
def graph(graph_name):
    # デフォルト値を設定
    default_time_end = my_lib.time.now()
    default_time_start = default_time_end - datetime.timedelta(days=1)

    # パラメータから時間を取得（JSON文字列として）
    time_end_str = flask.request.args.get("end", None)
    time_start_str = flask.request.args.get("start", None)

    # 文字列をUTC時間のdatetimeに変換してからローカルタイムに変換
    if time_end_str:
        time_end = datetime.datetime.fromisoformat(json.loads(time_end_str).replace("Z", "+00:00"))
        time_end = time_end.astimezone(my_lib.time.get_zoneinfo())
    else:
        time_end = default_time_end

    if time_start_str:
        time_start = datetime.datetime.fromisoformat(json.loads(time_start_str).replace("Z", "+00:00"))
        time_start = time_start.astimezone(my_lib.time.get_zoneinfo())
    else:
        time_start = default_time_start

    logging.info("request: %s graph (start: %s, end: %s)", graph_name, time_start, time_end)

    # ETag生成用のキーを作成（グラフ名+時間範囲+1分単位の時刻）
    current_minute = my_lib.time.now().replace(second=0, microsecond=0)
    etag_key = f"{graph_name}:{time_start.isoformat()}:{time_end.isoformat()}:{current_minute.isoformat()}"
    etag = hashlib.md5(etag_key.encode()).hexdigest()  # noqa: S324

    # クライアントのETagをチェック
    if flask.request.headers.get("If-None-Match") == f'"{etag}"':
        return flask.Response(status=304)  # Not Modified

    config = flask.current_app.config["CONFIG"]

    image_bytes = plot(config, graph_name, time_start, time_end)

    res = flask.Response(image_bytes, mimetype="image/png")

    # キャッシュヘッダーを設定（1分間キャッシュ）
    res.headers["Cache-Control"] = "public, max-age=60"
    res.headers["ETag"] = f'"{etag}"'
    res.headers["Last-Modified"] = current_minute.strftime("%a, %d %b %Y %H:%M:%S GMT")

    return res


if __name__ == "__main__":

    def plot(raw_data):
        data = prepare_data(raw_data)

        if data is None:
            logging.warning("プロット用のデータがありません")
            return

        set_font(config["font"])

        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
            for graph_def in GRAPH_DEF_MAP.values():
                figsize = tuple(x / IMAGE_DPI for x in graph_def["size"])
                graph_def["future"] = executor.submit(graph_def["func"], data, figsize)

            for graph_name, graph_def in GRAPH_DEF_MAP.items():
                img, elapsed = graph_def["future"].result()
                img.save(graph_def["file"])

                logging.info("elapsed time: %s = %.3f sec", graph_name, elapsed)

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

    conn = modes.database_postgresql.open(
        config["database"]["host"],
        config["database"]["port"],
        config["database"]["name"],
        config["database"]["user"],
        config["database"]["pass"],
    )
    time_end = my_lib.time.now()
    time_start = time_end - datetime.timedelta(days=period_days)

    plot(
        modes.database_postgresql.fetch_by_time(
            conn,
            time_start,
            time_end,
            config["filter"]["area"]["distance"],
            columns=["time", "altitude", "temperature", "distance"],
        )
    )
