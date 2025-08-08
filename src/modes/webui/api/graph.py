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

import atexit
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


# グローバルプロセスプール管理（matplotlib マルチスレッド問題対応）
class ProcessPoolManager:
    """シングルトンパターンでプロセスプールを管理"""

    _instance = None
    _lock = multiprocessing.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance.pool = None
        return cls._instance

    def get_pool(self):
        """プロセスプールを取得（必要に応じて作成）"""
        if self.pool is None:
            with self._lock:
                if self.pool is None:
                    # CPUコア数に基づいてプロセス数を決定（最大4、最小1）
                    max_workers = min(max(multiprocessing.cpu_count() // 2, 1), 4)
                    self.pool = multiprocessing.Pool(processes=max_workers)
                    # アプリ終了時にプールをクリーンアップ
                    atexit.register(self.cleanup)
                    logging.info("Created global process pool with %d workers", max_workers)
        return self.pool

    def cleanup(self):
        """プロセスプールのクリーンアップ"""
        if self.pool is not None:
            try:
                self.pool.close()
                self.pool.join()
                self.pool = None
                logging.info("Cleaned up global process pool")
            except Exception as e:
                logging.warning("Error cleaning up process pool: %s", e)


# プロセスプールマネージャーのインスタンス
_pool_manager = ProcessPoolManager()


def connect_database(config):
    return modes.database_postgresql.open(
        config["database"]["host"],
        config["database"]["port"],
        config["database"]["name"],
        config["database"]["user"],
        config["database"]["pass"],
    )


def set_title(title_text):
    matplotlib.pyplot.title(title_text, fontsize=TITLE_SIZE, fontweight="bold", pad=20)


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


def create_grid(time_numeric, altitudes, temperatures, grid_points=100, time_range=None):
    # グリッド範囲を指定できるようにするが、データは全範囲を使用
    if time_range is not None:
        time_min, time_max = time_range
    else:
        time_min, time_max = time_numeric.min(), time_numeric.max()
    alt_min, alt_max = ALT_MIN, ALT_MAX

    # メモリ効率とキャッシュ効率を考慮したグリッド作成
    time_grid = numpy.linspace(time_min, time_max, grid_points)
    alt_grid = numpy.linspace(alt_min, alt_max, grid_points)
    time_mesh, alt_mesh = numpy.meshgrid(time_grid, alt_grid, indexing="xy")

    # データポイントの事前フィルタリング（範囲外データを除外）
    valid_data_mask = (
        (time_numeric >= time_min)
        & (time_numeric <= time_max)
        & (altitudes >= alt_min)
        & (altitudes <= alt_max)
        & numpy.isfinite(temperatures)
    )

    if valid_data_mask.sum() < 3:
        # 補間に必要な最小データ数が不足
        temp_grid = numpy.full_like(time_mesh, numpy.nan)
    else:
        # 有効なデータポイントのみで補間処理
        valid_points = numpy.column_stack((time_numeric[valid_data_mask], altitudes[valid_data_mask]))
        valid_temps = temperatures[valid_data_mask]

        # scipy.interpolate.griddataの最適化：
        # - method='linear'は最も高速
        # - fill_valueを明示的に指定してwarningを回避
        temp_grid = scipy.interpolate.griddata(
            valid_points, valid_temps, (time_mesh, alt_mesh), method="linear", fill_value=numpy.nan
        )

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
        top=0.90,  # 上余白（タイトル用スペースを拡大）
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
    # リスト内包表記からnumpy配列への直接変換で高速化
    if not raw_data:
        return {
            "count": 0,
            "times": numpy.array([]),
            "time_numeric": numpy.array([]),
            "altitudes": numpy.array([]),
            "temperatures": numpy.array([]),
            "dataframe": pandas.DataFrame(),
        }

    # 温度フィルタリングを先に行い、有効なインデックスを特定
    temperatures = numpy.array([d["temperature"] for d in raw_data])
    valid_mask = temperatures > TEMPERATURE_THRESHOLD

    if not valid_mask.any():
        return {
            "count": 0,
            "times": numpy.array([]),
            "time_numeric": numpy.array([]),
            "altitudes": numpy.array([]),
            "temperatures": numpy.array([]),
            "dataframe": pandas.DataFrame(),
        }

    # 有効なデータのみを効率的に抽出
    filtered_data = [raw_data[i] for i in numpy.where(valid_mask)[0]]

    # pandas.DataFrameの作成を最小限に（dataframeが必要な場合のみ）
    # 基本的な配列処理はnumpyで高速化
    times_list = [d["time"] for d in filtered_data]
    altitudes = numpy.array([d["altitude"] for d in filtered_data])
    temperatures = temperatures[valid_mask]

    # pandas.to_datetimeは比較的重いので、必要最小限で使用
    times = pandas.to_datetime(times_list).to_numpy()

    # matplotlib.dates.date2numをベクトル化して高速化
    time_numeric = matplotlib.dates.date2num(times)

    # DataFrameは風向・風速グラフでのみ必要
    # filtered_dataには既に有効なデータのみが含まれている
    clean_df = pandas.DataFrame(filtered_data)

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
        top=0.91,  # 上余白（タイトル用スペースを拡大）
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


def plot_contour_2d(data, figsize, plot_time_start=None, plot_time_end=None):
    logging.info("Staring plot contour")

    start = time.perf_counter()

    # プロット時間範囲が指定されている場合は、グリッドをその範囲で作成
    # ただし、実際のデータ範囲を超えないように制限
    if plot_time_start and plot_time_end:
        plot_time_min = matplotlib.dates.date2num(plot_time_start)
        plot_time_max = matplotlib.dates.date2num(plot_time_end)
        # 実際のデータ範囲内に制限
        if len(data["time_numeric"]) > 0:
            actual_min = data["time_numeric"].min()
            actual_max = data["time_numeric"].max()
            plot_time_min = max(plot_time_min, actual_min)
            plot_time_max = min(plot_time_max, actual_max)
        grid = create_grid(
            data["time_numeric"],
            data["altitudes"],
            data["temperatures"],
            grid_points=80,
            time_range=(plot_time_min, plot_time_max),
        )
    else:
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

    # プロット時間範囲が指定されている場合はそれを使用、そうでなければグリッド範囲を使用
    if plot_time_start and plot_time_end:
        time_range = [plot_time_start, plot_time_end]
    else:
        time_range = [
            matplotlib.dates.num2date(grid["time_min"]),
            matplotlib.dates.num2date(grid["time_max"]),
        ]

    set_axis_2d_default(ax, time_range)

    append_colorbar(contourf, shrink=1.0, pad=0.01, aspect=35, fraction=0.03)

    set_title("航空機の気象データ (等高線)")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


def plot_heatmap(data, figsize, plot_time_start=None, plot_time_end=None):
    logging.info("Staring plot heatmap")

    start = time.perf_counter()

    # プロット時間範囲が指定されている場合は、グリッドをその範囲で作成
    # ただし、実際のデータ範囲を超えないように制限
    if plot_time_start and plot_time_end:
        plot_time_min = matplotlib.dates.date2num(plot_time_start)
        plot_time_max = matplotlib.dates.date2num(plot_time_end)
        # 実際のデータ範囲内に制限
        if len(data["time_numeric"]) > 0:
            actual_min = data["time_numeric"].min()
            actual_max = data["time_numeric"].max()
            plot_time_min = max(plot_time_min, actual_min)
            plot_time_max = min(plot_time_max, actual_max)
        grid = create_grid(
            data["time_numeric"],
            data["altitudes"],
            data["temperatures"],
            grid_points=80,
            time_range=(plot_time_min, plot_time_max),
        )
    else:
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

    # プロット時間範囲が指定されている場合はそれを使用、そうでなければグリッド範囲を使用
    if plot_time_start and plot_time_end:
        time_range = [plot_time_start, plot_time_end]
    else:
        time_range = [
            matplotlib.dates.num2date(grid["time_min"]),
            matplotlib.dates.num2date(grid["time_max"]),
        ]

    set_axis_2d_default(ax, time_range)

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


def _prepare_wind_data(data):
    """風データの前処理とビニング処理"""
    # 風データが利用可能かチェック
    if "dataframe" not in data or len(data["dataframe"]) == 0:
        logging.warning("Wind data not available for wind direction plot")
        raise ValueError("Wind data not available")  # noqa: TRY003, EM101

    df = data["dataframe"]

    # 風データのカラムが存在するかチェック
    required_columns = ["time", "altitude", "wind_x", "wind_y", "wind_speed", "wind_angle"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.warning("Missing wind data columns: %s", missing_columns)
        logging.warning("Available columns: %s", list(df.columns))
        raise ValueError(f"Missing wind data columns: {missing_columns}")  # noqa: TRY003, EM102

    # 200m毎の高度ビンを作成
    altitude_bins = numpy.arange(0, 13000, 200)  # 0-13000mを200m間隔
    df_copy = df.copy()
    df_copy["altitude_bin"] = pandas.cut(df_copy["altitude"], bins=altitude_bins, labels=altitude_bins[:-1])

    # 時間ビンを作成
    df_copy["time_numeric"] = df_copy["time"].apply(matplotlib.dates.date2num)
    time_range = df_copy["time_numeric"].max() - df_copy["time_numeric"].min()
    if time_range <= 1:  # 1日以内
        time_bins = 48  # 30分間隔
    elif time_range <= 3:  # 3日以内
        time_bins = 24  # 3時間間隔
    else:
        time_bins = int(time_range * 4)  # 6時間間隔

    time_bin_edges = numpy.linspace(
        df_copy["time_numeric"].min(), df_copy["time_numeric"].max(), time_bins + 1
    )
    df_copy["time_bin"] = pandas.cut(df_copy["time_numeric"], bins=time_bin_edges)

    # 各ビンで風向成分を平均化
    grouped = (
        df_copy.groupby(["time_bin", "altitude_bin"], observed=False)
        .agg({"wind_x": "mean", "wind_y": "mean", "time_numeric": "mean"})
        .reset_index()
    )

    grouped = grouped.dropna()
    if len(grouped) == 0:
        logging.warning("No valid wind data after binning")
        raise ValueError("No valid wind data after binning")  # noqa: TRY003, EM101

    # 風速と風向を再計算
    grouped["wind_speed"] = numpy.sqrt(grouped["wind_x"] ** 2 + grouped["wind_y"] ** 2)
    grouped["wind_angle"] = (90 - numpy.degrees(numpy.arctan2(grouped["wind_y"], grouped["wind_x"]))) % 360

    # 無風を除外
    grouped = grouped[grouped["wind_speed"] > 0.1]
    if len(grouped) == 0:
        logging.warning("No valid wind vectors after speed filtering")
        raise ValueError("No valid wind vectors after speed filtering")  # noqa: TRY003, EM101

    return grouped


def plot_wind_direction(data, figsize):
    logging.info("Starting plot wind direction")
    start = time.perf_counter()

    # デバッグ情報
    if "dataframe" in data and len(data["dataframe"]) > 0:
        df = data["dataframe"]
        logging.info("Available columns in dataframe: %s", list(df.columns))
        logging.info("Dataframe shape: %s", df.shape)

    # データ前処理
    grouped = _prepare_wind_data(data)

    # ベクトル計算
    time_range = grouped["time_numeric"].max() - grouped["time_numeric"].min()
    altitude_range = 13000
    u_scale = time_range / 30
    v_scale = altitude_range / 30

    wind_magnitude = numpy.sqrt(grouped["wind_x"] ** 2 + grouped["wind_y"] ** 2)
    # 風向きベクトルの符号を反転（wind_x, wind_yは風が来る方向、矢印は風が来る方向を指すべき）
    grouped["u_normalized"] = -(grouped["wind_x"] / wind_magnitude) * u_scale
    grouped["v_normalized"] = -(grouped["wind_y"] / wind_magnitude) * v_scale

    grouped = grouped.dropna()
    if len(grouped) == 0:
        logging.warning("No valid wind vectors after angle conversion")
        raise ValueError("No valid wind vectors after angle conversion")  # noqa: TRY003, EM101

    # プロット作成
    fig, ax = create_figure(figsize)
    wind_speeds = grouped["wind_speed"].to_numpy()
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
    )

    cbar = matplotlib.pyplot.colorbar(quiver, shrink=0.8, pad=0.01, aspect=35, fraction=0.046)
    cbar.set_label("風速 (m/s)", fontsize=AXIS_LABEL_SIZE)
    set_tick_label_size(cbar.ax)

    set_title("航空機観測による風向・風速分布")

    # デバッグ情報出力
    logging.info("Wind direction plot: %d vectors calculated", len(grouped))
    logging.info("Vector scales: u_scale=%s, v_scale=%s", u_scale, v_scale)

    img = conver_to_img(fig)
    return (img, time.perf_counter() - start)


def plot_temperature(data, figsize):
    logging.info("Starting plot temperature timeseries")

    start = time.perf_counter()

    fig, ax = create_figure(figsize)

    # 高度範囲の定義
    altitude_ranges = [
        {"min": 1400, "max": 1600, "label": "1500±100m", "color": "blue", "marker": "o"},
        {"min": 2900, "max": 3100, "label": "3000±100m", "color": "green", "marker": "s"},
        {"min": 4400, "max": 4600, "label": "4500±100m", "color": "orange", "marker": "^"},
        {"min": 5900, "max": 6100, "label": "6000±100m", "color": "red", "marker": "d"},
    ]

    # 各高度範囲のデータをプロット
    for alt_range in altitude_ranges:
        # 高度範囲でフィルタリング
        mask = (data["altitudes"] >= alt_range["min"]) & (data["altitudes"] <= alt_range["max"])
        if not numpy.any(mask):
            continue

        filtered_temps = data["temperatures"][mask]
        filtered_time_numeric = data["time_numeric"][mask]

        # 時系列でソート
        sort_indices = numpy.argsort(filtered_time_numeric)
        sorted_times = filtered_time_numeric[sort_indices]
        sorted_temps = filtered_temps[sort_indices]

        # 同じ時間帯のデータを平均化（30分間隔でビニング）
        if len(sorted_times) > 1:
            # 30分 = 0.020833日
            bin_size = 0.020833
            unique_times = []
            avg_temps = []

            current_bin_start = sorted_times[0]
            current_temps = []

            for i, time_val in enumerate(sorted_times):
                if time_val <= current_bin_start + bin_size:
                    current_temps.append(sorted_temps[i])
                else:
                    if current_temps:
                        unique_times.append(current_bin_start + bin_size / 2)
                        avg_temps.append(numpy.mean(current_temps))

                    current_bin_start = time_val
                    current_temps = [sorted_temps[i]]

            # 最後のビンを処理
            if current_temps:
                unique_times.append(current_bin_start + bin_size / 2)
                avg_temps.append(numpy.mean(current_temps))

            # プロット
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

    # 軸の設定
    ax.set_xlabel("日時")
    ax.set_ylabel("温度 (℃)")
    ax.grid(True, alpha=0.7)

    # 時間軸の書式設定
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%m/%d\n%H:%M"))
    ax.xaxis.set_major_locator(matplotlib.dates.HourLocator(interval=6))
    ax.xaxis.set_minor_locator(matplotlib.dates.HourLocator(interval=2))

    # Y軸の範囲設定
    ax.set_ylim(-20, 30)

    # 凡例の追加
    ax.legend(loc="upper right", framealpha=0.9)

    set_title("高度別温度の時系列変化")

    img = conver_to_img(fig)

    return (img, time.perf_counter() - start)


GRAPH_DEF_MAP = {
    "scatter_2d": {"func": plot_scatter_2d, "size": (2400, 1600), "file": "scatter_2d.png"},
    "scatter_3d": {"func": plot_scatter_3d, "size": (2800, 2800), "file": "scatter_3d.png"},
    "contour_2d": {"func": plot_contour_2d, "size": (2400, 1600), "file": "contour_2d.png"},
    "contour_3d": {"func": plot_contour_3d, "size": (2800, 2800), "file": "contour_3d.png"},
    "density": {"func": plot_density, "size": (2400, 1600), "file": "density.png"},
    "heatmap": {"func": plot_heatmap, "size": (2400, 1600), "file": "heatmap.png"},
    "temperature": {
        "func": plot_temperature,
        "size": (2400, 1600),
        "file": "temperature.png",
    },
    "wind_direction": {
        "func": plot_wind_direction,
        "size": (2400, 1600),
        "file": "wind_direction.png",
    },
}


def plot_in_subprocess(config, graph_name, time_start, time_end, figsize):
    """子プロセス内でデータ取得からグラフ描画まで一貫して実行する関数"""
    import matplotlib  # noqa: ICN001

    matplotlib.use("Agg")

    import matplotlib.pyplot  # noqa: ICN001

    # データベース接続とデータ取得を子プロセス内で実行
    conn = connect_database(config)
    # 風向グラフの場合は風データも取得
    if graph_name == "wind_direction":
        columns = [
            "time",
            "altitude",
            "temperature",
            "distance",
            "wind_x",
            "wind_y",
            "wind_speed",
            "wind_angle",
        ]
    else:
        # グラフ作成に必要な最小限のカラムのみ取得してパフォーマンス向上
        columns = ["time", "altitude", "temperature", "distance"]

    # heatmapとcontourグラフの場合、端の部分のプロットを改善するためデータ取得範囲を10%拡張
    if graph_name in ["heatmap", "contour_2d"]:
        time_range = time_end - time_start
        extension = time_range * 0.1  # 10%拡張
        extended_time_start = time_start - extension
        extended_time_end = time_end + extension
    else:
        extended_time_start = time_start
        extended_time_end = time_end

    raw_data = modes.database_postgresql.fetch_by_time(
        conn,
        extended_time_start,
        extended_time_end,
        config["filter"]["area"]["distance"],
        columns=columns,
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

    try:
        # heatmapとcontourグラフの場合、元の時間範囲を渡してプロット範囲を制限
        if graph_name in ["heatmap", "contour_2d"]:
            img, elapsed = GRAPH_DEF_MAP[graph_name]["func"](data, figsize, time_start, time_end)
        else:
            img, elapsed = GRAPH_DEF_MAP[graph_name]["func"](data, figsize)
    except Exception as e:
        logging.warning("Failed to generate %s: %s", graph_name, str(e))
        # エラー時は「データなし」画像を生成
        try:
            img = create_no_data_image(config, graph_name)
            elapsed = 0
        except Exception:
            logging.exception("Failed to create no data image")
            img = create_no_data_image(config, graph_name, "グラフの作成に失敗しました")
            elapsed = 0

    # PIL.Imageを直接returnできないので、bytesに変換して返す
    bytes_io = io.BytesIO()
    img.save(bytes_io, "PNG")
    bytes_io.seek(0)

    return bytes_io.getvalue(), elapsed


def plot(config, graph_name, time_start, time_end):
    # グラフサイズを計算
    figsize = tuple(x / IMAGE_DPI for x in GRAPH_DEF_MAP[graph_name]["size"])

    # グローバルプロセスプールを使用してデータ取得から描画まで実行
    pool = _pool_manager.get_pool()
    try:
        result = pool.apply(plot_in_subprocess, (config, graph_name, time_start, time_end, figsize))
        image_bytes, elapsed = result

        if elapsed > 0:
            logging.info("elapsed time: %s = %.3f sec", graph_name, elapsed)
        else:
            logging.info("No data available for %s", graph_name)

        return image_bytes
    except Exception:
        logging.exception("Error in plot generation for %s", graph_name)
        # エラー時は直接エラー画像を生成
        try:
            img = create_no_data_image(config, graph_name, "グラフの作成に失敗しました")
            bytes_io = io.BytesIO()
            img.save(bytes_io, "PNG")
            bytes_io.seek(0)
            return bytes_io.getvalue()
        except Exception:
            # 最終的にフォールバック画像を返す
            logging.exception("Failed to create error image for %s", graph_name)
            return b""


@blueprint.route("/api/data-range", methods=["GET"])
def data_range():
    """データベースの最古・最新データの日時を返すAPI"""
    try:
        config = flask.current_app.config["CONFIG"]
        conn = connect_database(config)

        # データ範囲を取得
        result = modes.database_postgresql.fetch_data_range(conn)
        conn.close()

        if result["earliest"] and result["latest"]:
            # タイムゾーン情報を追加してJSONシリアライゼーション可能にする
            earliest = result["earliest"]
            latest = result["latest"]

            # タイムゾーン情報がない場合はローカルタイムゾーンを適用
            if earliest.tzinfo is None:
                earliest = earliest.replace(tzinfo=my_lib.time.get_zoneinfo())
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=my_lib.time.get_zoneinfo())

            response_data = {
                "earliest": earliest.isoformat(),
                "latest": latest.isoformat(),
                "count": result["count"],
            }
        else:
            # データがない場合
            response_data = {"earliest": None, "latest": None, "count": 0}

        return flask.jsonify(response_data)

    except Exception as e:
        logging.exception("Error fetching data range")
        return flask.jsonify({"error": "データ範囲の取得に失敗しました", "details": str(e)}), 500


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

    # キャッシュヘッダーを設定（1時間キャッシュ）
    res.headers["Cache-Control"] = "public, max-age=3600"
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
