#!/usr/bin/env python3
"""
気象データをグラフにプロットします．

Usage:
  graph.py [-c CONFIG]

Options:
  -c CONFIG     : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
"""

import datetime
import logging

import matplotlib.pyplot  # noqa: ICN001
import numpy as np
import pandas as pd
from matplotlib import font_manager
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

# カスタムフォントの設定
medium_font_path = "font/A-OTF-ShinGoPro-Medium.otf"
bold_font_path = "font/A-OTF-ShinGoPro-Bold.otf"

try:
    font_manager.fontManager.addfont(medium_font_path)
    font_manager.fontManager.addfont(bold_font_path)

    # フォント名を確認して設定
    prop = font_manager.FontProperties(fname=medium_font_path)
    font_name = prop.get_name()
    # フォント名にスペースが含まれる場合の処理
    matplotlib.pyplot.rcParams["font.family"] = [font_name, "sans-serif"]
    matplotlib.pyplot.rcParams["font.sans-serif"] = [font_name] + matplotlib.pyplot.rcParams[
        "font.sans-serif"
    ]
    logging.info("フォントを設定しました: %s", font_name)
except Exception as e:
    logging.warning("カスタムフォントの読み込みに失敗しました: %s", e)
    logging.info("デフォルトフォントを使用します")

# NOTE: 温度がこれより高いデータのみ残す
TEMPERATURE_THRESHOLD = -100


def weighted_distance(x1, x2, altitude_weight=1.0, time_weight=1.0):  # noqa: ARG001
    weight = np.array([1.0, 2.0])  # feature1の重みは1.0, feature2の重みは2.0

    return np.sqrt(np.sum(weight * (x1 - x2) ** 2))


def _prepare_data(data_list):
    """データの前処理（温度閾値フィルタとDataFrame変換）"""
    filtered_data = [d for d in data_list if d["temperature"] > TEMPERATURE_THRESHOLD]
    return pd.DataFrame(filtered_data)


def plot(data_list):
    import concurrent.futures

    # データの前処理を一度だけ実行
    logging.info("データ前処理を開始...")
    clean_df = _prepare_data(data_list)

    if len(clean_df) == 0:
        logging.warning("プロット用のデータがありません")
        return

    # DataFrameをdictに変換してプロセス間で渡せるようにする
    clean_data_dict = clean_df.to_dict("records")

    # 2つのプロット生成を並列実行（マルチプロセス）
    with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
        # シリアライズ可能なデータを渡す
        future_2d = executor.submit(_plot_2d_multiprocess, clean_data_dict, "a.png")
        future_3d = executor.submit(_plot_3d_multiprocess, clean_data_dict, "3d_plot.png")

        # 両方の処理が完了するまで待機
        concurrent.futures.wait([future_2d, future_3d])

        # エラーがあった場合は例外を発生させる
        future_2d.result()
        future_3d.result()


def _setup_fonts_multiprocess():
    """マルチプロセス用のフォント設定"""
    import logging

    import matplotlib.pyplot as plt
    from matplotlib import font_manager

    medium_font_path = "font/A-OTF-ShinGoPro-Medium.otf"
    try:
        font_manager.fontManager.addfont(medium_font_path)
        prop = font_manager.FontProperties(fname=medium_font_path)
        font_name = prop.get_name()
        plt.rcParams["font.family"] = [font_name, "sans-serif"]
        plt.rcParams["font.sans-serif"] = [font_name] + plt.rcParams["font.sans-serif"]
    except Exception as e:
        logging.warning("フォント設定に失敗: %s", e)


def _plot_2d_multiprocess(clean_data_dict, filename):
    """マルチプロセス用の2Dプロット生成関数"""
    import logging

    import matplotlib.pyplot as plt
    import pandas as pd
    from matplotlib import dates as mdates
    from matplotlib import font_manager
    from matplotlib.ticker import StrMethodFormatter

    _setup_fonts_multiprocess()
    bold_font_path = "font/A-OTF-ShinGoPro-Bold.otf"

    # データ準備
    clean_df = pd.DataFrame(clean_data_dict)
    clean_df["time"] = pd.to_datetime(clean_df["time"])
    logging.info("2Dプロット生成開始: %s", filename)

    # データ取得
    times = clean_df["time"].to_numpy()
    altitudes = clean_df["altitude"].to_numpy()
    temperatures = clean_df["temperature"].to_numpy()

    # 高速化設定とプロット作成
    plt.rcParams["path.simplify"] = True
    plt.rcParams["path.simplify_threshold"] = 0.1
    plt.rcParams["agg.path.chunksize"] = 10000

    fig, ax = plt.subplots()
    sc = ax.scatter(
        times,
        altitudes,
        c=temperatures,
        cmap="plasma",
        marker="o",
        s=8,
        rasterized=True,
        edgecolors="none",
    )
    sc.set_clim(-80, 30)
    ax.set_ylim(0, 14000)
    ax.set_xlabel("日時")
    ax.set_ylabel("高度 (m)")

    # カラーバーと時間軸フォーマット
    cbar = plt.colorbar(sc)
    cbar.set_label("温度 (℃)")

    time_range = mdates.date2num(times[-1]) - mdates.date2num(times[0])
    if time_range <= 1:
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=8))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-H時"))
    elif time_range <= 3:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-d日\n%-H時"))
    elif time_range <= 7:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))
    else:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=int(time_range / 3)))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))

    ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
    plt.grid()

    # タイトル設定
    try:
        bold_prop = font_manager.FontProperties(fname=bold_font_path)
        ax.set_title("航空機の気象データ", fontsize=28, fontproperties=bold_prop, pad=10)
    except Exception:
        ax.set_title("航空機の気象データ", fontsize=28, fontweight="bold", pad=10)

    plt.tight_layout()
    plt.savefig(filename, format="png", dpi=200, transparent=True)
    plt.close(fig)
    logging.info("2Dプロットを保存しました: %s", filename)


def _setup_3d_time_axis(ax, time_numeric):
    """3D用の時間軸フォーマット設定"""
    from matplotlib import dates as mdates

    time_range = time_numeric[-1] - time_numeric[0]
    if time_range <= 1:
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=8))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-H時"))
    elif time_range <= 3:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))
    elif time_range <= 7:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))
    else:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=int(time_range / 3)))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))


def _setup_3d_axes(ax, time_numeric):
    """3D軸の設定"""
    from matplotlib.ticker import StrMethodFormatter

    ax.set_xlabel("日時", labelpad=5, fontsize=12)
    ax.set_ylabel("高度 (m)", labelpad=12, fontsize=12)
    ax.set_zlabel("温度 (℃)", labelpad=8, fontsize=12)

    _setup_3d_time_axis(ax, time_numeric)
    ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
    ax.tick_params(axis="x", labelsize=10)
    ax.tick_params(axis="y", labelsize=10)
    ax.tick_params(axis="z", labelsize=10)
    ax.set_ylim(0, 14000)
    ax.set_zlim(-80, 30)


def _setup_3d_colorbar_and_layout(ax, scatter):
    """3Dプロットのカラーバーとレイアウト設定"""
    import matplotlib.pyplot as plt

    scatter.set_clim(-80, 30)
    cbar = plt.colorbar(scatter, shrink=0.8, pad=0.15)
    cbar.set_label("温度 (℃)", fontsize=12)
    cbar.ax.tick_params(labelsize=10)

    ax.view_init(elev=25, azim=35)
    plt.subplots_adjust(left=0.01, right=0.99, top=0.95, bottom=0.05)
    ax.set_position([0.05, 0.08, 0.70, 0.82])


def _set_3d_title(ax):
    """3Dプロットのタイトル設定"""
    from matplotlib import font_manager

    bold_font_path = "font/A-OTF-ShinGoPro-Bold.otf"
    try:
        bold_prop = font_manager.FontProperties(fname=bold_font_path)
        ax.set_title("航空機の気象データ(3D)", fontsize=28, fontproperties=bold_prop, pad=10)
    except Exception:
        ax.set_title("航空機の気象データ(3D)", fontsize=28, fontweight="bold", pad=10)


def _plot_3d_multiprocess(clean_data_dict, filename):
    """マルチプロセス用の3Dプロット生成関数"""
    import logging

    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    from matplotlib import dates as mdates

    _setup_fonts_multiprocess()

    # データ準備
    clean_df = pd.DataFrame(clean_data_dict)
    clean_df["time"] = pd.to_datetime(clean_df["time"])
    logging.info("3Dプロット生成開始: %s", filename)

    # データ取得
    time_numeric = np.array([mdates.date2num(t) for t in clean_df["time"]])
    altitudes = clean_df["altitude"].to_numpy()
    temperatures = clean_df["temperature"].to_numpy()

    # 高速化設定
    plt.rcParams["path.simplify"] = True
    plt.rcParams["path.simplify_threshold"] = 0.1
    plt.rcParams["agg.path.chunksize"] = 10000

    # 3Dプロット作成
    fig = plt.figure(figsize=(14, 10))
    ax = fig.add_subplot(111, projection="3d")
    scatter = ax.scatter(
        time_numeric,
        altitudes,
        temperatures,
        c=temperatures,
        cmap="plasma",
        marker="o",
        s=20,
        alpha=0.7,
        rasterized=True,
        edgecolors="none",
    )

    # 各種設定
    _setup_3d_axes(ax, time_numeric)
    _setup_3d_colorbar_and_layout(ax, scatter)
    _set_3d_title(ax)

    # 保存
    plt.savefig(filename, format="png", dpi=200, transparent=True, bbox_inches=None)
    plt.close(fig)
    logging.info("3Dプロットを保存しました: %s", filename)


if __name__ == "__main__":
    import docopt
    import my_lib.config
    import my_lib.logger
    import my_lib.time

    import modes.database_postgresql

    args = docopt.docopt(__doc__)

    my_lib.logger.init("ModeS sensing", level=logging.INFO)

    config_file = args["-c"]
    config = my_lib.config.load(args["-c"])

    sqlite = modes.database_postgresql.open(
        config["database"]["host"],
        config["database"]["port"],
        config["database"]["name"],
        config["database"]["user"],
        config["database"]["pass"],
    )
    time_end = my_lib.time.now()
    time_start = time_end - datetime.timedelta(hours=600)

    plot(modes.database_postgresql.fetch_by_time(sqlite, time_start, time_end))
