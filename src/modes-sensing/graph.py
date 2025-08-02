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
from matplotlib import dates as mdates
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


def plot_3d(data_list, filename="3d_plot.png"):
    """
    時間(x軸)、高度(y軸)、温度(z軸)の3次元プロットを生成

    Args:
        data_list: 気象データのリスト
        filename (str): 保存するファイル名

    """
    clean_df = _prepare_data(data_list)

    if len(clean_df) == 0:
        logging.warning("プロット用のデータがありません")
        return

    # NumPy配列への変換で高速化
    time_numeric = np.array([mdates.date2num(t) for t in clean_df["time"]])
    altitudes = clean_df["altitude"].to_numpy()
    temperatures = clean_df["temperature"].to_numpy()

    # 3D図の作成（サイズを大きくしてラベル用の余白を確保）
    # rcParamsで高速化設定
    matplotlib.pyplot.rcParams["path.simplify"] = True
    matplotlib.pyplot.rcParams["path.simplify_threshold"] = 0.1
    matplotlib.pyplot.rcParams["agg.path.chunksize"] = 10000

    fig = matplotlib.pyplot.figure(figsize=(14, 10))
    ax = fig.add_subplot(111, projection="3d")

    # 3D散布図の作成（rasterizedで高速化）
    scatter = ax.scatter(
        time_numeric,
        altitudes,
        temperatures,
        c=temperatures,
        cmap="plasma",
        marker="o",
        s=20,  # 元の設定に戻す
        alpha=0.7,  # 元の設定に戻す
        rasterized=True,  # ベクター描画を無効にして高速化
        edgecolors="none",  # エッジを無効にして高速化
    )

    # 軸ラベルの設定（日本語ラベル、altitudeラベルを少し離す、2Dと同じフォントサイズ）
    ax.set_xlabel("日時", labelpad=5, fontsize=12)  # 2Dと同じ
    ax.set_ylabel("高度 (m)", labelpad=12, fontsize=12)  # 2Dと同じ
    ax.set_zlabel("温度 (℃)", labelpad=8, fontsize=12)  # 2Dと同じ

    # 時間軸のフォーマット設定（日本語フォーマット）
    # データの時間範囲を取得
    time_range = time_numeric[-1] - time_numeric[0]

    # 時間範囲に応じて適切な間隔を設定（約3本のメモリ）
    if time_range <= 1:  # 1日以内
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=8))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-H時"))
    elif time_range <= 3:  # 3日以内
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))
    elif time_range <= 7:  # 1週間以内
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))
    else:  # それ以上
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=int(time_range / 3)))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))

    # 高度軸にカンマ区切りフォーマッターを追加
    from matplotlib.ticker import StrMethodFormatter

    ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))

    # 軸メモリのフォントサイズ（2Dと同じ）
    ax.tick_params(axis="x", labelsize=10)  # 2Dと同じ
    ax.tick_params(axis="y", labelsize=10)  # 2Dと同じ
    ax.tick_params(axis="z", labelsize=10)  # 2Dと同じ

    # 軸の範囲設定
    ax.set_ylim(0, 14000)
    ax.set_zlim(-80, 30)

    # カラーバーの追加と範囲設定（間隔を詰める、フォントサイズを大きく）
    scatter.set_clim(-80, 30)
    cbar = matplotlib.pyplot.colorbar(
        scatter, shrink=0.8, pad=0.15
    )  # padを0.4から0.15に減少してプロットエリアに近づける
    cbar.set_label("温度 (℃)", fontsize=12)  # 2Dと同じ
    cbar.ax.tick_params(labelsize=10)  # 2Dと同じ

    # 視点の設定（z軸ラベルが見えやすい角度に調整）
    ax.view_init(elev=25, azim=35)

    # 外側余白を削減してプロットエリアを拡大（全体的に余白を削減）
    matplotlib.pyplot.subplots_adjust(
        left=0.01, right=0.99, top=0.95, bottom=0.05
    )  # 上下左右の余白をさらに削減

    # プロットエリアを調整（カラーバーとの間隔を詰めて、全体を拡大）
    ax.set_position([0.05, 0.08, 0.70, 0.82])  # 左右の余白を削減し、プロットエリアを拡大

    # タイトルを上部に配置（日本語タイトル、Boldフォント、フォントサイズをさらに大きく、位置を下に）
    try:
        bold_prop = font_manager.FontProperties(fname=bold_font_path)
        ax.set_title("航空機の気象データ(3D)", pad=10, fontsize=20, fontproperties=bold_prop)  # 2Dと同じ
    except Exception:
        ax.set_title("航空機の気象データ(3D)", pad=10, fontsize=20, fontweight="bold")  # 2Dと同じ

    # ファイル保存（bbox_inchesをNoneにして図全体を保存）
    matplotlib.pyplot.savefig(filename, format="png", dpi=200, transparent=True, bbox_inches=None)
    logging.info("3Dプロットを保存しました: %s", filename)


def plot_2d(data_list, filename="a.png"):
    """
    2次元プロット（時間 vs 高度、温度を色で表現）を生成

    Args:
        data_list: 気象データのリスト
        filename (str): 保存するファイル名

    """
    clean_df = _prepare_data(data_list)

    if len(clean_df) == 0:
        logging.warning("2Dプロット用のデータがありません")
        return

    # NumPy配列への変換で高速化
    times = clean_df["time"].to_numpy()
    altitudes = clean_df["altitude"].to_numpy()
    temperatures = clean_df["temperature"].to_numpy()

    # 高速化設定
    matplotlib.pyplot.rcParams["path.simplify"] = True
    matplotlib.pyplot.rcParams["path.simplify_threshold"] = 0.1
    matplotlib.pyplot.rcParams["agg.path.chunksize"] = 10000

    fig, ax = matplotlib.pyplot.subplots()

    # 散布図の作成（rasterizedで高速化）
    sc = ax.scatter(
        times,
        altitudes,
        c=temperatures,
        cmap="plasma",
        marker="o",
        s=8,  # マーカーサイズを少し小さく
        rasterized=True,  # ベクター描画を無効にして高速化
        edgecolors="none",  # エッジを無効にして高速化
    )
    sc.set_clim(-80, 30)
    ax.set_ylim(0, 14000)

    # 軸ラベルの設定（日本語ラベル）
    ax.set_xlabel("日時")
    ax.set_ylabel("高度 (m)")

    # カラーバーの追加
    cbar = matplotlib.pyplot.colorbar(sc)
    cbar.set_label("温度 (°C)")

    # 時刻軸のラベルを日付形式に設定（日本語フォーマット）
    # データの時間範囲を取得
    time_range = mdates.date2num(times[-1]) - mdates.date2num(times[0])

    # 時間範囲に応じて適切な間隔を設定（約3本のメモリ）
    if time_range <= 1:  # 1日以内
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=8))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-H時"))
    elif time_range <= 3:  # 3日以内
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-d日\n%-H時"))
    elif time_range <= 7:  # 1週間以内
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))
    else:  # それ以上
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=int(time_range / 3)))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))

    from matplotlib.ticker import StrMethodFormatter

    ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))

    # グリッドとレイアウトの調整
    matplotlib.pyplot.grid()

    # タイトルを設定（Boldフォント）
    try:
        bold_prop = font_manager.FontProperties(fname=bold_font_path)
        ax.set_title("航空機の気象データ", fontsize=20, fontproperties=bold_prop, pad=10)
    except Exception:
        ax.set_title("航空機の気象データ", fontsize=20, fontweight="bold", pad=10)

    matplotlib.pyplot.tight_layout()

    matplotlib.pyplot.savefig(filename, format="png", dpi=200, transparent=True)
    logging.info("2Dプロットを保存しました: %s", filename)


def plot(data_list):
    import concurrent.futures

    # データの前処理を一度だけ実行してキャッシュ
    logging.info("データ前処理を開始...")
    clean_df = _prepare_data(data_list)

    if len(clean_df) == 0:
        logging.warning("プロット用のデータがありません")
        return

    # 2つのプロット生成を並列実行（前処理済みデータを使用）
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # 前処理済みデータを直接渡すための専用関数を使用
        future_2d = executor.submit(_plot_2d_from_clean_data, clean_df, "a.png")
        future_3d = executor.submit(_plot_3d_from_clean_data, clean_df, "3d_plot.png")

        # 両方の処理が完了するまで待機
        concurrent.futures.wait([future_2d, future_3d])

        # エラーがあった場合は例外を発生させる
        future_2d.result()
        future_3d.result()


def _set_time_axis_format(ax, time_data):
    """時間軸のフォーマットを設定する共通関数"""
    time_range = mdates.date2num(time_data[-1]) - mdates.date2num(time_data[0])

    # 時間範囲に応じて適切な間隔を設定（約3本のメモリ）
    if time_range <= 1:  # 1日以内
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=8))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-H時"))
    elif time_range <= 3:  # 3日以内
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-d日\n%-H時"))
    elif time_range <= 7:  # 1週間以内
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))
    else:  # それ以上
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=int(time_range / 3)))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))


def _set_plot_title(ax, title, fontsize=24):
    """プロットタイトルを設定する共通関数"""
    try:
        bold_prop = font_manager.FontProperties(fname=bold_font_path)
        ax.set_title(title, fontsize=fontsize, fontproperties=bold_prop, pad=10)
    except Exception:
        ax.set_title(title, fontsize=fontsize, fontweight="bold", pad=10)


def _configure_plot_performance():
    """プロット描画の高速化設定"""
    matplotlib.pyplot.rcParams["path.simplify"] = True
    matplotlib.pyplot.rcParams["path.simplify_threshold"] = 0.1
    matplotlib.pyplot.rcParams["agg.path.chunksize"] = 10000


def _plot_2d_from_clean_data(clean_df, filename):
    """前処理済みデータから2Dプロットを生成（内部使用）"""
    logging.info("2Dプロット生成開始: %s", filename)

    # NumPy配列への変換で高速化
    times = clean_df["time"].to_numpy()
    altitudes = clean_df["altitude"].to_numpy()
    temperatures = clean_df["temperature"].to_numpy()

    # 高速化設定
    _configure_plot_performance()

    # 明示的に新しいfigureを作成
    fig, ax = matplotlib.pyplot.subplots()

    # 散布図の作成（rasterizedで高速化）
    sc = ax.scatter(
        times, altitudes, c=temperatures, cmap="plasma", marker="o", s=8, rasterized=True, edgecolors="none"
    )
    sc.set_clim(-80, 30)
    ax.set_ylim(0, 14000)

    # 軸ラベルの設定（日本語ラベル）
    ax.set_xlabel("日時")
    ax.set_ylabel("高度 (m)")

    # カラーバーの追加
    cbar = matplotlib.pyplot.colorbar(sc)
    cbar.set_label("温度 (°C)")

    # 時間軸のフォーマット設定
    _set_time_axis_format(ax, times)

    # 高度軸にカンマ区切りフォーマッターを追加
    from matplotlib.ticker import StrMethodFormatter

    ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))

    # グリッドとレイアウトの調整
    matplotlib.pyplot.grid()
    _set_plot_title(ax, "航空機の気象データ")
    matplotlib.pyplot.tight_layout()

    matplotlib.pyplot.savefig(filename, format="png", dpi=200, transparent=True)
    matplotlib.pyplot.close(fig)  # メモリ解放
    logging.info("2Dプロットを保存しました: %s", filename)


def _set_3d_time_axis_format(ax, time_numeric):
    """3D用の時間軸フォーマット設定"""
    time_range = time_numeric[-1] - time_numeric[0]

    if time_range <= 1:  # 1日以内
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=8))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-H時"))
    elif time_range <= 3:  # 3日以内
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))
    elif time_range <= 7:  # 1週間以内
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))
    else:  # それ以上
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=int(time_range / 3)))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m月%-d日"))


def _plot_3d_from_clean_data(clean_df, filename):
    """前処理済みデータから3Dプロットを生成（内部使用）"""
    logging.info("3Dプロット生成開始: %s", filename)

    # NumPy配列への変換で高速化
    time_numeric = np.array([mdates.date2num(t) for t in clean_df["time"]])
    altitudes = clean_df["altitude"].to_numpy()
    temperatures = clean_df["temperature"].to_numpy()

    # 高速化設定
    _configure_plot_performance()

    # 明示的に新しいfigureを作成
    fig = matplotlib.pyplot.figure(figsize=(14, 10))
    ax = fig.add_subplot(111, projection="3d")

    # 3D散布図の作成（rasterizedで高速化）
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

    # 軸ラベルの設定（日本語ラベル、2Dと同じフォントサイズ）
    ax.set_xlabel("日時", labelpad=5, fontsize=12)
    ax.set_ylabel("高度 (m)", labelpad=12, fontsize=12)
    ax.set_zlabel("温度 (°C)", labelpad=8, fontsize=12)

    # 時間軸のフォーマット設定
    _set_3d_time_axis_format(ax, time_numeric)

    # 高度軸にカンマ区切りフォーマッターを追加
    from matplotlib.ticker import StrMethodFormatter

    ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))

    # 軸メモリのフォントサイズ（2Dと同じ）
    ax.tick_params(axis="x", labelsize=10)
    ax.tick_params(axis="y", labelsize=10)
    ax.tick_params(axis="z", labelsize=10)

    # 軸の範囲設定
    ax.set_ylim(0, 14000)
    ax.set_zlim(-80, 30)

    # カラーバーの追加と範囲設定
    scatter.set_clim(-80, 30)
    cbar = matplotlib.pyplot.colorbar(scatter, shrink=0.8, pad=0.15)
    cbar.set_label("温度 (°C)", fontsize=12)
    cbar.ax.tick_params(labelsize=10)

    # 視点の設定
    ax.view_init(elev=25, azim=35)

    # レイアウト調整
    matplotlib.pyplot.subplots_adjust(left=0.01, right=0.99, top=0.95, bottom=0.05)
    ax.set_position([0.05, 0.08, 0.70, 0.82])

    # タイトル設定
    _set_plot_title(ax, "航空機の気象データ(3D)")

    # ファイル保存
    matplotlib.pyplot.savefig(filename, format="png", dpi=200, transparent=True, bbox_inches=None)
    matplotlib.pyplot.close(fig)  # メモリ解放
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
