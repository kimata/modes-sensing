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
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler

# カスタムフォントの設定
font_path = "font/FuturaStd-Medium.otf"
font_manager.fontManager.addfont(font_path)
matplotlib.pyplot.rcParams["font.family"] = "Futura Std"

# NOTE: 温度がこれより高いデータのみ残す
TEMPERATURE_THRESHOLD = -100


def weighted_distance(x1, x2, altitude_weight=1.0, time_weight=1.0):  # noqa: ARG001
    weight = np.array([1.0, 2.0])  # feature1の重みは1.0, feature2の重みは2.0

    return np.sqrt(np.sum(weight * (x1 - x2) ** 2))


# KNeighborsRegressorを使用して外れ値を除去する関数
def remove_outliers(data_list, n_neighbors=20, threshold=3, altitude_weight=1.0, time_weight=1.0):
    data_list = [d for d in data_list if d["temperature"] > TEMPERATURE_THRESHOLD]

    data_map = {key: [d[key] for d in data_list] for key in ["temperature", "altitude", "time"]}
    data_map["timestamp"] = [x.timestamp() for x in data_map["time"]]

    df = pd.DataFrame(data_map)

    X = df[["altitude", "timestamp"]]
    y = df["temperature"]

    # 近傍回帰モデルを訓練
    knn = KNeighborsRegressor(
        n_neighbors=n_neighbors,
        metric=weighted_distance,
        algorithm="ball_tree",
        metric_params={"altitude_weight": altitude_weight, "time_weight": time_weight},
        weights="distance",
    )

    knn.fit(X, y)

    # 全データポイントに対して予測を実行
    df["predicted_temp"] = knn.predict(X)

    # 温度の差がしきい値を超える場合に外れ値とする
    df["temp_diff"] = np.abs(df["temperature"] - df["predicted_temp"])
    clean_df = df[df["temp_diff"] <= threshold]

    return clean_df.drop(columns=["predicted_temp", "temp_diff"])


def prep_time_alt_temp2(data_list, altitude_window=500, time_window=12 * 3600, threshold=20):
    data_list = [d for d in data_list if d["temperature"] > TEMPERATURE_THRESHOLD]

    data_map = {key: [d[key] for d in data_list] for key in ["temperature", "altitude", "time"]}
    data_map["timestamp"] = [x.timestamp() for x in data_map["time"]]

    df = pd.DataFrame(data_map)

    clean_data = []
    for i in range(len(df)):
        current_point = df.iloc[i]

        # 高度と時刻で近傍のデータを選択
        mask = (
            (df["altitude"] >= current_point["altitude"] - altitude_window)
            & (df["altitude"] <= current_point["altitude"] + altitude_window)
            & (df["timestamp"] >= current_point["timestamp"] - time_window)
            & (df["timestamp"] <= current_point["timestamp"] + time_window)
        )
        local_data = df[mask]

        if len(local_data) > 1:
            x_local = local_data[["altitude", "timestamp"]]
            y_local = local_data["temperature"]

            scaler = StandardScaler()
            df[["altitude_scaled", "time_scaled"]] = scaler.fit_transform(df[["altitude", "timestamp"]])
            knn = KNeighborsRegressor(n_neighbors=min(20, len(local_data)))
            knn.fit(x_local, y_local)

            predicted_temp = knn.predict([current_point[["altitude", "timestamp"]]])
            temp_diff = abs(current_point["temperature"] - predicted_temp[0])

            if temp_diff <= threshold:
                clean_data.append(current_point)

    return clean_data


def plot_3d(data_list, filename="3d_plot.png"):
    """
    時間(x軸)、高度(y軸)、温度(z軸)の3次元プロットを生成

    Args:
        data_list: 気象データのリスト
        filename (str): 保存するファイル名

    """
    clean_df = remove_outliers(data_list)

    if len(clean_df) == 0:
        logging.warning("プロット用のデータがありません")
        return

    # 時間を数値に変換（matplotlibの日付形式）
    time_numeric = [mdates.date2num(t) for t in clean_df["time"]]

    # 3D図の作成（サイズを大きくしてラベル用の余白を確保）
    fig = matplotlib.pyplot.figure(figsize=(14, 10))
    ax = fig.add_subplot(111, projection="3d")

    # 3D散布図の作成
    scatter = ax.scatter(
        time_numeric,
        list(clean_df["altitude"]),
        list(clean_df["temperature"]),
        c=list(clean_df["temperature"]),
        cmap="plasma",
        marker="o",
        s=20,
        alpha=0.7,
    )

    # 軸ラベルの設定（altitudeラベルを少し離す、フォントサイズを大きく）
    ax.set_xlabel("Time", labelpad=5, fontsize=14)
    ax.set_ylabel("Altitude (m)", labelpad=12, fontsize=14)
    ax.set_zlabel("Temperature (°C)", labelpad=8, fontsize=14)

    # 時間軸のフォーマット設定
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%-m/%-d\n%-H:00"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))

    # 高度軸にカンマ区切りフォーマッターを追加
    from matplotlib.ticker import StrMethodFormatter

    ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))

    # 軸メモリのフォントサイズを大きく
    ax.tick_params(axis="x", labelsize=12)
    ax.tick_params(axis="y", labelsize=12)
    ax.tick_params(axis="z", labelsize=12)

    # 軸の範囲設定
    ax.set_ylim(0, 14000)
    ax.set_zlim(-80, 30)

    # カラーバーの追加と範囲設定（間隔をさらに確保、フォントサイズを大きく）
    scatter.set_clim(-80, 30)
    cbar = matplotlib.pyplot.colorbar(scatter, shrink=0.8, pad=0.25)
    cbar.set_label("Temperature (°C)", fontsize=16)
    cbar.ax.tick_params(labelsize=14)

    # 視点の設定（z軸ラベルが見えやすい角度に調整）
    ax.view_init(elev=25, azim=35)

    # 外側余白を削減してプロットエリアを拡大（タイトル用の上余白を確保）
    matplotlib.pyplot.subplots_adjust(left=0.02, right=0.82, top=0.90, bottom=0.02)

    # プロットエリアを調整（カラーバーとの間隔を確保、高さを増やす）
    ax.set_position([0.08, 0.02, 0.62, 0.85])

    # タイトルを上部に配置（フォントサイズをさらに大きく、位置を下に）
    ax.set_title("3D Meteorological Data\n(Time vs Altitude vs Temperature)", pad=10, fontsize=18)

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
    clean_df = remove_outliers(data_list)

    if len(clean_df) == 0:
        logging.warning("2Dプロット用のデータがありません")
        return

    fig, ax = matplotlib.pyplot.subplots()

    # 散布図の作成
    sc = ax.scatter(
        list(clean_df["time"]),
        list(clean_df["altitude"]),
        c=list(clean_df["temperature"]),
        cmap="plasma",
        marker="o",
        s=10,
    )
    sc.set_clim(-80, 30)
    ax.set_ylim(0, 14000)

    # 軸ラベルの設定
    ax.set_xlabel("Time")
    ax.set_ylabel("Altitude (m)")

    # カラーバーの追加
    cbar = matplotlib.pyplot.colorbar(sc)
    cbar.set_label("Temperature (°C)")

    # 時刻軸のラベルを日付形式に設定
    ax.xaxis.set_minor_locator(mdates.HourLocator(byhour=range(0, 24, 6)))
    ax.xaxis.set_minor_formatter(mdates.DateFormatter("%-H"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%-H\nDay %-d"))

    from matplotlib.ticker import StrMethodFormatter

    ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))

    # グリッドとレイアウトの調整
    matplotlib.pyplot.grid()
    matplotlib.pyplot.tight_layout()

    matplotlib.pyplot.savefig(filename, format="png", dpi=200, transparent=True)
    logging.info("2Dプロットを保存しました: %s", filename)


def plot(data_list):
    import concurrent.futures

    # 2つのプロット生成を並列実行
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # 2Dプロットと3Dプロットを並列で実行
        future_2d = executor.submit(plot_2d, data_list, "a.png")
        future_3d = executor.submit(plot_3d, data_list, "3d_plot.png")

        # 両方の処理が完了するまで待機
        concurrent.futures.wait([future_2d, future_3d])

        # エラーがあった場合は例外を発生させる
        future_2d.result()
        future_3d.result()


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
