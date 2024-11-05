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

import matplotlib.pyplot
import numpy as np
import pandas as pd
from matplotlib import dates as mdates
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler

# NOTE: 温度がこれより高いデータのみ残す
TEMPERATURE_THRESHOLD = -100

# def prep_time_alt_temp(data_list):
#     data_list = [d for d in data_list if d["temperature"] > TEMPERATURE_THRESHOLD]

#     data_map = {key: [d[key] for d in data_list] for key in ["temperature", "altitude", "time"]}
#     data_map["timestamp"] = map(lambda x: x.timestamp(), data_map["time"])

#     period_day = (data_map["time"][-1] - data_map["time"][0]).total_seconds() / (60 * 60 * 24)

#     df = pd.DataFrame(data_map)

#     scaler = StandardScaler()
#     df[["altitude_scaled", "time_scaled"]] = scaler.fit_transform(df[["altitude", "timestamp"]])
#     knn = KNeighborsRegressor(n_neighbors=20)
#     x = df[["altitude_scaled", "time_scaled"]]
#     y = df["temperature"]
#     knn.fit(x, y)
#     df["temperature_predicted"] = knn.predict(x)

#     df["temperature_diff"] = df["temperature"] - df["temperature_predicted"]
#     sigma = 3  # 標準偏差の閾値
#     df["outliers"] = np.abs(df["temperature_diff"]) > sigma * df["temperature_diff"].std()

#     # 外れ値フラグが立っているデータを表示
#     # print(df[df["外れ値"]])

#     # # 外れ値を除去したデータフレーム
#     clean_df = df[~df["outliers"]].drop(
#         columns=["altitude_scaled", "time_scaled", "temperature_predicted", "temperature_diff", "outliers"]
#     )

#     return clean_df


def weighted_distance(X1, X2, altitude_weight=1.0, time_weight=1.0):
    weight = np.array([1.0, 2.0])  # feature1の重みは1.0, feature2の重みは2.0

    return np.sqrt(np.sum(weight * (X1 - X2) ** 2))


#     # if X2.ndim == 1:
#     #     X2 = X2.reshape(1, -1)
#     # # 重み付きユークリッド距離の計算
#     # return np.sqrt(np.sum(weight * (X1 - X2) ** 2, axis=1))


# カスタム距離関数の定義
# def weighted_distance(X, Y, altitude_weight=1.0, time_weight=1.0):
#     d_altitude = np.abs(X[:, 0] - Y[:, 0]) * altitude_weight
#     d_time = np.abs(X[:, 1] - Y[:, 1]) * time_weight
#     return np.sqrt(d_altitude**2 + d_time**2)


# KNeighborsRegressorを使用して外れ値を除去する関数
def remove_outliers(data_list, n_neighbors=20, threshold=3, altitude_weight=1.0, time_weight=1.0):
    data_list = [d for d in data_list if d["temperature"] > TEMPERATURE_THRESHOLD]

    data_map = {key: [d[key] for d in data_list] for key in ["temperature", "altitude", "time"]}
    data_map["timestamp"] = map(lambda x: x.timestamp(), data_map["time"])

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
    logging.info("A")
    knn.fit(X, y)
    logging.info("B")
    # 全データポイントに対して予測を実行
    df["predicted_temp"] = knn.predict(X)
    logging.info("C")
    # 温度の差がしきい値を超える場合に外れ値とする
    df["temp_diff"] = np.abs(df["temperature"] - df["predicted_temp"])
    clean_df = df[df["temp_diff"] <= threshold]

    return clean_df.drop(columns=["predicted_temp", "temp_diff"])


def prep_time_alt_temp2(data_list, altitude_window=500, time_window=12 * 3600, threshold=20):
    data_list = [d for d in data_list if d["temperature"] > TEMPERATURE_THRESHOLD]

    data_map = {key: [d[key] for d in data_list] for key in ["temperature", "altitude", "time"]}
    data_map["timestamp"] = map(lambda x: x.timestamp(), data_map["time"])

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

            # knn = KNeighborsRegressor(n_neighbors=10, weights='distance')
            # knn.fit(X_local, y_local)

            scaler = StandardScaler()
            df[["altitude_scaled", "time_scaled"]] = scaler.fit_transform(df[["altitude", "timestamp"]])
            knn = KNeighborsRegressor(n_neighbors=min(20, len(local_data)))
            # x = df[["altitude_scaled", "time_scaled"]]
            # y = df["temperature"]
            knn.fit(x_local, y_local)

            # df["temperature_predicted"] = knn.predict(x)

            predicted_temp = knn.predict([current_point[["altitude", "timestamp"]]])
            temp_diff = abs(current_point["temperature"] - predicted_temp[0])

            if temp_diff <= threshold:
                clean_data.append(current_point)

    # clean_df = pd.DataFrame(clean_data).reset_index(drop=True)
    return clean_data


# # 外れ値の除去を実行
# clean_df = remove_outliers(df)


#     return clean_df


def plot(data_list):
    time_list = []
    altitude_list = []
    temperature_list = []

    # clean_df = prep_time_alt_temp2(data_list)

    clean_df = remove_outliers(data_list)

    fig = matplotlib.pyplot.figure()

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
    sc.set_clim(-70, 20)
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

    # for label in ax.get_xticklabels():
    #     label.set_fontproperties(face_map["axis_major"])
    # for label in ax.get_xminorticklabels():
    #     label.set_fontproperties(face_map["axis_minor"])

    # グリッドとレイアウトの調整
    matplotlib.pyplot.grid()
    # fig.autofmt_xdate()
    matplotlib.pyplot.tight_layout()

    matplotlib.pyplot.savefig("a.png", format="png", dpi=200, transparent=True)


if __name__ == "__main__":
    import local_lib.config
    import local_lib.logger
    import modes.database
    from docopt import docopt

    args = docopt(__doc__)

    local_lib.logger.init("ModeS sensing", level=logging.INFO)

    config_file = args["-c"]
    config = local_lib.config.load(args["-c"])

    sqlite = modes.database.open(config["database"]["path"])
    time_end = datetime.datetime.now()
    time_start = time_end - datetime.timedelta(hours=600)

    plot(modes.database.fetch_by_time(sqlite, time_start, time_end))
