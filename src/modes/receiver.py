#!/usr/bin/env python3
"""
ModeS のメッセージを解析し，上空の温度と風速を算出して出力します．

Usage:
  receiver.py [-c CONFIG] [-D]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
  -D                : デバッグモードで動作します．
"""
# 参考: https://www.ishikawa-lab.com/RasPi_ModeS.html

from __future__ import annotations

import collections
import logging
import math
import pathlib
import queue
import socket
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict

import my_lib.footprint
import my_lib.notify.slack
import numpy as np

if TYPE_CHECKING:
    import multiprocessing
    from collections.abc import Generator

    from modes.config import Area, Config

import numpy.typing as npt
import pyModeS
import sklearn.ensemble
import sklearn.linear_model

from modes.database_postgresql import MeasurementData, WindData

# receiver.py内ではMeteorologicalDataという名前で使用
MeteorologicalData = MeasurementData


@dataclass
class HistoryData:
    """履歴データ（外れ値検出用）"""

    altitude: float
    temperature: float


class MessageFragment(TypedDict, total=False):
    """メッセージフラグメント"""

    icao: str
    adsb_pos: tuple[float, float | None, float | None]
    adsb_sign: tuple[str]
    bsd50: tuple[float | None, float | None, float | None]
    bsd60: tuple[float | None, float | None, float | None]


FRAGMENT_BUF_SIZE: int = 100

# 再接続設定
RECONNECT_MAX_RETRIES: int = 10
RECONNECT_BASE_DELAY: float = 2.0
RECONNECT_MAX_DELAY: float = 60.0
SOCKET_TIMEOUT: float = 30.0

fragment_list: list[MessageFragment] = []

should_terminate = threading.Event()

# receiver専用Livenessファイルパス（start()で設定される）
_receiver_liveness_file: pathlib.Path = pathlib.Path()

# Slack通知設定
_slack_config: my_lib.notify.slack.SlackErrorOnlyConfig | my_lib.notify.slack.SlackEmptyConfig = (
    my_lib.notify.slack.SlackEmptyConfig()
)

HISTRY_SAMPLES: int = 30000
meteorological_history: collections.deque[HistoryData] = collections.deque(maxlen=HISTRY_SAMPLES)
OUTLIER_DETECTION_MIN_SAMPLES: int = 100  # 外れ値検出を開始する最小サンプル数


def receive_lines(sock: socket.socket) -> Generator[str, None, None]:
    buffer = b""

    while True:
        data = sock.recv(1024)

        if data is None:
            return

        buffer += data
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            yield line.decode()


def calc_temperature(trueair: float, mach: float) -> float:
    k = 1.403  # 比熱比(空気)
    M = 28.966e-3  # 分子量(空気) [kg/mol]
    R = 8.314472  # 気体定数

    K = M / k / R

    return (trueair / mach) * (trueair / mach) * K - 273.15


def calc_magnetic_declination(latitude: float, longitude: float) -> float:
    # NOTE:
    # 地磁気値(2020.0年値)を求める
    # https://vldb.gsi.go.jp/sokuchi/geomag/menu_04/
    delta_latitude = latitude - 37
    delta_longitude = longitude - 138

    return (
        (8 + 15.822 / 60)
        + (18.462 / 60) * delta_latitude
        - (7.726 / 60) * delta_longitude
        + (0.007 / 60) * delta_latitude * delta_latitude
        + (0.007 / 60) * delta_latitude * delta_longitude
        - (0.655 / 60) * delta_longitude * delta_longitude
    )


def calc_wind(
    latitude: float,
    longitude: float,
    trackangle: float,
    groundspeed: float,
    heading: float,
    trueair: float,
) -> WindData:
    magnetic_declination = calc_magnetic_declination(latitude, longitude)

    ground_dir = math.pi / 2 - math.radians(trackangle)
    ground_x = groundspeed * math.cos(ground_dir)
    ground_y = groundspeed * math.sin(ground_dir)
    air_dir = math.pi / 2 - math.radians(heading) + math.radians(magnetic_declination)
    air_x = trueair * math.cos(air_dir)
    air_y = trueair * math.sin(air_dir)

    wind_x = ground_x - air_x
    wind_y = ground_y - air_y

    return WindData(
        x=wind_x,
        y=wind_y,
        # NOTE: 北を 0 として，風が来る方の角度
        angle=math.degrees(
            (math.pi / 2 - math.atan2(wind_y, wind_x) + 2 * math.pi + math.pi) % (2 * math.pi)
        ),
        speed=math.sqrt(wind_x * wind_x + wind_y * wind_y),
    )


def calc_meteorological_data(
    callsign: str,
    altitude: float,
    latitude: float,
    longitude: float,
    trackangle: float,
    groundspeed: float,
    trueair: float,
    heading: float,
    indicatedair: float,
    mach: float,
    distance: float,
) -> MeteorologicalData:
    altitude *= 0.3048  # 単位換算: feet →  mete
    groundspeed *= 0.514  # 単位換算: knot → m/s
    trueair *= 0.514

    temperature = calc_temperature(trueair, mach)
    wind = calc_wind(latitude, longitude, trackangle, groundspeed, heading, trueair)

    if temperature < -100:
        logging.warning(
            "温度が異常なので捨てます．(callsign: %s, temperature: %.1f, "
            "altitude: %s, trueair: %s, mach: %s)",
            callsign,
            temperature,
            altitude,
            trueair,
            mach,
        )
    return MeasurementData(
        callsign=callsign,
        altitude=altitude,
        latitude=latitude,
        longitude=longitude,
        temperature=temperature,
        wind=wind,
        distance=distance,
    )


def is_physically_reasonable(
    altitude: float,
    temperature: float,
    regression_model: sklearn.linear_model.LinearRegression,
    tolerance_factor: float = 4,
) -> bool:
    """
    高度-温度の物理的相関が妥当かどうかを判定

    Args:
        altitude: 高度
        temperature: 気温
        regression_model: 学習済み線形回帰モデル
        tolerance_factor: 許容範囲の倍率

    Returns:
        物理的に妥当な場合True

    """
    try:
        # 予測温度を計算
        predicted_temp = regression_model.predict([[altitude]])[0]

        # 高度-温度の一般的な関係：高度が1000m上がると約6.5°C下がる
        # 標準大気での温度減率を考慮した許容範囲を設定
        standard_lapse_rate = 0.0065  # °C/m
        altitude_diff_threshold = 200  # m（許容する高度差）
        temp_tolerance = standard_lapse_rate * altitude_diff_threshold * tolerance_factor

        # 予測値との差が許容範囲内かチェック
        residual = abs(temperature - predicted_temp)

        judge = residual <= temp_tolerance

        if judge:
            logging.info(
                "物理的に妥当な高度-温度相関のため正常値として扱います "
                "(altitude: %.1fm, temperature: %.1f°C, predicted_temp=%.1f°C, residual=%.1f°C)",
                altitude,
                temperature,
                predicted_temp,
                residual,
            )

        return judge

    except Exception:
        return True  # エラー時は保守的に妥当とみなす


def detect_outlier_by_altitude_neighbors(
    altitude: float,
    temperature: float,
    altitudes: npt.NDArray[np.floating[Any]],
    temperatures: npt.NDArray[np.floating[Any]],
    callsign: str,
    n_neighbors: int = 200,
    deviation_threshold: float = 20,
    sigma_threshold: float = 4,
) -> bool:
    """
    高度近傍ベースの異常検知を実行

    高度が近いデータポイントの局所的な分布を使用して異常値を検出します。
    低高度では温度のばらつきが大きいという特性に対応できます。

    Args:
        altitude: 検査対象の高度
        temperature: 検査対象の気温
        altitudes: 履歴データの高度配列
        temperatures: 履歴データの温度配列
        callsign: 航空機のコールサイン（ログ用）
        n_neighbors: 使用する近傍データ数（デフォルト: 200）
        deviation_threshold: 絶対偏差による異常値判定閾値（デフォルト: 20）
        sigma_threshold: 異常値判定のシグマ閾値（デフォルト: 4）

    Returns:
        外れ値の場合True、正常値の場合False

    """
    # 高度差を計算
    altitude_diffs = np.abs(altitudes.flatten() - altitude)

    # 最も近い高度のインデックスを取得（最大n_neighbors個）
    n_actual = min(n_neighbors, len(altitude_diffs))
    nearest_indices = np.argpartition(altitude_diffs, n_actual - 1)[:n_actual]

    # 近傍データの温度を取得
    neighbor_temps = temperatures.flatten()[nearest_indices]
    neighbor_alts = altitudes.flatten()[nearest_indices]

    # 近傍データの平均高度と温度統計を計算
    mean_neighbor_alt = np.mean(neighbor_alts)
    mean_temp = np.mean(neighbor_temps)
    std_temp = np.std(neighbor_temps)

    # 温度の偏差を計算
    temp_deviation = abs(temperature - mean_temp)
    z_score = temp_deviation / std_temp if std_temp > 0 else 0

    # 異常値判定
    is_outlier = (temp_deviation > deviation_threshold) or (z_score > sigma_threshold)

    # 判定結果をログ出力
    if is_outlier:
        logging.warning(
            "%s: callsign=%s, altitude=%.1fm, temperature=%.1f°C, "
            "neighbor_mean_alt=%.1fm, neighbor_mean_temp=%.1f°C, neighbor_std=%.1f°C, "
            "deviation=%.1f°C(threshold=%.1f), z_score=%.2f (threshold=%.1f)",
            "外れ値検出（高度近傍）",
            callsign or "Unknown",
            altitude,
            temperature,
            mean_neighbor_alt,
            mean_temp,
            std_temp,
            temp_deviation,
            deviation_threshold,
            z_score,
            sigma_threshold,
        )
    else:
        logging.info(
            "%s: callsign=%s, altitude=%.1fm, temperature=%.1f°C, "
            "neighbor_mean_alt=%.1fm, neighbor_mean_temp=%.1f°C, neighbor_std=%.1f°C, "
            "deviation=%.1f°C(threshold=%.1f), z_score=%.2f (threshold=%.1f)",
            "正常値判定（高度近傍）",
            callsign or "Unknown",
            altitude,
            temperature,
            mean_neighbor_alt,
            mean_temp,
            std_temp,
            temp_deviation,
            deviation_threshold,
            z_score,
            sigma_threshold,
        )

    return bool(is_outlier)


def is_outlier_data(
    temperature: float,
    altitude: float,
    callsign: str,
) -> bool:
    """
    高度-温度相関を考慮してaltitudeとtemperatureのペアが外れ値かどうかを判定

    二段階アプローチ：
    1. 物理的相関チェック（高度が低い→温度が高い関係を保護）
    2. 残差ベースの異常検知

    Args:
        temperature: 気温
        altitude: 高度
        callsign: 航空機のコールサイン（ログ用）

    Returns:
        外れ値の場合True、正常値の場合False

    """
    global meteorological_history

    # データが十分蓄積されていない場合は外れ値として扱わない
    if len(meteorological_history) < OUTLIER_DETECTION_MIN_SAMPLES:
        return False

    try:
        # 履歴データから特徴量を抽出
        valid_data = [
            data
            for data in meteorological_history
            if data.altitude is not None and data.temperature is not None
        ]

        if len(valid_data) < OUTLIER_DETECTION_MIN_SAMPLES:
            return False

        altitudes = np.array([[data.altitude] for data in valid_data])
        temperatures = np.array([data.temperature for data in valid_data])

        # 第一段階：線形回帰で高度-温度関係を学習
        regression_model = sklearn.linear_model.LinearRegression()
        regression_model.fit(altitudes, temperatures)

        # 物理的相関チェック（より寛容に）
        if is_physically_reasonable(altitude, temperature, regression_model, tolerance_factor=2.5):
            return False  # 物理的に妥当なので外れ値ではない

        # 第二段階：高度近傍ベースの異常検知（低高度でのばらつき対応）
        return detect_outlier_by_altitude_neighbors(altitude, temperature, altitudes, temperatures, callsign)

    except Exception as e:
        logging.warning("外れ値検出でエラーが発生しました: %s", e)
        return False


def calc_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0

    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # NOTE: ハバースインの公式
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return R * c


def round_floats(obj: Any, ndigits: int = 1) -> Any:
    if isinstance(obj, float):
        return round(obj, ndigits)
    elif isinstance(obj, dict):
        return {k: round_floats(v, ndigits) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [round_floats(elem, ndigits) for elem in obj]
    elif isinstance(obj, tuple):
        return tuple(round_floats(elem, ndigits) for elem in obj)
    else:
        return obj


def _is_fragment_complete(fragment: MessageFragment) -> bool:
    """フラグメントが完全かどうかを判定する"""
    required_types = ["adsb_pos", "adsb_sign", "bsd50", "bsd60"]
    return all(packet_type in fragment for packet_type in required_types)


def _process_complete_fragment(
    fragment: MessageFragment,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """完全なフラグメントを処理してキューに送信する"""
    global meteorological_history

    # TypedDict の各キーを .get() で安全に取得
    adsb_pos = fragment.get("adsb_pos")
    adsb_sign = fragment.get("adsb_sign")
    bsd50 = fragment.get("bsd50")
    bsd60 = fragment.get("bsd60")

    # 全てのデータが揃っていることを確認（_is_fragment_complete で確認済みだが型チェック用）
    if adsb_pos is None or adsb_sign is None or bsd50 is None or bsd60 is None:
        return

    # タプル内の値が None でないことを確認
    if adsb_pos[1] is None or adsb_pos[2] is None:
        return
    if any(v is None for v in bsd50) or any(v is None for v in bsd60):
        return

    distance = calc_distance(
        area_config.lat.ref,
        area_config.lon.ref,
        adsb_pos[1],
        adsb_pos[2],
    )
    # NOTE: 上記の None チェック後でもタプル要素の型は絞り込まれないため type: ignore が必要
    meteorological_data = calc_meteorological_data(
        *adsb_sign,
        *adsb_pos,  # type: ignore[arg-type]
        *bsd50,  # type: ignore[arg-type]
        *bsd60,  # type: ignore[arg-type]
        distance,
    )

    # 温度異常値は外れ値検出の対象外
    if meteorological_data.temperature < -100:
        logging.debug("温度異常値のため外れ値検出をスキップ")
        return

    # 外れ値検出
    is_outlier = is_outlier_data(
        meteorological_data.temperature,
        meteorological_data.altitude,
        meteorological_data.callsign,
    )
    if is_outlier:
        return

    # 正常値の場合、queueに送信し履歴に追加
    logging.info(round_floats(meteorological_data))
    data_queue.put(meteorological_data)
    meteorological_history.append(
        HistoryData(
            altitude=meteorological_data.altitude,
            temperature=meteorological_data.temperature,
        )
    )


def _add_new_fragment(icao: str, packet_type: str, data: tuple[Any, ...]) -> None:
    """新しいフラグメントをリストに追加する"""
    global fragment_list

    # 動的キーを使用するため type: ignore が必要
    fragment_list.append({"icao": icao, packet_type: data})  # type: ignore[list-item, misc]
    if len(fragment_list) >= FRAGMENT_BUF_SIZE:
        fragment_list.pop(0)


def message_pairing(
    icao: str,
    packet_type: str,
    data: tuple[Any, ...],
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """メッセージフラグメントをペアリングして気象データを生成する"""
    global fragment_list

    if not all(value is not None for value in data):
        logging.warning("データに欠損があるので捨てます．(type: %s, data: %s)", packet_type, data)
        return

    fragment = next((f for f in fragment_list if f.get("icao") == icao), None)

    if fragment is None:
        _add_new_fragment(icao, packet_type, data)
        return

    # 動的キーを使用するため type: ignore が必要
    fragment[packet_type] = data  # type: ignore[literal-required]

    if not _is_fragment_complete(fragment):
        return

    _process_complete_fragment(fragment, data_queue, area_config)
    fragment_list.remove(fragment)


def _process_adsb_position(
    message: str,
    icao: str,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """ADS-B位置情報メッセージを処理する"""
    altitude = pyModeS.adsb.altitude(message)
    if altitude == 0:
        return

    latitude, longitude = pyModeS.adsb.position_with_ref(message, area_config.lat.ref, area_config.lon.ref)
    message_pairing(icao, "adsb_pos", (altitude, latitude, longitude), data_queue, area_config)


def _process_adsb_message(
    message: str,
    icao: str,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """ADS-Bメッセージ（dformat=17）を処理する"""
    logging.debug("receive ADSB")
    code = pyModeS.typecode(message)

    if code is None:
        return

    # 位置情報（typecode 5-18, 20-22）
    if (5 <= code <= 18) or (20 <= code <= 22):
        _process_adsb_position(message, icao, data_queue, area_config)
    # コールサイン（typecode 1-4）
    elif 1 <= code <= 4:
        callsign = pyModeS.adsb.callsign(message).rstrip("_")
        message_pairing(icao, "adsb_sign", (callsign,), data_queue, area_config)


def _process_commb_message(
    message: str,
    icao: str,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """Comm-Bメッセージ（dformat=20,21）を処理する"""
    if pyModeS.bds.bds50.is50(message):
        logging.debug("receive BDS50")
        trackangle = pyModeS.commb.trk50(message)
        groundspeed = pyModeS.commb.gs50(message)
        trueair = pyModeS.commb.tas50(message)
        message_pairing(icao, "bsd50", (trackangle, groundspeed, trueair), data_queue, area_config)

    elif pyModeS.bds.bds60.is60(message):
        logging.debug("receive BDS60")
        heading = pyModeS.commb.hdg60(message)
        indicatedair = pyModeS.commb.ias60(message)
        mach = pyModeS.commb.mach60(message)
        message_pairing(icao, "bsd60", (heading, indicatedair, mach), data_queue, area_config)


def process_message(
    message: str,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """受信したMode-Sメッセージを解析して処理する"""
    logging.debug("receive: %s", message)

    if len(message) < 2:
        return

    # NOTE: 先頭と末尾の文字を除去
    message = message[1:-1]

    if len(message) < 22:
        return

    icao = str(pyModeS.icao(message))
    dformat = pyModeS.df(message)

    if dformat == 17:
        _process_adsb_message(message, icao, data_queue, area_config)
    elif dformat in (20, 21):
        _process_commb_message(message, icao, data_queue, area_config)


def _calculate_retry_delay(retry_count: int) -> float:
    """指数バックオフで再接続遅延時間を計算する"""
    return min(RECONNECT_BASE_DELAY * (2 ** (retry_count - 1)), RECONNECT_MAX_DELAY)


def _wait_with_interrupt(delay: float) -> None:
    """中断可能な待機を行う"""
    for _ in range(int(delay * 10)):
        if should_terminate.is_set():
            break
        time.sleep(0.1)


def _process_socket_messages(
    sock: socket.socket,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """ソケットからメッセージを受信して処理する"""
    for line in receive_lines(sock):
        if should_terminate.is_set():
            break

        try:
            process_message(line, data_queue, area_config)

            # データ受信成功時にLivenessファイル更新
            my_lib.footprint.update(_receiver_liveness_file)

        except Exception:
            logging.exception("メッセージ処理に失敗しました")


def _handle_connection(
    host: str,
    port: int,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> bool:
    """TCP接続を確立しメッセージを処理する

    Returns:
        接続が正常に閉じられた場合True、エラーの場合False

    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(SOCKET_TIMEOUT)
        sock.connect((host, port))
        logging.info("%s:%d に接続しました", host, port)

        _process_socket_messages(sock, data_queue, area_config)

        if should_terminate.is_set():
            return True

        logging.warning("リモートホストによって接続が閉じられました")
        return True


def worker(
    host: str,
    port: int,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """再接続機能付きワーカー

    TCP接続が切断された場合、指数バックオフで再接続を試みます。
    最大リトライ回数に達した場合のみワーカーを終了します。
    """
    logging.info("受信ワーカーを開始します")
    should_terminate.clear()
    retry_count = 0

    while not should_terminate.is_set():
        try:
            _handle_connection(host, port, data_queue, area_config)
            retry_count = 0  # 接続成功でリセット

            if should_terminate.is_set():
                break

        except TimeoutError:
            logging.warning("ソケットタイムアウトが発生しました")

        except (OSError, ConnectionError) as e:
            retry_count += 1
            if retry_count > RECONNECT_MAX_RETRIES:
                error_message = f"最大再接続回数（{RECONNECT_MAX_RETRIES}回）に達しました。処理を終了します"
                logging.error(error_message)
                my_lib.notify.slack.error(
                    _slack_config,
                    "Mode-S受信エラー",
                    f"{error_message}\n接続先: {host}:{port}\n最後のエラー: {e}",
                )
                break

            delay = _calculate_retry_delay(retry_count)
            logging.warning(
                "接続に失敗しました（%d/%d回目）: %s。%.1f秒後に再試行します...",
                retry_count,
                RECONNECT_MAX_RETRIES,
                e,
                delay,
            )
            _wait_with_interrupt(delay)

        except Exception:
            logging.exception("受信ワーカーで予期しないエラーが発生しました")
            break

    logging.warning("受信ワーカーを停止します")


def init(data: list[HistoryData]) -> None:
    meteorological_history.extend(data)


def start(
    config: Config,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
) -> threading.Thread:
    """receiverワーカースレッドを開始する

    Args:
        config: アプリケーション設定
        data_queue: データを送信するキュー

    Returns:
        開始されたスレッド

    """
    global _receiver_liveness_file, _slack_config
    _receiver_liveness_file = config.liveness.file.receiver
    _slack_config = config.slack

    thread = threading.Thread(
        target=worker,
        args=(
            config.modes.decoder.host,
            config.modes.decoder.port,
            data_queue,
            config.filter.area,
        ),
    )
    thread.start()

    return thread


def term() -> None:
    should_terminate.set()


if __name__ == "__main__":
    import docopt
    import my_lib.config
    import my_lib.logger

    from modes.config import load_from_dict

    assert __doc__ is not None  # noqa: S101
    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    debug_mode = args["-D"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config_dict = my_lib.config.load(config_file)
    config = load_from_dict(config_dict, pathlib.Path.cwd())

    measurement_queue: queue.Queue[MeteorologicalData] = queue.Queue()

    start(config, measurement_queue)

    while True:
        logging.info(measurement_queue.get())

        if should_terminate.is_set():
            break
