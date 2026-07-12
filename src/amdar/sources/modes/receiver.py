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

import logging
import pathlib
import queue
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import my_lib.footprint
import my_lib.notify.slack
import my_lib.time
import pyModeS

if TYPE_CHECKING:
    import multiprocessing
    from collections.abc import Generator

    from amdar.config import Area, Config
    from amdar.sources.aggregator import IntegratedBuffer

import amdar.constants
import amdar.core.geo
import amdar.core.physics
import amdar.sources.outlier
from amdar.core.types import WeatherObservation
from amdar.core.types import WindData as CoreWindData
from amdar.database.postgresql import MeasurementData as MeteorologicalData

_FRAGMENT_BUF_SIZE: int = 100
"""フラグメントの最大保持件数"""

_FOOTPRINT_UPDATE_INTERVAL_SECONDS: float = 5.0
"""Liveness ファイル更新のスロットル間隔（秒）"""


@dataclass
class _MessageFragment:
    """ICAO ごとのメッセージフラグメント（リアルタイム受信・ファイル解析共用）"""

    icao: str
    callsign: str | None = None
    altitude_ft: float | None = None
    latitude: float | None = None
    longitude: float | None = None
    # BDS 5,0: (trackangle, groundspeed, trueair)
    bds50: tuple[float, float, float] | None = None
    # BDS 6,0: (heading, indicatedair, mach)
    bds60: tuple[float, float, float] | None = None
    # BDS 4,4: (temperature, wind_speed, wind_direction)
    bds44: tuple[float, float, float] | None = None
    updated_at: float = 0.0
    """最終更新時刻（time.time()、TTL 失効判定用）"""


@dataclass
class _ReceiverState:
    """モジュールの可変状態

    start() で初期化され、ワーカースレッドから参照されます。
    """

    fragments: dict[str, _MessageFragment] = field(default_factory=dict)
    """ICAO -> フラグメントのマッピング（リアルタイム受信用）"""

    should_terminate: threading.Event = field(default_factory=threading.Event)
    """終了フラグ"""

    liveness_file: pathlib.Path | None = None
    """receiver 専用 Liveness ファイルパス（start() で設定される）"""

    slack_config: my_lib.notify.slack.SlackErrorOnlyConfig | my_lib.notify.slack.SlackEmptyConfig | None = (
        None
    )
    """Slack 通知設定（start() で設定される）"""

    shared_buffer: IntegratedBuffer | None = None
    """共有 IntegratedBuffer（VDL2 との高度補完用）"""

    last_footprint_update: float = 0.0
    """Liveness ファイルの最終更新時刻（time.time()）"""


_state = _ReceiverState()


def reset() -> None:
    """モジュール状態を初期化する（テスト用）"""
    global _state
    _state = _ReceiverState()


@dataclass
class WeatherRecord:
    """ペアリングされた気象レコード

    以下のいずれかの形式:
    - 日時、高度、気温
    - 日時、高度、風向・風速
    - 日時、高度、気温、風向・風速
    """

    icao: str
    altitude_ft: float
    callsign: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    temperature_c: float | None = None
    wind_speed_kt: float | None = None
    wind_direction_deg: float | None = None
    data_source: str = ""  # "bds44" or "bds50_60"

    def has_temperature(self) -> bool:
        """温度データを持つか"""
        return self.temperature_c is not None

    def has_wind(self) -> bool:
        """風データを持つか"""
        return self.wind_speed_kt is not None and self.wind_direction_deg is not None


def _decode_bds44(message: str) -> tuple[float, float, float] | None:
    """BDS 4,4 から (temperature, wind_speed, wind_direction) を取得する

    Returns:
        (気温 [℃], 風速 [kt], 風向 [度]) または欠損時 None
    """
    temperature = pyModeS.bds.bds44.temp44(message)
    wind_data = pyModeS.bds.bds44.wind44(message)
    if temperature is None or wind_data is None:
        return None
    wind_speed, wind_direction = wind_data
    if wind_speed is None or wind_direction is None:
        return None
    return float(temperature), float(wind_speed), float(wind_direction)


def _decode_bds50(message: str) -> tuple[float, float, float] | None:
    """BDS 5,0 から (trackangle, groundspeed, trueair) を取得する

    Returns:
        (対地進行方向 [度], 対地速度 [kt], 真気速度 [kt]) または欠損時 None
    """
    trackangle = pyModeS.commb.trk50(message)
    groundspeed = pyModeS.commb.gs50(message)
    trueair = pyModeS.commb.tas50(message)
    if trackangle is None or groundspeed is None or trueair is None:
        return None
    return float(trackangle), float(groundspeed), float(trueair)


def _decode_bds60(message: str) -> tuple[float, float, float] | None:
    """BDS 6,0 から (heading, indicatedair, mach) を取得する

    Returns:
        (機首方位 [度], 指示対気速度 [kt], マッハ数) または欠損時 None
    """
    heading = pyModeS.commb.hdg60(message)
    indicatedair = pyModeS.commb.ias60(message)
    mach = pyModeS.commb.mach60(message)
    if heading is None or indicatedair is None or mach is None:
        return None
    return float(heading), float(indicatedair), float(mach)


def _calc_bds50_60_weather(
    latitude: float,
    longitude: float,
    bds50: tuple[float, float, float],
    bds60: tuple[float, float, float],
    callsign: str | None = None,
) -> tuple[float, CoreWindData] | None:
    """BDS 5,0/6,0 ペアから気温と風を計算する

    マッハ数が 0 以下、または温度が異常値閾値未満の場合はレコードを
    破棄するため None を返します。

    Args:
        latitude: 緯度 [度]
        longitude: 経度 [度]
        bds50: (trackangle [度], groundspeed [kt], trueair [kt])
        bds60: (heading [度], indicatedair [kt], mach)
        callsign: コールサイン（ログ用）

    Returns:
        (気温 [℃], WindData) または破棄時 None
    """
    trackangle, groundspeed, trueair = bds50
    heading, _indicatedair, mach = bds60

    if mach <= 0:
        logging.warning("マッハ数が不正なので捨てます．(callsign: %s, mach: %s)", callsign, mach)
        return None

    trueair_ms = trueair * amdar.constants.KNOTS_TO_MS
    groundspeed_ms = groundspeed * amdar.constants.KNOTS_TO_MS

    temperature = amdar.core.physics.calc_temperature(trueair_ms, mach)
    if temperature < amdar.constants.GRAPH_TEMPERATURE_THRESHOLD:
        logging.warning(
            "温度が異常なので捨てます．(callsign: %s, temperature: %.1f, trueair: %s, mach: %s)",
            callsign,
            temperature,
            trueair_ms,
            mach,
        )
        return None

    wind = amdar.core.physics.calc_wind(latitude, longitude, trackangle, groundspeed_ms, heading, trueair_ms)

    return temperature, wind


def parse_weather_records_from_file(
    file_path: pathlib.Path,
    ref_lat: float = amdar.constants.DEFAULT_REFERENCE_LATITUDE,
    ref_lon: float = amdar.constants.DEFAULT_REFERENCE_LONGITUDE,
) -> list[WeatherRecord]:
    """Mode S メッセージファイルからペアリングされた気象レコードを抽出する

    ADS-B 位置情報と気象データ（BDS 4,4 または BDS 5,0/6,0）をペアリングして、
    高度と気象データが紐付いたレコードを生成します。

    Args:
        file_path: Mode S メッセージファイルのパス（1行1メッセージ）
        ref_lat: 位置計算の基準緯度
        ref_lon: 位置計算の基準経度

    Returns:
        list[WeatherRecord]: ペアリングされた気象レコードのリスト

    """
    # ICAO ごとのフラグメントを管理
    fragments: dict[str, _MessageFragment] = {}
    results: list[WeatherRecord] = []

    with file_path.open() as f:
        for line in f:
            line = line.strip()
            if not line or not line.startswith("*"):
                continue

            msg = line[1:].rstrip(";")

            if len(msg) < 22:
                continue

            try:
                icao = str(pyModeS.icao(msg))  # pyright: ignore[reportPrivateImportUsage]
                dformat = pyModeS.df(msg)  # pyright: ignore[reportPrivateImportUsage]

                # フラグメントを取得または作成
                if icao not in fragments:
                    fragments[icao] = _MessageFragment(icao=icao)
                frag = fragments[icao]

                # DF=17,18: ADS-B
                if dformat in (17, 18) and len(msg) == 28:
                    _update_fragment_from_adsb_file(frag, msg, ref_lat, ref_lon)

                # DF=20,21: Comm-B
                elif dformat in (20, 21) and len(msg) == 28:
                    record = _parse_commb_for_file(frag, msg)
                    if record is not None:
                        results.append(record)

            except Exception:
                logging.debug("メッセージ解析に失敗: %s", msg)

    return results


def _update_fragment_from_adsb_file(
    frag: _MessageFragment,
    msg: str,
    ref_lat: float,
    ref_lon: float,
) -> None:
    """ADS-B メッセージからフラグメントを更新する（ファイル解析用）"""
    code = pyModeS.typecode(msg)  # pyright: ignore[reportPrivateImportUsage]
    if code is None:
        return

    # 位置情報（高度含む）
    if (5 <= code <= 18) or (20 <= code <= 22):
        altitude = pyModeS.adsb.altitude(msg)
        if altitude and altitude > 0:
            frag.altitude_ft = float(altitude)
            try:
                lat, lon = pyModeS.adsb.position_with_ref(msg, ref_lat, ref_lon)
                if lat is not None and lon is not None:
                    frag.latitude = lat
                    frag.longitude = lon
            except Exception:
                logging.debug("位置計算に失敗: %s", msg)

    # コールサイン
    elif 1 <= code <= 4:
        callsign = pyModeS.adsb.callsign(msg).rstrip("_")
        if callsign:
            frag.callsign = callsign


def _parse_commb_for_file(frag: _MessageFragment, msg: str) -> WeatherRecord | None:
    """Comm-B メッセージを解析し、ペアリング可能なら WeatherRecord を返す（ファイル解析用）"""
    # BDS 4,4 を優先（直接気象データ）
    if pyModeS.bds.bds44.is44(msg):
        decoded = _decode_bds44(msg)
        if decoded is None or frag.altitude_ft is None:
            return None
        temperature, wind_speed, wind_direction = decoded
        return WeatherRecord(
            icao=frag.icao,
            altitude_ft=frag.altitude_ft,
            callsign=frag.callsign,
            latitude=frag.latitude,
            longitude=frag.longitude,
            temperature_c=temperature,
            wind_speed_kt=wind_speed,
            wind_direction_deg=wind_direction,
            data_source="bds44",
        )

    if pyModeS.bds.bds50.is50(msg):
        decoded = _decode_bds50(msg)
        if decoded is not None:
            frag.bds50 = decoded
    elif pyModeS.bds.bds60.is60(msg):
        decoded = _decode_bds60(msg)
        if decoded is not None:
            frag.bds60 = decoded

    # BDS 5,0 + 6,0 + 高度 + 位置でペアリング
    if (
        frag.bds50 is None
        or frag.bds60 is None
        or frag.altitude_ft is None
        or frag.latitude is None
        or frag.longitude is None
    ):
        return None

    weather = _calc_bds50_60_weather(frag.latitude, frag.longitude, frag.bds50, frag.bds60, frag.callsign)

    # フラグメントをリセット（使用済み）
    frag.bds50 = None
    frag.bds60 = None

    if weather is None:
        return None

    temperature_c, wind = weather
    return WeatherRecord(
        icao=frag.icao,
        altitude_ft=frag.altitude_ft,
        callsign=frag.callsign,
        latitude=frag.latitude,
        longitude=frag.longitude,
        temperature_c=temperature_c,
        wind_speed_kt=wind.speed / amdar.constants.KNOTS_TO_MS,  # m/s -> kt
        wind_direction_deg=wind.angle,
        data_source="bds50_60",
    )


def _receive_lines(sock: socket.socket) -> Generator[str, None, None]:
    buffer = b""

    while True:
        data = sock.recv(1024)

        if not data:
            return

        buffer += data
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            yield line.decode()


def _round_floats(obj: Any, ndigits: int = 1) -> Any:
    match obj:
        case float():
            return round(obj, ndigits)
        case dict():
            return {k: _round_floats(v, ndigits) for k, v in obj.items()}
        case list():
            return [_round_floats(elem, ndigits) for elem in obj]
        case tuple():
            return tuple(_round_floats(elem, ndigits) for elem in obj)
        case _:
            return obj


def _prune_fragments(now: float) -> None:
    """TTL を超過したフラグメントを破棄する"""
    expired = [
        icao
        for icao, frag in _state.fragments.items()
        if now - frag.updated_at > amdar.constants.FRAGMENT_TTL_SECONDS
    ]
    for icao in expired:
        del _state.fragments[icao]


def _get_fragment(icao: str) -> _MessageFragment:
    """フラグメントを取得または作成する

    期限切れフラグメントの破棄と件数上限の維持も行います。
    """
    now = time.time()
    _prune_fragments(now)

    frag = _state.fragments.get(icao)
    if frag is None:
        frag = _MessageFragment(icao=icao, updated_at=now)
        _state.fragments[icao] = frag

        # 件数上限を超えた場合は最も古いフラグメントから破棄する
        while len(_state.fragments) > _FRAGMENT_BUF_SIZE:
            oldest_icao = min(_state.fragments, key=lambda k: _state.fragments[k].updated_at)
            del _state.fragments[oldest_icao]

    frag.updated_at = now
    return frag


def _emit_observation(
    observation: WeatherObservation,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
) -> None:
    """観測データを検証し、正常値をキューに送信する"""
    # 温度が None の場合はスキップ
    if observation.temperature is None:
        logging.debug("温度データなしのためスキップ")
        return

    # 温度異常値は外れ値検出の対象外
    if observation.temperature < amdar.constants.GRAPH_TEMPERATURE_THRESHOLD:
        logging.debug("温度異常値のためスキップ")
        return

    # 外れ値検出
    detector = amdar.sources.outlier.get_default_detector()
    if detector.is_outlier(
        observation.altitude,
        observation.temperature,
        observation.callsign or "",
    ):
        return

    # WeatherObservation を MeasurementData に変換してキューに送信
    measurement_data = observation.to_measurement_data()

    # 正常値の場合、queueに送信し履歴に追加
    logging.info(_round_floats(measurement_data))
    data_queue.put(measurement_data)
    detector.add_history(observation.altitude, observation.temperature)


def _try_emit_weather(
    frag: _MessageFragment,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """フラグメントが完全なら気象データを生成してキューに送信する

    完全なフラグメントは処理後に破棄されます。
    """
    if frag.callsign is None or frag.altitude_ft is None or frag.latitude is None or frag.longitude is None:
        return

    # BDS44 ルート: 直接気象データを持つ（BDS50/BDS60 より優先）
    if frag.bds44 is not None:
        data_source = "bds44"
    elif frag.bds50 is not None and frag.bds60 is not None:
        data_source = "bds50_60"
    else:
        return

    # 完全なフラグメントは処理後に破棄する
    _state.fragments.pop(frag.icao, None)

    distance = amdar.core.geo.haversine_distance(
        area_config.lat.ref,
        area_config.lon.ref,
        frag.latitude,
        frag.longitude,
    )

    if data_source == "bds44":
        logging.debug("BDS44 から気象データを生成")
        temperature, wind_speed, wind_direction = frag.bds44  # type: ignore[misc]
        observation = WeatherObservation.from_imperial(
            callsign=frag.callsign,
            altitude_ft=frag.altitude_ft,
            latitude=frag.latitude,
            longitude=frag.longitude,
            temperature_c=temperature,
            wind_speed_kt=wind_speed,
            wind_direction_deg=wind_direction,
            distance=distance,
            method=amdar.constants.MODE_S_METHOD,
            data_source="bds44",
        )
    else:
        weather = _calc_bds50_60_weather(
            frag.latitude,
            frag.longitude,
            frag.bds50,  # type: ignore[arg-type]
            frag.bds60,  # type: ignore[arg-type]
            frag.callsign,
        )
        if weather is None:
            return
        temperature_c, wind = weather
        observation = WeatherObservation(
            callsign=frag.callsign,
            altitude=frag.altitude_ft * amdar.constants.FEET_TO_METERS,
            latitude=frag.latitude,
            longitude=frag.longitude,
            temperature=temperature_c,
            wind=wind,
            distance=distance,
            method=amdar.constants.MODE_S_METHOD,
            data_source="bds50_60",
        )

    _emit_observation(observation, data_queue)


def _process_adsb_position(
    message: str,
    icao: str,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """ADS-B位置情報メッセージを処理する"""
    altitude = pyModeS.adsb.altitude(message)
    if altitude is None or altitude == 0:
        return

    latitude, longitude = pyModeS.adsb.position_with_ref(message, area_config.lat.ref, area_config.lon.ref)

    if latitude is None or longitude is None:
        logging.warning(
            "データに欠損があるので捨てます．(type: adsb_pos, data: %s)",
            (altitude, latitude, longitude),
        )
    else:
        frag = _get_fragment(icao)
        frag.altitude_ft = float(altitude)
        frag.latitude = latitude
        frag.longitude = longitude
        _try_emit_weather(frag, data_queue, area_config)

    # 共有バッファに ADS-B 位置情報をフィード（VDL2 高度補完用）
    if _state.shared_buffer is not None and altitude > 0:
        altitude_m = float(altitude) * amdar.constants.FEET_TO_METERS
        # フラグメントからコールサインを取得
        current = _state.fragments.get(icao)
        callsign = current.callsign if current is not None else None
        _state.shared_buffer.add_adsb_position(
            icao=icao,
            callsign=callsign,
            timestamp=my_lib.time.now(),
            altitude_m=altitude_m,
            lat=latitude,
            lon=longitude,
        )


def _process_adsb_message(
    message: str,
    icao: str,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """ADS-Bメッセージ（dformat=17）を処理する"""
    logging.debug("receive ADSB")
    code = pyModeS.typecode(message)  # pyright: ignore[reportPrivateImportUsage]

    if code is None:
        return

    # 位置情報（typecode 5-18, 20-22）
    if (5 <= code <= 18) or (20 <= code <= 22):
        _process_adsb_position(message, icao, data_queue, area_config)
    # コールサイン（typecode 1-4）
    elif 1 <= code <= 4:
        callsign = pyModeS.adsb.callsign(message).rstrip("_")
        frag = _get_fragment(icao)
        frag.callsign = callsign
        _try_emit_weather(frag, data_queue, area_config)


def _process_commb_message(
    message: str,
    icao: str,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """Comm-Bメッセージ（dformat=20,21）を処理する"""
    # BDS44: Meteorological routine air report（直接気象データを持つ）
    if pyModeS.bds.bds44.is44(message):
        logging.debug("receive BDS44 (MRAR)")
        decoded = _decode_bds44(message)
        if decoded is None:
            return
        frag = _get_fragment(icao)
        frag.bds44 = decoded
        _try_emit_weather(frag, data_queue, area_config)

    elif pyModeS.bds.bds50.is50(message):
        logging.debug("receive BDS50")
        decoded = _decode_bds50(message)
        if decoded is None:
            logging.warning("データに欠損があるので捨てます．(type: bds50)")
            return
        frag = _get_fragment(icao)
        frag.bds50 = decoded
        _try_emit_weather(frag, data_queue, area_config)

    elif pyModeS.bds.bds60.is60(message):
        logging.debug("receive BDS60")
        decoded = _decode_bds60(message)
        if decoded is None:
            logging.warning("データに欠損があるので捨てます．(type: bds60)")
            return
        frag = _get_fragment(icao)
        frag.bds60 = decoded
        _try_emit_weather(frag, data_queue, area_config)


def _process_message(
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

    icao = str(pyModeS.icao(message))  # pyright: ignore[reportPrivateImportUsage]
    dformat = pyModeS.df(message)  # pyright: ignore[reportPrivateImportUsage]

    # DF=17: ADS-B, DF=18: TIS-B/ADS-R（同じ Extended Squitter 形式）
    if dformat in (17, 18):
        _process_adsb_message(message, icao, data_queue, area_config)
    elif dformat in (20, 21):
        _process_commb_message(message, icao, data_queue, area_config)


def _calculate_retry_delay(retry_count: int) -> float:
    """指数バックオフで再接続遅延時間を計算する"""
    return min(
        amdar.constants.MODES_RECEIVER_BASE_DELAY * (2 ** (retry_count - 1)),
        amdar.constants.MODES_RECEIVER_MAX_DELAY,
    )


def _wait_with_interrupt(delay: float) -> None:
    """中断可能な待機を行う"""
    _state.should_terminate.wait(delay)


def _update_liveness_throttled() -> None:
    """Liveness ファイルをスロットル付きで更新する

    受信1行ごとのファイル書き込みを避けるため、
    _FOOTPRINT_UPDATE_INTERVAL_SECONDS に1回だけ更新します。
    """
    if _state.liveness_file is None:
        return

    now = time.time()
    if now - _state.last_footprint_update < _FOOTPRINT_UPDATE_INTERVAL_SECONDS:
        return

    _state.last_footprint_update = now
    my_lib.footprint.update(_state.liveness_file)


def _process_socket_messages(
    sock: socket.socket,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> bool:
    """ソケットからメッセージを受信して処理する

    Returns:
        1行以上データを受信した場合 True
    """
    received = False
    try:
        for line in _receive_lines(sock):
            received = True

            if _state.should_terminate.is_set():
                break

            try:
                _process_message(line, data_queue, area_config)

                # データ受信時にLivenessファイル更新（スロットル付き）
                _update_liveness_throttled()

            except Exception:
                logging.exception("メッセージ処理に失敗しました")

    except TimeoutError:
        logging.warning("ソケットタイムアウトが発生しました")
    except (OSError, ConnectionError) as e:
        logging.warning("受信中に接続エラーが発生しました: %s", e)

    return received


def _handle_connection(
    host: str,
    port: int,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> bool:
    """TCP接続を確立しメッセージを処理する

    Returns:
        この接続で1行以上データを受信した場合True

    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(amdar.constants.MODES_RECEIVER_SOCKET_TIMEOUT)
        sock.connect((host, port))
        logging.info("%s:%d に接続しました", host, port)

        received = _process_socket_messages(sock, data_queue, area_config)

        if not _state.should_terminate.is_set():
            logging.warning("リモートホストによって接続が閉じられました")

        return received


def _worker(
    host: str,
    port: int,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """再接続機能付きワーカー

    TCP接続が切断された場合、指数バックオフで再接続を試みます。
    データを1行も受信できずに終了した接続（即クローズ・タイムアウト含む）は
    失敗としてカウントし、最大リトライ回数に達した場合のみワーカーを終了します。
    """
    logging.info("受信ワーカーを開始します")
    _state.should_terminate.clear()
    retry_count = 0

    while not _state.should_terminate.is_set():
        error: Exception | None = None
        received = False

        try:
            received = _handle_connection(host, port, data_queue, area_config)
        except (OSError, ConnectionError) as e:
            # NOTE: connect 時のタイムアウト（TimeoutError）もここで捕捉される
            error = e
        except Exception:
            logging.exception("受信ワーカーで予期しないエラーが発生しました")
            break

        if _state.should_terminate.is_set():
            break

        if received:
            # データを受信できた接続のみ成功として扱う
            retry_count = 0
            continue

        retry_count += 1
        if retry_count > amdar.constants.MODES_RECEIVER_MAX_RETRIES:
            max_retries = amdar.constants.MODES_RECEIVER_MAX_RETRIES
            error_message = f"最大再接続回数（{max_retries}回）に達しました。処理を終了します"
            logging.error(error_message)
            if _state.slack_config is not None:
                last_error = str(error) if error is not None else "データを受信できませんでした"
                my_lib.notify.slack.error(
                    _state.slack_config,
                    "Mode-S受信エラー",
                    f"{error_message}\n接続先: {host}:{port}\n最後のエラー: {last_error}",
                )
            break

        delay = _calculate_retry_delay(retry_count)
        logging.warning(
            "接続に失敗しました（%d/%d回目）: %s。%.1f秒後に再試行します...",
            retry_count,
            amdar.constants.MODES_RECEIVER_MAX_RETRIES,
            error if error is not None else "受信データなし",
            delay,
        )
        _wait_with_interrupt(delay)

    logging.warning("受信ワーカーを停止します")


def init(data: list[tuple[float, float]]) -> None:
    """履歴データを初期化

    Args:
        data: (altitude, temperature) のタプルのリスト
    """
    detector = amdar.sources.outlier.get_default_detector()
    for altitude, temperature in data:
        detector.add_history(altitude, temperature)


def start(
    config: Config,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    buffer: IntegratedBuffer | None = None,
) -> threading.Thread:
    """receiverワーカースレッドを開始する

    Args:
        config: アプリケーション設定
        data_queue: データを送信するキュー
        buffer: ADS-B 位置情報をフィードするバッファ（VDL2 高度補完用、オプション）

    Returns:
        開始されたスレッド

    """
    _state.liveness_file = config.liveness.file.receiver.modes
    _state.slack_config = config.slack
    _state.shared_buffer = buffer

    thread = threading.Thread(
        target=_worker,
        args=(
            config.decoder.modes.host,
            config.decoder.modes.port,
            data_queue,
            config.filter.area,
        ),
    )
    thread.start()

    return thread


def term() -> None:
    _state.should_terminate.set()


if __name__ == "__main__":
    import docopt
    import my_lib.logger

    import amdar.config

    assert __doc__ is not None  # noqa: S101
    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    debug_mode = args["-D"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = amdar.config.load_config(config_file)

    measurement_queue: queue.Queue[MeteorologicalData] = queue.Queue()

    start(config, measurement_queue)

    while True:
        logging.info(measurement_queue.get())

        if _state.should_terminate.is_set():
            break
