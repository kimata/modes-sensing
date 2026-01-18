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
import pyModeS

if TYPE_CHECKING:
    import multiprocessing
    from collections.abc import Generator

    from amdar.config import Area, Config
    from amdar.sources.aggregator import IntegratedBuffer

import amdar.constants
import amdar.core.geo
import amdar.sources.outlier
from amdar.core.types import WeatherObservation
from amdar.core.types import WindData as CoreWindData
from amdar.database.postgresql import MeasurementData as MeteorologicalData


class MessageFragment(TypedDict, total=False):
    """メッセージフラグメント"""

    icao: str
    adsb_pos: tuple[float, float | None, float | None]
    adsb_sign: tuple[str]
    bds50: tuple[float | None, float | None, float | None]
    bds60: tuple[float | None, float | None, float | None]
    # BDS44: (temperature, wind_speed, wind_direction)
    # 温度(℃), 風速(kt), 風向(度, 真北基準)
    bds44: tuple[float, float, float]


_FRAGMENT_BUF_SIZE: int = 100

_fragment_list: list[MessageFragment] = []

_should_terminate = threading.Event()

# receiver専用Livenessファイルパス（start()で設定される）
_receiver_liveness_file: pathlib.Path | None = None

# Slack通知設定（start()で設定される）
_slack_config: my_lib.notify.slack.SlackErrorOnlyConfig | my_lib.notify.slack.SlackEmptyConfig | None = None

# 共有 IntegratedBuffer（VDL2 との高度補完用）
_shared_buffer: IntegratedBuffer | None = None


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


@dataclass
class _FileParseFragment:
    """ICAO ごとのメッセージフラグメント（ファイル解析用）"""

    icao: str
    callsign: str | None = None
    altitude_ft: float | None = None
    latitude: float | None = None
    longitude: float | None = None
    # BDS 5,0: (trackangle, groundspeed, trueair)
    bds50: tuple[float | None, float | None, float | None] | None = None
    # BDS 6,0: (heading, indicatedair, mach)
    bds60: tuple[float | None, float | None, float | None] | None = None
    # BDS 4,4: (temperature, wind_speed, wind_direction)
    bds44: tuple[float, float, float] | None = None


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
    fragments: dict[str, _FileParseFragment] = {}
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
                icao = str(pyModeS.icao(msg))
                dformat = pyModeS.df(msg)

                # フラグメントを取得または作成
                if icao not in fragments:
                    fragments[icao] = _FileParseFragment(icao=icao)
                frag = fragments[icao]

                # DF=17,18: ADS-B
                if dformat in (17, 18) and len(msg) == 28:
                    code = pyModeS.typecode(msg)
                    if code is None:
                        continue

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

                # DF=20,21: Comm-B
                elif dformat in (20, 21) and len(msg) == 28:
                    # BDS 4,4 を優先（直接気象データ）
                    if pyModeS.bds.bds44.is44(msg):
                        temperature = pyModeS.bds.bds44.temp44(msg)
                        wind_data = pyModeS.bds.bds44.wind44(msg)
                        if temperature is not None and wind_data is not None:
                            wind_speed, wind_direction = wind_data
                            if wind_speed is not None and wind_direction is not None:
                                frag.bds44 = (temperature, wind_speed, wind_direction)

                                # BDS 4,4 + 高度でペアリング
                                if frag.altitude_ft is not None:
                                    record = WeatherRecord(
                                        icao=icao,
                                        altitude_ft=frag.altitude_ft,
                                        callsign=frag.callsign,
                                        latitude=frag.latitude,
                                        longitude=frag.longitude,
                                        temperature_c=temperature,
                                        wind_speed_kt=wind_speed,
                                        wind_direction_deg=wind_direction,
                                        data_source="bds44",
                                    )
                                    results.append(record)
                                    frag.bds44 = None  # 使用済み
                                continue

                    # BDS 5,0
                    if pyModeS.bds.bds50.is50(msg):
                        trackangle = pyModeS.commb.trk50(msg)
                        groundspeed = pyModeS.commb.gs50(msg)
                        trueair = pyModeS.commb.tas50(msg)
                        if all(v is not None for v in (trackangle, groundspeed, trueair)):
                            frag.bds50 = (trackangle, groundspeed, trueair)

                    # BDS 6,0
                    elif pyModeS.bds.bds60.is60(msg):
                        heading = pyModeS.commb.hdg60(msg)
                        indicatedair = pyModeS.commb.ias60(msg)
                        mach = pyModeS.commb.mach60(msg)
                        if all(v is not None for v in (heading, indicatedair, mach)):
                            frag.bds60 = (heading, indicatedair, mach)

                    # BDS 5,0 + 6,0 + 高度 + 位置でペアリング
                    if (
                        frag.bds50 is not None
                        and frag.bds60 is not None
                        and frag.altitude_ft is not None
                        and frag.latitude is not None
                        and frag.longitude is not None
                    ):
                        trackangle, groundspeed, trueair = frag.bds50
                        heading, indicatedair, mach = frag.bds60

                        # None チェックは上で済んでいるが、型ガードのため明示的にキャスト
                        trackangle_f = float(trackangle)  # type: ignore[arg-type]
                        groundspeed_f = float(groundspeed)  # type: ignore[arg-type]
                        trueair_f = float(trueair)  # type: ignore[arg-type]
                        heading_f = float(heading)  # type: ignore[arg-type]
                        mach_f = float(mach)  # type: ignore[arg-type]

                        # 気温と風を計算（既存の関数を使用）
                        temperature_c = _calc_temperature(trueair_f * amdar.constants.KNOTS_TO_MS, mach_f)
                        wind = _calc_wind(
                            frag.latitude,
                            frag.longitude,
                            trackangle_f,
                            groundspeed_f * amdar.constants.KNOTS_TO_MS,
                            heading_f,
                            trueair_f * amdar.constants.KNOTS_TO_MS,
                        )

                        # 異常値チェック
                        if temperature_c >= amdar.constants.GRAPH_TEMPERATURE_THRESHOLD:
                            record = WeatherRecord(
                                icao=icao,
                                altitude_ft=frag.altitude_ft,
                                callsign=frag.callsign,
                                latitude=frag.latitude,
                                longitude=frag.longitude,
                                temperature_c=temperature_c,
                                wind_speed_kt=wind.speed / amdar.constants.KNOTS_TO_MS,  # m/s -> kt
                                wind_direction_deg=wind.angle,
                                data_source="bds50_60",
                            )
                            results.append(record)

                        # フラグメントをリセット
                        frag.bds50 = None
                        frag.bds60 = None

            except Exception:
                logging.debug("メッセージ解析に失敗: %s", msg)

    return results


def _receive_lines(sock: socket.socket) -> Generator[str, None, None]:
    buffer = b""

    while True:
        data = sock.recv(1024)

        if data is None:
            return

        buffer += data
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            yield line.decode()


def _calc_temperature(trueair: float, mach: float) -> float:
    k = 1.403  # 比熱比(空気)
    M = 28.966e-3  # 分子量(空気) [kg/mol]
    R = 8.314472  # 気体定数

    K = M / k / R

    return (trueair / mach) * (trueair / mach) * K - 273.15


def _calc_magnetic_declination(latitude: float, longitude: float) -> float:
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


def _calc_wind(
    latitude: float,
    longitude: float,
    trackangle: float,
    groundspeed: float,
    heading: float,
    trueair: float,
) -> CoreWindData:
    magnetic_declination = _calc_magnetic_declination(latitude, longitude)

    ground_dir = math.pi / 2 - math.radians(trackangle)
    ground_x = groundspeed * math.cos(ground_dir)
    ground_y = groundspeed * math.sin(ground_dir)
    air_dir = math.pi / 2 - math.radians(heading) + math.radians(magnetic_declination)
    air_x = trueair * math.cos(air_dir)
    air_y = trueair * math.sin(air_dir)

    wind_x = ground_x - air_x
    wind_y = ground_y - air_y

    return CoreWindData(
        x=wind_x,
        y=wind_y,
        # NOTE: 北を 0 として，風が来る方の角度
        angle=math.degrees(
            (math.pi / 2 - math.atan2(wind_y, wind_x) + 2 * math.pi + math.pi) % (2 * math.pi)
        ),
        speed=math.sqrt(wind_x * wind_x + wind_y * wind_y),
    )


def _calc_meteorological_data(
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
) -> WeatherObservation:
    altitude_m = altitude * amdar.constants.FEET_TO_METERS  # 単位換算: feet → meter
    groundspeed_ms = groundspeed * amdar.constants.KNOTS_TO_MS  # 単位換算: knot → m/s
    trueair_ms = trueair * amdar.constants.KNOTS_TO_MS

    temperature = _calc_temperature(trueair_ms, mach)
    wind = _calc_wind(latitude, longitude, trackangle, groundspeed_ms, heading, trueair_ms)

    if temperature < amdar.constants.GRAPH_TEMPERATURE_THRESHOLD:
        logging.warning(
            "温度が異常なので捨てます．(callsign: %s, temperature: %.1f, "
            "altitude: %s, trueair: %s, mach: %s)",
            callsign,
            temperature,
            altitude_m,
            trueair_ms,
            mach,
        )
    return WeatherObservation(
        callsign=callsign,
        altitude=altitude_m,
        latitude=latitude,
        longitude=longitude,
        temperature=temperature,
        wind=wind,
        distance=distance,
        method=amdar.constants.MODE_S_METHOD,
        data_source="bds50_60",
    )


def _calc_meteorological_data_from_bds44(
    callsign: str,
    altitude: float,
    latitude: float,
    longitude: float,
    temperature: float,
    wind_speed: float,
    wind_direction: float,
    distance: float,
) -> WeatherObservation:
    """BDS44 の直接気象データから WeatherObservation を生成する

    Args:
        callsign: コールサイン
        altitude: 高度 (feet)
        latitude: 緯度
        longitude: 経度
        temperature: 気温 (℃) - BDS44 から直接取得
        wind_speed: 風速 (kt) - BDS44 から直接取得（真の風）
        wind_direction: 風向 (度, 真北基準) - BDS44 から直接取得
        distance: 基準点からの距離 (km)

    Returns:
        WeatherObservation

    """
    # from_imperial を使用して航空単位系から変換
    return WeatherObservation.from_imperial(
        callsign=callsign,
        altitude_ft=altitude,
        latitude=latitude,
        longitude=longitude,
        temperature_c=temperature,
        wind_speed_kt=wind_speed,
        wind_direction_deg=wind_direction,
        distance=distance,
        method=amdar.constants.MODE_S_METHOD,
        data_source="bds44",
    )


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


def _is_fragment_complete(fragment: MessageFragment) -> tuple[bool, str]:
    """フラグメントが完全かどうかを判定する

    Returns:
        (bool, str): (完全かどうか, データソース種別)
        データソース種別: "bds50_60" または "bds44"

    """
    # 共通の必須フィールド
    base_required = ["adsb_pos", "adsb_sign"]
    if not all(packet_type in fragment for packet_type in base_required):
        return False, ""

    # BDS44 ルート: 直接気象データを持つ（BDS50/BDS60 より優先）
    if "bds44" in fragment:
        return True, "bds44"

    # BDS50/BDS60 ルート: 従来の計算方式
    if "bds50" in fragment and "bds60" in fragment:
        return True, "bds50_60"

    return False, ""


def _process_complete_fragment(
    fragment: MessageFragment,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
    data_source: str,
) -> None:
    """完全なフラグメントを処理してキューに送信する

    Args:
        fragment: メッセージフラグメント
        data_queue: データ送信キュー
        area_config: エリア設定
        data_source: データソース種別 ("bds44" または "bds50_60")

    """
    # TypedDict の各キーを .get() で安全に取得
    adsb_pos = fragment.get("adsb_pos")
    adsb_sign = fragment.get("adsb_sign")

    # 共通の必須フィールドチェック
    if adsb_pos is None or adsb_sign is None:
        return
    if adsb_pos[1] is None or adsb_pos[2] is None:
        return

    distance = amdar.core.geo.haversine_distance(
        area_config.lat.ref,
        area_config.lon.ref,
        adsb_pos[1],
        adsb_pos[2],
    )

    # データソースに応じて気象データを生成
    if data_source == "bds44":
        bds44 = fragment.get("bds44")
        if bds44 is None:
            return

        logging.debug("BDS44 から気象データを生成")
        meteorological_data = _calc_meteorological_data_from_bds44(
            callsign=adsb_sign[0],
            altitude=adsb_pos[0],
            latitude=adsb_pos[1],
            longitude=adsb_pos[2],
            temperature=bds44[0],
            wind_speed=bds44[1],
            wind_direction=bds44[2],
            distance=distance,
        )
    else:  # bds50_60
        bds50 = fragment.get("bds50")
        bds60 = fragment.get("bds60")

        if bds50 is None or bds60 is None:
            return
        if any(v is None for v in bds50) or any(v is None for v in bds60):
            return

        # NOTE: 上記の None チェック後でもタプル要素の型は絞り込まれないため type: ignore が必要
        meteorological_data = _calc_meteorological_data(
            *adsb_sign,
            *adsb_pos,  # type: ignore[arg-type]
            *bds50,  # type: ignore[arg-type]
            *bds60,  # type: ignore[arg-type]
            distance,
        )

    # WeatherObservation として処理（temperature は必ず設定されている）
    observation = meteorological_data

    # 温度が None の場合はスキップ
    if observation.temperature is None:
        logging.debug("温度データなしのためスキップ")
        return

    # 温度異常値は外れ値検出の対象外
    if observation.temperature < amdar.constants.GRAPH_TEMPERATURE_THRESHOLD:
        logging.debug("温度異常値のため外れ値検出をスキップ")
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


def _add_new_fragment(icao: str, packet_type: str, data: tuple[Any, ...]) -> None:
    """新しいフラグメントをリストに追加する"""
    global _fragment_list

    # 動的キーを使用するため TypedDict に完全に適合しない
    _fragment_list.append({"icao": icao, packet_type: data})  # type: ignore[misc]
    if len(_fragment_list) >= _FRAGMENT_BUF_SIZE:
        _fragment_list.pop(0)


def _message_pairing(
    icao: str,
    packet_type: str,
    data: tuple[Any, ...],
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """メッセージフラグメントをペアリングして気象データを生成する"""
    global _fragment_list

    if not all(value is not None for value in data):
        logging.warning("データに欠損があるので捨てます．(type: %s, data: %s)", packet_type, data)
        return

    fragment = next((f for f in _fragment_list if f.get("icao") == icao), None)

    if fragment is None:
        _add_new_fragment(icao, packet_type, data)
        return

    # 動的キーを使用するため TypedDict に完全に適合しない
    fragment[packet_type] = data  # type: ignore[literal-required]

    is_complete, data_source = _is_fragment_complete(fragment)
    if not is_complete:
        return

    _process_complete_fragment(fragment, data_queue, area_config, data_source)
    _fragment_list.remove(fragment)


def _process_adsb_position(
    message: str,
    icao: str,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """ADS-B位置情報メッセージを処理する"""
    global _shared_buffer

    altitude = pyModeS.adsb.altitude(message)
    if altitude is None or altitude == 0:
        return

    latitude, longitude = pyModeS.adsb.position_with_ref(message, area_config.lat.ref, area_config.lon.ref)
    _message_pairing(icao, "adsb_pos", (altitude, latitude, longitude), data_queue, area_config)

    # 共有バッファに ADS-B 位置情報をフィード（VDL2 高度補完用）
    if _shared_buffer is not None and altitude > 0:
        import my_lib.time

        altitude_m = float(altitude) * amdar.constants.FEET_TO_METERS
        # フラグメントからコールサインを取得
        callsign = None
        for frag in _fragment_list:
            if frag.get("icao") == icao and "adsb_sign" in frag:
                callsign = frag["adsb_sign"][0]
                break
        _shared_buffer.add_adsb_position(
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
    code = pyModeS.typecode(message)

    if code is None:
        return

    # 位置情報（typecode 5-18, 20-22）
    if (5 <= code <= 18) or (20 <= code <= 22):
        _process_adsb_position(message, icao, data_queue, area_config)
    # コールサイン（typecode 1-4）
    elif 1 <= code <= 4:
        callsign = pyModeS.adsb.callsign(message).rstrip("_")
        _message_pairing(icao, "adsb_sign", (callsign,), data_queue, area_config)


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
        temperature = pyModeS.bds.bds44.temp44(message)
        wind_speed, wind_direction = pyModeS.bds.bds44.wind44(message)
        if temperature is not None and wind_speed is not None and wind_direction is not None:
            _message_pairing(
                icao, "bds44", (temperature, wind_speed, wind_direction), data_queue, area_config
            )

    elif pyModeS.bds.bds50.is50(message):
        logging.debug("receive BDS50")
        trackangle = pyModeS.commb.trk50(message)
        groundspeed = pyModeS.commb.gs50(message)
        trueair = pyModeS.commb.tas50(message)
        _message_pairing(icao, "bds50", (trackangle, groundspeed, trueair), data_queue, area_config)

    elif pyModeS.bds.bds60.is60(message):
        logging.debug("receive BDS60")
        heading = pyModeS.commb.hdg60(message)
        indicatedair = pyModeS.commb.ias60(message)
        mach = pyModeS.commb.mach60(message)
        _message_pairing(icao, "bds60", (heading, indicatedair, mach), data_queue, area_config)


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

    icao = str(pyModeS.icao(message))
    dformat = pyModeS.df(message)

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
    for _ in range(int(delay * 10)):
        if _should_terminate.is_set():
            break
        time.sleep(0.1)


def _process_socket_messages(
    sock: socket.socket,
    data_queue: multiprocessing.Queue[MeteorologicalData] | queue.Queue[MeteorologicalData],
    area_config: Area,
) -> None:
    """ソケットからメッセージを受信して処理する"""
    for line in _receive_lines(sock):
        if _should_terminate.is_set():
            break

        try:
            _process_message(line, data_queue, area_config)

            # データ受信成功時にLivenessファイル更新
            if _receiver_liveness_file is not None:
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
        sock.settimeout(amdar.constants.MODES_RECEIVER_SOCKET_TIMEOUT)
        sock.connect((host, port))
        logging.info("%s:%d に接続しました", host, port)

        _process_socket_messages(sock, data_queue, area_config)

        if _should_terminate.is_set():
            return True

        logging.warning("リモートホストによって接続が閉じられました")
        return True


def _worker(
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
    _should_terminate.clear()
    retry_count = 0

    while not _should_terminate.is_set():
        try:
            _handle_connection(host, port, data_queue, area_config)
            retry_count = 0  # 接続成功でリセット

            if _should_terminate.is_set():
                break

        except TimeoutError:
            logging.warning("ソケットタイムアウトが発生しました")

        except (OSError, ConnectionError) as e:
            retry_count += 1
            if retry_count > amdar.constants.MODES_RECEIVER_MAX_RETRIES:
                max_retries = amdar.constants.MODES_RECEIVER_MAX_RETRIES
                error_message = f"最大再接続回数（{max_retries}回）に達しました。処理を終了します"
                logging.error(error_message)
                if _slack_config is not None:
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
                amdar.constants.MODES_RECEIVER_MAX_RETRIES,
                e,
                delay,
            )
            _wait_with_interrupt(delay)

        except Exception:
            logging.exception("受信ワーカーで予期しないエラーが発生しました")
            break

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
    global _receiver_liveness_file, _slack_config, _shared_buffer
    _receiver_liveness_file = config.liveness.file.receiver.modes
    _slack_config = config.slack
    _shared_buffer = buffer

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
    _should_terminate.set()


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

        if _should_terminate.is_set():
            break
