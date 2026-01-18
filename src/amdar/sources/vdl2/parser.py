"""ACARS メッセージから気象データを抽出するパーサー

dumpvdl2 の JSON 出力から気象データを抽出し、WeatherObservation に変換します。
"""

from __future__ import annotations

import datetime
import json
import pathlib
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import amdar.sources.modes.receiver

import my_lib.time

import amdar.constants
import amdar.core.geo
import amdar.database.postgresql
from amdar.core.types import WeatherObservation


@dataclass
class ParsedWeatherData:
    """パーサー関数の共通戻り値型

    各フォーマットのパーサー関数が返す解析結果を統一的に扱う。
    """

    latitude: float | None = None
    longitude: float | None = None
    altitude_ft: int | None = None
    temperature_c: float | None = None
    wind_dir_deg: int | None = None
    wind_speed_kt: int | None = None


@dataclass
class AcarsWeatherData:
    """ACARS から抽出した気象データ（生データ形式）"""

    flight: str
    reg: str | None
    timestamp: datetime.datetime | None
    latitude: float | None  # 度
    longitude: float | None  # 度
    altitude_ft: int | None  # フィート
    temperature_c: float | None  # 摂氏
    wind_dir_deg: int | None  # 度（風が来る方向）
    wind_speed_kt: int | None  # ノット


@dataclass
class XidLocationData:
    """XID メッセージから抽出した位置・高度データ"""

    icao: str  # 航空機アドレス
    timestamp: datetime.datetime | None
    latitude: float | None  # 度
    longitude: float | None  # 度
    altitude_ft: int | None  # フィート


def _parse_wn_line(msg_text: str) -> ParsedWeatherData | None:
    """WN形式の位置報告から気象データを抽出する

    フォーマット例:
    - WN35050E13655100384918002-24291044005200 (実データ形式)
    - WN34514E13729000390739998-48258119 54770 (スペース区切り)
    - WN35123E136555014610P24008M33260081027720 (P接頭辞付き高度)

    構造:
    WN + 緯度(5桁) + E/W + 経度(5-6桁) + 時刻(6桁)
        + (P?)高度(5桁) + 温度符号(M/P/-) + 温度(2桁)
        + 風向(3桁) + 風速(2-3桁) + ...
    """
    # WN行を探す
    lines = msg_text.split("\r\n")
    wn_line = None
    for line in lines:
        if "WN" in line:
            wn_line = line
            break

    if not wn_line:
        return None

    # パターン1: 全フィールドがスペースなしで連結されている場合
    # WN35050E13655100384918002-24291044005200
    # WN35123E136555014610P24008M33260081027720
    pattern1 = re.search(
        r"WN(\d{5})"  # 緯度 (5桁)
        r"([EW])(\d{5,6})"  # 経度方向 + 経度 (5-6桁)
        r"\d{6}"  # 時刻 (6桁)
        r"P?(\d{5})"  # 高度 (5桁, P接頭辞はオプション)
        r"([MP-])(\d{2})"  # 温度符号 + 温度 (2桁)
        r"(\d{3})(\d{2,3})",  # 風向 (3桁) + 風速 (2-3桁)
        wn_line,
    )

    # パターン2: 風速の前後にスペースがある場合
    # WN34514E13729000390739998-48258119 54770
    pattern2 = re.search(
        r"WN(\d{5})"  # 緯度 (5桁)
        r"([EW])(\d{5,6})"  # 経度方向 + 経度 (5-6桁)
        r"\d{6}"  # 時刻 (6桁)
        r"P?(\d{5})"  # 高度 (5桁, P接頭辞はオプション)
        r"([MP-])(\d{2})"  # 温度符号 + 温度 (2桁)
        r"(\d{3})\s+(\d{2,3})",  # 風向 (3桁) + 空白 + 風速 (2-3桁)
        wn_line,
    )

    pattern = pattern1 or pattern2
    if not pattern:
        return None

    # 緯度の解析 (35050 → 35.050度)
    lat_raw = pattern.group(1)
    lat = int(lat_raw[:2]) + int(lat_raw[2:]) / 1000

    # 経度の解析
    lon_dir = pattern.group(2)
    lon_raw = pattern.group(3)
    if len(lon_raw) == 5:
        # 13655 → 136度 55分 → 136.917度
        lon = int(lon_raw[:3]) + int(lon_raw[3:]) / 60
    else:
        # 136551 → 136.551度 (小数表記)
        lon = int(lon_raw[:3]) + int(lon_raw[3:]) / 1000
    if lon_dir == "W":
        lon = -lon

    altitude = int(pattern.group(4))
    temp_sign = pattern.group(5)
    temp_value = int(pattern.group(6))

    temperature = -temp_value if temp_sign in ("M", "-") else temp_value

    wind_dir = int(pattern.group(7))
    wind_speed = int(pattern.group(8))

    return ParsedWeatherData(
        latitude=lat,
        longitude=lon,
        altitude_ft=altitude,
        temperature_c=temperature,
        wind_dir_deg=wind_dir,
        wind_speed_kt=wind_speed,
    )


def _parse_pntaf_format(msg_text: str) -> ParsedWeatherData | None:
    """PNTAF形式（JAL等で使用）から気象データを抽出する

    フォーマット例:
    - N34571E137256020924001-34258 69 106  (パターン1: スペース区切り)
    - N35053E137022023522410M302590750086  (パターン2: 連続)

    構造:
    N/S + 緯度(5桁) + E/W + 経度(6桁) + 時刻(6桁) + 高度?(3桁)
        + 温度(M/P/-)(2桁) + 風向(3桁) + 風速(2-3桁) + ...
    """
    # パターン1: スペース区切り（元のパターン）
    pattern1 = re.search(
        r"([NS])(\d{5})([EW])(\d{6})"  # 緯度 + 経度
        r"(\d{6})"  # 時刻
        r"(\d{3})"  # 高度?（不明）
        r"([MP-])(\d{2})"  # 温度
        r"(\d{3})\s+(\d{2})",  # 風向 + 風速（スペース区切り）
        msg_text,
    )

    # パターン2: 連続（PNT形式）
    pattern2 = re.search(
        r"([NS])(\d{5})([EW])(\d{6})"  # 緯度 + 経度
        r"(\d{6})"  # 時刻
        r"(\d{3})"  # 高度（FL形式、例: 410 = FL410 = 41000ft）
        r"([MP])(\d{2})"  # 温度
        r"(\d{3})(\d{2,3})",  # 風向 + 風速（連続）
        msg_text,
    )

    pattern = pattern1 or pattern2
    if not pattern:
        return None

    # 緯度の解析
    lat_dir = pattern.group(1)
    lat_raw = pattern.group(2)
    lat = int(lat_raw[:2]) + int(lat_raw[2:]) / 1000
    if lat_dir == "S":
        lat = -lat

    # 経度の解析
    lon_dir = pattern.group(3)
    lon_raw = pattern.group(4)
    lon = int(lon_raw[:3]) + int(lon_raw[3:]) / 1000
    if lon_dir == "W":
        lon = -lon

    # 高度の解析（パターン2の場合はFL形式）
    altitude = None
    if pattern2:
        fl_value = int(pattern.group(6))
        if 100 <= fl_value <= 500:  # FL100-FL500 (10000-50000ft)
            altitude = fl_value * 100

    # 温度の解析
    temp_sign = pattern.group(7)
    temp_value = int(pattern.group(8))
    temperature = -temp_value if temp_sign in ("M", "-") else temp_value

    # 風向・風速の解析
    wind_dir = int(pattern.group(9))
    wind_speed = int(pattern.group(10))

    return ParsedWeatherData(
        latitude=lat,
        longitude=lon,
        altitude_ft=altitude,
        temperature_c=temperature,
        wind_dir_deg=wind_dir,
        wind_speed_kt=wind_speed,
    )


def _parse_wx_format(msg_text: str) -> ParsedWeatherData | None:
    """WX形式（ANA等で使用）から気象データを抽出する

    フォーマット例:
    - /WX02EN05RJORRJTT
      N35302E13630603042690M4302490750CRS 24003020)

    構造が複雑で、CRS（巡航高度）と温度を抽出
    """
    # WX で始まるメッセージかチェック
    if "/WX" not in msg_text:
        return None

    # N/S + 緯度 + E/W + 経度 + ... + M/P + 温度 + ... + CRS + 高度
    pattern = re.search(
        r"([NS])(\d{5})([EW])(\d{6})"  # 緯度 + 経度
        r".*?"
        r"([MP])(\d{2})"  # 温度
        r".*?"
        r"CRS\s+(\d{5})",  # CRS + 高度
        msg_text,
    )

    if not pattern:
        return None

    # 緯度の解析
    lat_dir = pattern.group(1)
    lat_raw = pattern.group(2)
    lat = int(lat_raw[:2]) + int(lat_raw[2:]) / 1000
    if lat_dir == "S":
        lat = -lat

    # 経度の解析
    lon_dir = pattern.group(3)
    lon_raw = pattern.group(4)
    lon = int(lon_raw[:3]) + int(lon_raw[3:]) / 1000
    if lon_dir == "W":
        lon = -lon

    # 温度の解析
    temp_sign = pattern.group(5)
    temp_value = int(pattern.group(6))
    temperature = -temp_value if temp_sign in ("M", "-") else temp_value

    # 高度の解析
    altitude = int(pattern.group(7))

    return ParsedWeatherData(
        latitude=lat,
        longitude=lon,
        altitude_ft=altitude,
        temperature_c=temperature,
        wind_dir_deg=None,  # WX形式では風向の解析が複雑
        wind_speed_kt=None,
    )


def _parse_fl_format(msg_text: str) -> ParsedWeatherData | None:
    """FL形式（Flight Level）から高度と温度を抽出する"""
    # FL350 = 35000ft
    fl_match = re.search(r"FL(\d{3})", msg_text)
    if not fl_match:
        return None

    altitude = int(fl_match.group(1)) * 100

    # FLの後に温度情報があるか探す
    # 例: FL350 M45 または FL350/-45
    temp_match = re.search(r"FL\d{3}\s*[/\s]?\s*([MP-])(\d{2})", msg_text)
    temperature = None
    if temp_match:
        temp_sign = temp_match.group(1)
        temp_value = int(temp_match.group(2))
        temperature = -temp_value if temp_sign in ("M", "-") else temp_value

    return ParsedWeatherData(
        altitude_ft=altitude,
        temperature_c=temperature,
        wind_dir_deg=None,
        wind_speed_kt=None,
    )


def parse_acars_weather(json_line: str | bytes) -> AcarsWeatherData | None:
    """dumpvdl2 の JSON から ACARS 気象データを抽出する

    Args:
        json_line: dumpvdl2 から受信した JSON 行

    Returns:
        抽出した気象データ、または None
    """
    try:
        data = json.loads(json_line)
    except json.JSONDecodeError:
        return None

    vdl2 = data.get("vdl2", {})
    avlc = vdl2.get("avlc", {})
    acars = avlc.get("acars", {})
    msg_text = acars.get("msg_text", "")
    if not msg_text:
        return None

    # フライト情報
    flight = acars.get("flight", "")
    reg = acars.get("reg", "")

    # タイムスタンプを datetime に変換
    sec = vdl2.get("t", {}).get("sec")
    usec = vdl2.get("t", {}).get("usec", 0)
    timestamp = None
    if sec:
        timestamp = datetime.datetime.fromtimestamp(sec + usec / 1e6, tz=datetime.UTC)

    # WN形式を優先的に試す（最も情報量が多い）
    result = _parse_wn_line(msg_text)
    if result:
        return AcarsWeatherData(
            flight=flight,
            reg=reg,
            timestamp=timestamp,
            latitude=result.latitude,
            longitude=result.longitude,
            altitude_ft=result.altitude_ft,
            temperature_c=result.temperature_c,
            wind_dir_deg=result.wind_dir_deg,
            wind_speed_kt=result.wind_speed_kt,
        )

    # PNTAF形式を試す（JAL等で使用）
    result = _parse_pntaf_format(msg_text)
    if result:
        return AcarsWeatherData(
            flight=flight,
            reg=reg,
            timestamp=timestamp,
            latitude=result.latitude,
            longitude=result.longitude,
            altitude_ft=result.altitude_ft,
            temperature_c=result.temperature_c,
            wind_dir_deg=result.wind_dir_deg,
            wind_speed_kt=result.wind_speed_kt,
        )

    # WX形式を試す（ANA等で使用）
    # NOTE: WX形式のパーサーは温度フィールドの解釈に問題があるため無効化
    # 例: M48 が温度ではなくマッハ数の可能性があり、誤った温度値を生成する
    # result = _parse_wx_format(msg_text)
    # if result:
    #     return AcarsWeatherData(
    #         flight=flight,
    #         reg=reg,
    #         timestamp=timestamp,
    #         latitude=result.latitude,
    #         longitude=result.longitude,
    #         altitude_ft=result.altitude_ft,
    #         temperature_c=result.temperature_c,
    #         wind_dir_deg=result.wind_dir_deg,
    #         wind_speed_kt=result.wind_speed_kt,
    #     )

    # FL形式を試す
    result = _parse_fl_format(msg_text)
    if result and result.altitude_ft:
        return AcarsWeatherData(
            flight=flight,
            reg=reg,
            timestamp=timestamp,
            latitude=None,
            longitude=None,
            altitude_ft=result.altitude_ft,
            temperature_c=result.temperature_c,
            wind_dir_deg=result.wind_dir_deg,
            wind_speed_kt=result.wind_speed_kt,
        )

    return None


def convert_to_measurement_data(
    acars: AcarsWeatherData,
    ref_lat: float,
    ref_lon: float,
    received_at: datetime.datetime | None = None,
) -> amdar.database.postgresql.MeasurementData | None:
    """AcarsWeatherData を MeasurementData に変換する

    WeatherObservation.from_imperial() を使用して変換を行い、
    最終的に MeasurementData (DB形式) に変換します。

    Args:
        acars: ACARS 気象データ
        ref_lat: 基準点の緯度（距離計算用）
        ref_lon: 基準点の経度（距離計算用）
        received_at: 受信時刻（指定しない場合は現在時刻）

    Returns:
        変換された MeasurementData、または必須データが欠けている場合は None
    """
    # 必須データのチェック
    if acars.altitude_ft is None or acars.temperature_c is None:
        return None

    # 受信時刻（VDL2 データ内のタイムスタンプは無視し、受信タイミングを使用）
    timestamp = received_at if received_at is not None else my_lib.time.now()

    # 距離計算
    distance = 0.0
    if acars.latitude is not None and acars.longitude is not None:
        distance = amdar.core.geo.simple_distance(acars.latitude, acars.longitude, ref_lat, ref_lon)

    # WeatherObservation.from_imperial() を使用して変換
    observation = WeatherObservation.from_imperial(
        timestamp=timestamp,
        callsign=acars.flight,
        altitude_ft=float(acars.altitude_ft),
        latitude=acars.latitude,
        longitude=acars.longitude,
        temperature_c=float(acars.temperature_c),
        wind_speed_kt=float(acars.wind_speed_kt) if acars.wind_speed_kt is not None else None,
        wind_direction_deg=float(acars.wind_dir_deg) if acars.wind_dir_deg is not None else None,
        distance=distance,
        method=amdar.constants.VDL2_METHOD,
        data_source="acars",
    )

    # MeasurementData (DB形式) に変換して返す
    return observation.to_measurement_data()


def convert_to_weather_observation(
    acars: AcarsWeatherData,
    ref_lat: float,
    ref_lon: float,
    received_at: datetime.datetime | None = None,
) -> WeatherObservation | None:
    """AcarsWeatherData を WeatherObservation に変換する

    from_imperial() を使用して航空単位系からの変換を行います。

    Args:
        acars: ACARS 気象データ
        ref_lat: 基準点の緯度（距離計算用）
        ref_lon: 基準点の経度（距離計算用）
        received_at: 受信時刻（指定しない場合は現在時刻）

    Returns:
        変換された WeatherObservation、または必須データが欠けている場合は None
    """
    # 必須データのチェック（高度と温度が必要）
    if acars.altitude_ft is None or acars.temperature_c is None:
        return None

    # 受信時刻（VDL2 データ内のタイムスタンプは無視し、受信タイミングを使用）
    timestamp = received_at if received_at is not None else my_lib.time.now()

    # 距離計算
    distance = 0.0
    if acars.latitude is not None and acars.longitude is not None:
        distance = amdar.core.geo.simple_distance(acars.latitude, acars.longitude, ref_lat, ref_lon)

    return WeatherObservation.from_imperial(
        timestamp=timestamp,
        callsign=acars.flight,
        altitude_ft=float(acars.altitude_ft),
        latitude=acars.latitude,
        longitude=acars.longitude,
        temperature_c=float(acars.temperature_c),
        wind_speed_kt=float(acars.wind_speed_kt) if acars.wind_speed_kt is not None else None,
        wind_direction_deg=float(acars.wind_dir_deg) if acars.wind_dir_deg is not None else None,
        distance=distance,
        method=amdar.constants.VDL2_METHOD,
        data_source="acars",
        altitude_source="acars",
    )


def parse_xid_location(json_line: str | bytes) -> XidLocationData | None:
    """dumpvdl2 の JSON から XID 位置・高度データを抽出する

    XID メッセージには ac_location フィールドがあり、
    航空機の現在位置と高度が含まれることがある。

    Args:
        json_line: dumpvdl2 から受信した JSON 行

    Returns:
        抽出した位置・高度データ、または None
    """
    try:
        data = json.loads(json_line)
    except json.JSONDecodeError:
        return None

    vdl2 = data.get("vdl2", {})
    avlc = vdl2.get("avlc", {})

    # 航空機アドレス（ICAO）
    src = avlc.get("src", {})
    icao = src.get("addr", "")
    if not icao:
        return None

    # XID 情報
    xid = avlc.get("xid", {})
    if not xid:
        return None

    vdl_params = xid.get("vdl_params", [])

    # ac_location を探す
    ac_location = None
    for param in vdl_params:
        if param.get("name") == "ac_location":
            ac_location = param.get("value", {})
            break

    if not ac_location:
        return None

    # 高度を取得
    altitude_ft = ac_location.get("alt")
    if altitude_ft is None:
        return None

    # 位置を取得
    loc = ac_location.get("loc", {})
    latitude = loc.get("lat")
    longitude = loc.get("lon")

    # タイムスタンプを取得
    t = vdl2.get("t", {})
    sec = t.get("sec")
    usec = t.get("usec", 0)
    timestamp = None
    if sec:
        timestamp = datetime.datetime.fromtimestamp(sec + usec / 1e6, tz=datetime.UTC)

    return XidLocationData(
        icao=icao,
        timestamp=timestamp,
        latitude=latitude,
        longitude=longitude,
        altitude_ft=altitude_ft,
    )


def get_icao_from_message(json_line: str | bytes) -> str | None:
    """dumpvdl2 の JSON から航空機アドレス（ICAO）を抽出する

    Args:
        json_line: dumpvdl2 から受信した JSON 行

    Returns:
        ICAO アドレス、または None
    """
    try:
        data = json.loads(json_line)
    except json.JSONDecodeError:
        return None

    vdl2 = data.get("vdl2", {})
    avlc = vdl2.get("avlc", {})
    src = avlc.get("src", {})
    return src.get("addr") or None


def to_weather_record(acars: AcarsWeatherData) -> amdar.sources.modes.receiver.WeatherRecord | None:
    """AcarsWeatherData を WeatherRecord に変換する

    高度が必須のため、高度がない場合は None を返す。

    Args:
        acars: ACARS 気象データ

    Returns:
        WeatherRecord、または None（高度がない場合）
    """
    import amdar.sources.modes.receiver

    # 高度は必須
    if acars.altitude_ft is None:
        return None

    # 気温または風データが必要
    has_temp = acars.temperature_c is not None
    has_wind = acars.wind_dir_deg is not None and acars.wind_speed_kt is not None
    if not has_temp and not has_wind:
        return None

    return amdar.sources.modes.receiver.WeatherRecord(
        icao=get_icao_from_message(json.dumps({"dummy": True})) or "",  # VDL2 では ICAO 不明の場合あり
        callsign=acars.flight if acars.flight else None,
        altitude_ft=float(acars.altitude_ft),
        latitude=acars.latitude,
        longitude=acars.longitude,
        temperature_c=float(acars.temperature_c) if acars.temperature_c is not None else None,
        wind_speed_kt=float(acars.wind_speed_kt) if acars.wind_speed_kt is not None else None,
        wind_direction_deg=float(acars.wind_dir_deg) if acars.wind_dir_deg is not None else None,
    )


def parse_weather_records_from_file(
    file_path: pathlib.Path,
) -> list[amdar.sources.modes.receiver.WeatherRecord]:
    """VDL2 メッセージファイルからペアリングされた気象レコードを抽出する

    Args:
        file_path: VDL2 メッセージファイルのパス（1行1メッセージ、JSON形式）

    Returns:
        list[WeatherRecord]: ペアリングされた気象レコードのリスト
    """

    import amdar.sources.modes.receiver

    results: list[amdar.sources.modes.receiver.WeatherRecord] = []

    with file_path.open("rb") as f:
        for line in f:
            acars = parse_acars_weather(line)
            if acars is None:
                continue

            # ICAO を取得
            icao = get_icao_from_message(line) or ""

            # 高度が必須
            if acars.altitude_ft is None:
                continue

            # 気温または風データが必要
            has_temp = acars.temperature_c is not None
            has_wind = acars.wind_dir_deg is not None and acars.wind_speed_kt is not None
            if not has_temp and not has_wind:
                continue

            record = amdar.sources.modes.receiver.WeatherRecord(
                icao=icao,
                callsign=acars.flight if acars.flight else None,
                altitude_ft=float(acars.altitude_ft),
                latitude=acars.latitude,
                longitude=acars.longitude,
                temperature_c=float(acars.temperature_c) if acars.temperature_c is not None else None,
                wind_speed_kt=float(acars.wind_speed_kt) if acars.wind_speed_kt is not None else None,
                wind_direction_deg=float(acars.wind_dir_deg) if acars.wind_dir_deg is not None else None,
            )
            results.append(record)

    return results
