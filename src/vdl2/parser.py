"""ACARS メッセージから気象データを抽出するパーサー

dumpvdl2 の JSON 出力から気象データを抽出し、MeasurementData に変換します。
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import modes.database_postgresql


@dataclass
class AcarsWeatherData:
    """ACARS から抽出した気象データ（生データ形式）"""

    flight: str
    reg: str | None
    timestamp: datetime | None
    latitude: float | None  # 度
    longitude: float | None  # 度
    altitude_ft: int | None  # フィート
    temperature_c: float | None  # 摂氏
    wind_dir_deg: int | None  # 度（風が来る方向）
    wind_speed_kt: int | None  # ノット


def _parse_wn_line(msg_text: str) -> dict[str, Any] | None:
    """WN形式の位置報告から気象データを抽出する

    フォーマット例:
    - WN35123E136555014610P24008M33260081027720
    - WN35 95E137163014813 24003-35261 78 10520
    - WN35031E13647101520837998-47256177 72135

    構造:
    WN + 風向(2-3桁) + 空白? + 風速(2-3桁) + E/W + 経度 + 時刻(6桁)
        + (P?)高度(5桁) + (M/P/-)温度(2桁) + ...
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
    pattern1 = re.search(
        r"WN(\d{2,3})\s*(\d{2,3})([EW])(\d{5,6})\d{6}\s*"  # WN + 風向 + 風速 + 経度方向 + 経度 + 時刻
        r"(?:P)?(\d{5})"  # 高度 (Pはオプション)
        r"([MP-])(\d{2})",  # 温度 (M/P/-)
        wn_line,
    )

    # パターン2: スペース区切りで高度と温度がある場合
    pattern2 = re.search(
        r"WN(\d{2,3})\s*(\d{2,3})([EW])(\d{5,6})\s+"  # WN + 風向 + 風速 + 経度方向 + 経度 + ...
        r"(\d{5})([MP-])(\d{2})",  # 高度 + 温度
        wn_line,
    )

    pattern = pattern1 or pattern2
    if not pattern:
        return None

    wind_dir = int(pattern.group(1))
    if wind_dir < 100:  # 2桁の場合は10倍（35 → 350）
        wind_dir *= 10
    wind_speed = int(pattern.group(2))

    # 経度の解析
    lon_dir = pattern.group(3)
    lon_raw = pattern.group(4)
    if len(lon_raw) == 5:
        # 13655 → 136度 55分 → 136.917度
        lon = int(lon_raw[:3]) + int(lon_raw[3:]) / 60
    else:
        # 136555 → 136.555度 (小数表記)
        lon = int(lon_raw[:3]) + int(lon_raw[3:]) / 1000
    if lon_dir == "W":
        lon = -lon

    altitude = int(pattern.group(5))
    temp_sign = pattern.group(6)
    temp_value = int(pattern.group(7))

    temperature = -temp_value if temp_sign in ("M", "-") else temp_value

    return {
        "altitude_ft": altitude,
        "temperature_c": temperature,
        "wind_dir_deg": wind_dir,
        "wind_speed_kt": wind_speed,
        "longitude": lon,
    }


def _parse_pntaf_format(msg_text: str) -> dict[str, Any] | None:
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

    return {
        "altitude_ft": altitude,
        "temperature_c": temperature,
        "wind_dir_deg": wind_dir,
        "wind_speed_kt": wind_speed,
        "latitude": lat,
        "longitude": lon,
    }


def _parse_wx_format(msg_text: str) -> dict[str, Any] | None:
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

    return {
        "altitude_ft": altitude,
        "temperature_c": temperature,
        "wind_dir_deg": None,  # WX形式では風向の解析が複雑
        "wind_speed_kt": None,
        "latitude": lat,
        "longitude": lon,
    }


def _parse_fl_format(msg_text: str) -> dict[str, Any] | None:
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

    return {
        "altitude_ft": altitude,
        "temperature_c": temperature,
        "wind_dir_deg": None,
        "wind_speed_kt": None,
    }


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
        timestamp = datetime.fromtimestamp(sec + usec / 1e6, tz=UTC)

    # WN形式を優先的に試す（最も情報量が多い）
    result = _parse_wn_line(msg_text)
    if result:
        return AcarsWeatherData(
            flight=flight,
            reg=reg,
            timestamp=timestamp,
            latitude=result.get("latitude"),
            longitude=result.get("longitude"),
            altitude_ft=result.get("altitude_ft"),
            temperature_c=result.get("temperature_c"),
            wind_dir_deg=result.get("wind_dir_deg"),
            wind_speed_kt=result.get("wind_speed_kt"),
        )

    # PNTAF形式を試す（JAL等で使用）
    result = _parse_pntaf_format(msg_text)
    if result:
        return AcarsWeatherData(
            flight=flight,
            reg=reg,
            timestamp=timestamp,
            latitude=result.get("latitude"),
            longitude=result.get("longitude"),
            altitude_ft=result.get("altitude_ft"),
            temperature_c=result.get("temperature_c"),
            wind_dir_deg=result.get("wind_dir_deg"),
            wind_speed_kt=result.get("wind_speed_kt"),
        )

    # WX形式を試す（ANA等で使用）
    result = _parse_wx_format(msg_text)
    if result:
        return AcarsWeatherData(
            flight=flight,
            reg=reg,
            timestamp=timestamp,
            latitude=result.get("latitude"),
            longitude=result.get("longitude"),
            altitude_ft=result.get("altitude_ft"),
            temperature_c=result.get("temperature_c"),
            wind_dir_deg=result.get("wind_dir_deg"),
            wind_speed_kt=result.get("wind_speed_kt"),
        )

    # FL形式を試す
    result = _parse_fl_format(msg_text)
    if result and result["altitude_ft"]:
        return AcarsWeatherData(
            flight=flight,
            reg=reg,
            timestamp=timestamp,
            latitude=None,
            longitude=None,
            altitude_ft=result.get("altitude_ft"),
            temperature_c=result.get("temperature_c"),
            wind_dir_deg=result.get("wind_dir_deg"),
            wind_speed_kt=result.get("wind_speed_kt"),
        )

    return None


def convert_to_measurement_data(
    acars: AcarsWeatherData,
    ref_lat: float,
    ref_lon: float,
) -> modes.database_postgresql.MeasurementData | None:
    """AcarsWeatherData を MeasurementData に変換する

    単位変換:
    - 高度: feet → meter
    - 風速: knot → m/s

    Args:
        acars: ACARS 気象データ
        ref_lat: 基準点の緯度（距離計算用）
        ref_lon: 基準点の経度（距離計算用）

    Returns:
        変換された MeasurementData、または必須データが欠けている場合は None
    """
    # 必須データのチェック
    if acars.altitude_ft is None or acars.temperature_c is None:
        return None

    # 高度変換: feet → meter
    altitude_m = acars.altitude_ft * 0.3048

    # 風データの変換
    if acars.wind_dir_deg is not None and acars.wind_speed_kt is not None:
        # 風速変換: knot → m/s
        wind_speed_ms = acars.wind_speed_kt * 0.514444

        # 風向から x, y 成分を計算
        # 風向は「風が来る方向」なので、ベクトルは逆向き
        wind_rad = math.radians(acars.wind_dir_deg)
        wind_x = -wind_speed_ms * math.sin(wind_rad)
        wind_y = -wind_speed_ms * math.cos(wind_rad)

        wind = modes.database_postgresql.WindData(
            x=wind_x,
            y=wind_y,
            angle=float(acars.wind_dir_deg),
            speed=wind_speed_ms,
        )
    else:
        # 風データなし
        wind = modes.database_postgresql.WindData(x=0.0, y=0.0, angle=0.0, speed=0.0)

    # 距離計算
    distance = 0.0
    if acars.latitude is not None and acars.longitude is not None:
        # 簡易的な距離計算（度からの概算）
        lat_diff = acars.latitude - ref_lat
        lon_diff = acars.longitude - ref_lon
        # 緯度1度 ≈ 111km, 経度1度 ≈ 111km * cos(lat)
        lat_dist = lat_diff * 111.0
        lon_dist = lon_diff * 111.0 * math.cos(math.radians(ref_lat))
        distance = math.sqrt(lat_dist**2 + lon_dist**2)

    return modes.database_postgresql.MeasurementData(
        callsign=acars.flight,
        altitude=altitude_m,
        latitude=acars.latitude if acars.latitude is not None else 0.0,
        longitude=acars.longitude if acars.longitude is not None else 0.0,
        temperature=float(acars.temperature_c),
        wind=wind,
        distance=distance,
    )
