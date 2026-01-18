"""AMDAR 共通データ型定義

航空機気象観測データの統一型を定義します。
"""

from __future__ import annotations

import datetime
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Self

import amdar.constants
from amdar.constants import MethodType

# 再エクスポート: amdar.core.types.MethodType として使用可能にする
__all__ = ["AltitudeSourceType", "DataSourceType", "MethodType", "WeatherObservation", "WindData"]

DataSourceType = Literal["bds44", "bds50_60", "acars_wn", "acars_wx", "acars_fl", "acars_pntaf", "acars", ""]
AltitudeSourceType = Literal["adsb", "acars", "xid", "interpolated", ""]

if TYPE_CHECKING:
    import amdar.database.postgresql


@dataclass
class WindData:
    """風向・風速データ（SI単位系）

    Attributes:
        x: 東西成分 [m/s]（東が正）
        y: 南北成分 [m/s]（北が正）
        angle: 風向 [度]（北=0, 時計回り, 風が来る方向）
        speed: 風速 [m/s]
    """

    x: float
    y: float
    angle: float
    speed: float

    @classmethod
    def from_polar(cls, speed_ms: float, direction_deg: float) -> Self:
        """極座標（風速・風向）から生成

        Args:
            speed_ms: 風速 [m/s]
            direction_deg: 風向 [度]（北=0, 時計回り, 風が来る方向）

        Returns:
            WindData インスタンス
        """
        wind_rad = math.radians(direction_deg)
        # 風向は「風が来る方向」なので、ベクトルは逆向き
        return cls(
            x=-speed_ms * math.sin(wind_rad),
            y=-speed_ms * math.cos(wind_rad),
            angle=direction_deg,
            speed=speed_ms,
        )

    @classmethod
    def from_imperial(cls, speed_kt: float, direction_deg: float) -> Self:
        """航空単位系（ノット）から生成

        Args:
            speed_kt: 風速 [kt]
            direction_deg: 風向 [度]

        Returns:
            WindData インスタンス
        """
        speed_ms = speed_kt * amdar.constants.KNOTS_TO_MS
        return cls.from_polar(speed_ms, direction_deg)


@dataclass
class WeatherObservation:
    """航空機気象観測データ（統一型）

    ADS-B (Mode-S) と VDL2 の両方からの気象データを統一的に扱う。
    単位系は SI単位（m, m/s, ℃）で、DB との互換性を維持。

    必須条件:
    - 高度 (altitude) は必須（0より大きい）
    - 識別子 (icao または callsign) は少なくとも1つ必須
    - 気象データ (temperature または wind) は少なくとも1つ必須

    Attributes:
        timestamp: 観測日時（任意、ファイル解析時は None の場合あり）
        icao: Mode-S アドレス（24bit hex、例: "84C27A"）
        callsign: 便名/コールサイン（例: "JAL123"）
        altitude: 高度 [m]
        latitude: 緯度 [度]
        longitude: 経度 [度]
        distance: 基準点からの距離 [km]
        temperature: 気温 [℃]
        wind: 風データ
        method: 取得手段（"mode-s" または "vdl2"）
        data_source: 詳細ソース（"bds44", "bds50_60", "acars_wn" 等）
        altitude_source: 高度の取得元（"adsb", "acars", "xid", "interpolated"）
    """

    # 時刻（任意 - ファイル解析時は None の場合あり）
    timestamp: datetime.datetime | None = None

    # 識別子（少なくとも1つは必須）
    icao: str | None = None
    callsign: str | None = None

    # 位置・高度（高度は必須）
    altitude: float = 0.0
    latitude: float | None = None
    longitude: float | None = None
    distance: float = 0.0

    # 気象データ（気温または風の少なくとも一方が必須）
    temperature: float | None = None
    wind: WindData | None = None

    # メタ情報
    method: MethodType = amdar.constants.MODE_S_METHOD
    data_source: DataSourceType = ""
    altitude_source: AltitudeSourceType = ""

    def is_valid(self) -> bool:
        """有効な観測データかどうか

        Returns:
            識別子・高度・気象データが揃っている場合 True
        """
        has_id = self.icao is not None or self.callsign is not None
        has_weather = self.temperature is not None or self.wind is not None
        return has_id and has_weather and self.altitude > 0

    def has_temperature(self) -> bool:
        """温度データを持つか"""
        return self.temperature is not None

    def has_wind(self) -> bool:
        """風データを持つか"""
        return self.wind is not None

    @classmethod
    def from_imperial(
        cls,
        *,
        altitude_ft: float,
        temperature_c: float | None = None,
        wind_speed_kt: float | None = None,
        wind_direction_deg: float | None = None,
        **kwargs: object,
    ) -> Self:
        """航空単位系（ft, kt）から変換して生成

        Args:
            altitude_ft: 高度 [ft]
            temperature_c: 気温 [℃]
            wind_speed_kt: 風速 [kt]
            wind_direction_deg: 風向 [度]
            **kwargs: その他のフィールド

        Returns:
            WeatherObservation インスタンス
        """
        altitude_m = altitude_ft * amdar.constants.FEET_TO_METERS

        wind = None
        if wind_speed_kt is not None and wind_direction_deg is not None:
            wind = WindData.from_imperial(wind_speed_kt, wind_direction_deg)

        return cls(
            altitude=altitude_m,
            temperature=temperature_c,
            wind=wind,
            **kwargs,  # type: ignore[arg-type]
        )

    def to_measurement_data(self) -> amdar.database.postgresql.MeasurementData:
        """MeasurementData (DB format) に変換する

        DB 保存用の MeasurementData 形式に変換します。
        WeatherObservation では optional なフィールドも、MeasurementData では
        必須のため、適切なデフォルト値で補完します。

        Returns:
            MeasurementData インスタンス
        """
        import amdar.database.postgresql as db

        # WindData の変換（None の場合はゼロ値）
        wind = self.wind if self.wind is not None else WindData(x=0.0, y=0.0, angle=0.0, speed=0.0)

        return db.MeasurementData(
            callsign=self.callsign or self.icao or "",
            altitude=self.altitude,
            latitude=self.latitude or 0.0,
            longitude=self.longitude or 0.0,
            temperature=self.temperature if self.temperature is not None else 0.0,
            wind=wind,
            distance=self.distance,
            method=self.method,
        )
