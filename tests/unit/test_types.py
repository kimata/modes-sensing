#!/usr/bin/env python3
# ruff: noqa: S101
"""core/types.py のユニットテスト"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from amdar.core.types import WeatherObservation, WindData


class TestWindData:
    """WindData のテスト"""

    def test_from_polar(self) -> None:
        """極座標から生成"""
        wind = WindData.from_polar(speed_ms=10.0, direction_deg=270.0)

        # 270度（西風）なので、東向きのベクトル
        assert wind.speed == pytest.approx(10.0)
        assert wind.angle == 270.0
        # 風向は「風が来る方向」なので、ベクトルは逆向き
        assert wind.x == pytest.approx(10.0, abs=0.01)  # 東向き
        assert wind.y == pytest.approx(0.0, abs=0.01)

    def test_from_imperial(self) -> None:
        """航空単位系から生成"""
        wind = WindData.from_imperial(speed_kt=100.0, direction_deg=180.0)

        # 100kt ≈ 51.4444 m/s
        assert wind.speed == pytest.approx(51.4444, abs=0.01)
        assert wind.angle == 180.0
        # 180度（南風）なので、北向きのベクトル
        assert wind.x == pytest.approx(0.0, abs=0.01)
        assert wind.y == pytest.approx(51.4444, abs=0.01)


class TestWeatherObservation:
    """WeatherObservation のテスト"""

    def test_from_imperial_basic(self) -> None:
        """基本的な航空単位系からの変換"""
        obs = WeatherObservation.from_imperial(
            altitude_ft=35000.0,
            temperature_c=-50.0,
            callsign="JAL123",
        )

        # 35000ft ≈ 10668m
        assert obs.altitude == pytest.approx(10668.0, abs=1.0)
        assert obs.temperature == -50.0
        assert obs.callsign == "JAL123"
        assert obs.wind is None

    def test_from_imperial_with_wind(self) -> None:
        """風データ付きの変換"""
        obs = WeatherObservation.from_imperial(
            altitude_ft=30000.0,
            temperature_c=-45.0,
            wind_speed_kt=80.0,
            wind_direction_deg=270.0,
            callsign="ANA456",
        )

        assert obs.wind is not None
        assert obs.wind.speed == pytest.approx(80.0 * 0.514444, abs=0.01)
        assert obs.wind.angle == 270.0

    def test_from_imperial_with_all_fields(self) -> None:
        """全フィールド指定の変換"""
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        obs = WeatherObservation.from_imperial(
            timestamp=timestamp,
            icao="84C27A",
            callsign="JAL789",
            altitude_ft=40000.0,
            latitude=35.0,
            longitude=139.0,
            temperature_c=-55.0,
            wind_speed_kt=100.0,
            wind_direction_deg=180.0,
            distance=50.0,
            method="mode-s",
            data_source="bds44",
        )

        assert obs.timestamp == timestamp
        assert obs.icao == "84C27A"
        assert obs.callsign == "JAL789"
        assert obs.altitude == pytest.approx(12192.0, abs=1.0)
        assert obs.latitude == 35.0
        assert obs.longitude == 139.0
        assert obs.temperature == -55.0
        assert obs.distance == 50.0
        assert obs.method == "mode-s"
        assert obs.data_source == "bds44"

    def test_is_valid_complete(self) -> None:
        """完全なデータの有効性チェック"""
        obs = WeatherObservation.from_imperial(
            altitude_ft=30000.0,
            temperature_c=-45.0,
            callsign="TEST",
        )
        assert obs.is_valid() is True

    def test_is_valid_no_id(self) -> None:
        """識別子なしは無効"""
        obs = WeatherObservation(
            altitude=10000.0,
            temperature=-40.0,
        )
        assert obs.is_valid() is False

    def test_is_valid_no_weather(self) -> None:
        """気象データなしは無効"""
        obs = WeatherObservation(
            altitude=10000.0,
            callsign="TEST",
        )
        assert obs.is_valid() is False

    def test_is_valid_zero_altitude(self) -> None:
        """高度ゼロは無効"""
        obs = WeatherObservation(
            altitude=0.0,
            temperature=-40.0,
            callsign="TEST",
        )
        assert obs.is_valid() is False

    def test_has_temperature(self) -> None:
        """温度データ有無チェック"""
        with_temp = WeatherObservation(temperature=-40.0)
        without_temp = WeatherObservation()

        assert with_temp.has_temperature() is True
        assert without_temp.has_temperature() is False

    def test_has_wind(self) -> None:
        """風データ有無チェック"""
        wind = WindData.from_polar(10.0, 180.0)
        with_wind = WeatherObservation(wind=wind)
        without_wind = WeatherObservation()

        assert with_wind.has_wind() is True
        assert without_wind.has_wind() is False


class TestToMeasurementData:
    """to_measurement_data() のテスト"""

    def test_convert_with_wind(self) -> None:
        """風データ付きの変換"""
        obs = WeatherObservation.from_imperial(
            callsign="JAL123",
            altitude_ft=35000.0,
            latitude=35.0,
            longitude=139.0,
            temperature_c=-50.0,
            wind_speed_kt=100.0,
            wind_direction_deg=270.0,
            distance=50.0,
            method="mode-s",
        )

        data = obs.to_measurement_data()

        assert data.callsign == "JAL123"
        assert data.altitude == pytest.approx(35000.0 * 0.3048, rel=0.01)
        assert data.latitude == 35.0
        assert data.longitude == 139.0
        assert data.temperature == -50.0
        assert data.wind.speed == pytest.approx(100.0 * 0.514444, rel=0.01)
        assert data.wind.angle == 270.0
        assert data.distance == 50.0
        assert data.method == "mode-s"

    def test_convert_without_wind(self) -> None:
        """風データなしの変換"""
        obs = WeatherObservation.from_imperial(
            callsign="ANA456",
            altitude_ft=30000.0,
            temperature_c=-45.0,
        )

        data = obs.to_measurement_data()

        assert data.callsign == "ANA456"
        assert data.wind.speed == 0.0
        assert data.wind.angle == 0.0

    def test_convert_uses_icao_as_callsign(self) -> None:
        """callsign がない場合は icao を使用"""
        obs = WeatherObservation(
            icao="84C27A",
            altitude=10000.0,
            temperature=-40.0,
        )

        data = obs.to_measurement_data()

        assert data.callsign == "84C27A"

    def test_convert_none_latitude_longitude(self) -> None:
        """位置が None の場合は 0.0"""
        obs = WeatherObservation(
            callsign="TEST",
            altitude=10000.0,
            temperature=-40.0,
            latitude=None,
            longitude=None,
        )

        data = obs.to_measurement_data()

        assert data.latitude == 0.0
        assert data.longitude == 0.0

    def test_convert_none_temperature(self) -> None:
        """温度が None の場合は 0.0"""
        obs = WeatherObservation(
            callsign="TEST",
            altitude=10000.0,
            temperature=None,
        )

        data = obs.to_measurement_data()

        assert data.temperature == 0.0
