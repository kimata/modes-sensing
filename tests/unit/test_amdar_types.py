#!/usr/bin/env python3
# ruff: noqa: S101
"""AMDAR 共通型のユニットテスト"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from amdar.core.types import WeatherObservation, WindData


class TestWindData:
    """WindData のテスト"""

    def test_from_polar_north_wind(self) -> None:
        """北風（北から吹く風）のテスト"""
        wind = WindData.from_polar(speed_ms=10.0, direction_deg=0.0)

        assert wind.speed == pytest.approx(10.0)
        assert wind.angle == 0.0
        # 北風なので y 成分は負（南向きに吹く）
        assert wind.x == pytest.approx(0.0, abs=1e-10)
        assert wind.y == pytest.approx(-10.0)

    def test_from_polar_east_wind(self) -> None:
        """東風（東から吹く風）のテスト"""
        wind = WindData.from_polar(speed_ms=10.0, direction_deg=90.0)

        assert wind.speed == pytest.approx(10.0)
        assert wind.angle == 90.0
        # 東風なので x 成分は負（西向きに吹く）
        assert wind.x == pytest.approx(-10.0)
        assert wind.y == pytest.approx(0.0, abs=1e-10)

    def test_from_polar_south_wind(self) -> None:
        """南風（南から吹く風）のテスト"""
        wind = WindData.from_polar(speed_ms=10.0, direction_deg=180.0)

        assert wind.speed == pytest.approx(10.0)
        assert wind.angle == 180.0
        # 南風なので y 成分は正（北向きに吹く）
        assert wind.x == pytest.approx(0.0, abs=1e-10)
        assert wind.y == pytest.approx(10.0)

    def test_from_polar_west_wind(self) -> None:
        """西風（西から吹く風）のテスト"""
        wind = WindData.from_polar(speed_ms=10.0, direction_deg=270.0)

        assert wind.speed == pytest.approx(10.0)
        assert wind.angle == 270.0
        # 西風なので x 成分は正（東向きに吹く）
        assert wind.x == pytest.approx(10.0)
        assert wind.y == pytest.approx(0.0, abs=1e-10)

    def test_from_imperial(self) -> None:
        """航空単位系（ノット）からの変換テスト"""
        wind = WindData.from_imperial(speed_kt=100.0, direction_deg=180.0)

        # 100kt ≈ 51.4444 m/s
        assert wind.speed == pytest.approx(51.4444, rel=0.001)
        assert wind.angle == 180.0


class TestWeatherObservation:
    """WeatherObservation のテスト"""

    def test_is_valid_with_all_data(self) -> None:
        """全データが揃っている場合は有効"""
        obs = WeatherObservation(
            icao="84C27A",
            altitude=10000.0,
            temperature=-50.0,
        )
        assert obs.is_valid() is True

    def test_is_valid_with_wind_only(self) -> None:
        """風データのみでも有効"""
        obs = WeatherObservation(
            callsign="JAL123",
            altitude=10000.0,
            wind=WindData.from_polar(50.0, 270.0),
        )
        assert obs.is_valid() is True

    def test_is_invalid_without_id(self) -> None:
        """識別子がない場合は無効"""
        obs = WeatherObservation(
            altitude=10000.0,
            temperature=-50.0,
        )
        assert obs.is_valid() is False

    def test_is_invalid_without_altitude(self) -> None:
        """高度がない場合は無効"""
        obs = WeatherObservation(
            icao="84C27A",
            altitude=0.0,
            temperature=-50.0,
        )
        assert obs.is_valid() is False

    def test_is_invalid_without_weather(self) -> None:
        """気象データがない場合は無効"""
        obs = WeatherObservation(
            icao="84C27A",
            altitude=10000.0,
        )
        assert obs.is_valid() is False

    def test_has_temperature(self) -> None:
        """温度データの有無チェック"""
        obs_with_temp = WeatherObservation(temperature=-50.0)
        obs_without_temp = WeatherObservation()

        assert obs_with_temp.has_temperature() is True
        assert obs_without_temp.has_temperature() is False

    def test_has_wind(self) -> None:
        """風データの有無チェック"""
        obs_with_wind = WeatherObservation(wind=WindData.from_polar(50.0, 270.0))
        obs_without_wind = WeatherObservation()

        assert obs_with_wind.has_wind() is True
        assert obs_without_wind.has_wind() is False

    def test_from_imperial(self) -> None:
        """航空単位系からの変換テスト"""
        obs = WeatherObservation.from_imperial(
            altitude_ft=35000.0,
            temperature_c=-50.0,
            wind_speed_kt=100.0,
            wind_direction_deg=270.0,
            icao="84C27A",
            callsign="JAL123",
            method="mode-s",
            data_source="bds50_60",
        )

        # 35000ft ≈ 10668m
        assert obs.altitude == pytest.approx(10668.0, rel=0.001)
        assert obs.temperature == -50.0
        assert obs.wind is not None
        assert obs.wind.speed == pytest.approx(51.4444, rel=0.001)
        assert obs.wind.angle == 270.0
        assert obs.icao == "84C27A"
        assert obs.callsign == "JAL123"
        assert obs.method == "mode-s"
        assert obs.data_source == "bds50_60"

    def test_from_imperial_without_wind(self) -> None:
        """風データなしでの変換テスト"""
        obs = WeatherObservation.from_imperial(
            altitude_ft=35000.0,
            temperature_c=-50.0,
            icao="84C27A",
        )

        assert obs.altitude == pytest.approx(10668.0, rel=0.001)
        assert obs.temperature == -50.0
        assert obs.wind is None

    def test_from_imperial_with_timestamp(self) -> None:
        """タイムスタンプ付きの変換テスト"""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        obs = WeatherObservation.from_imperial(
            altitude_ft=35000.0,
            temperature_c=-50.0,
            timestamp=ts,
            icao="84C27A",
        )

        assert obs.timestamp == ts

    def test_default_method(self) -> None:
        """デフォルトの method は mode-s"""
        obs = WeatherObservation()
        assert obs.method == "mode-s"
