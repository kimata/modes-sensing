#!/usr/bin/env python3
# ruff: noqa: S101
"""amdar.core.physics のユニットテスト

Mode S 由来の気温・風計算の共通実装をテストします。
リアルタイム受信（modes/receiver.py）とファイル解析（aggregator.py）が
同一の計算結果を返すことを保証する回帰テストを含みます。
"""

from __future__ import annotations

import pytest

import amdar.core.geo
import amdar.core.physics


class TestCalcTemperature:
    """calc_temperature のテスト"""

    def test_typical_cruise(self) -> None:
        """巡航時の典型値

        TAS=250m/s, Mach=0.8 → 音速 312.5 m/s → 約 -30.7℃
        （k=1.403, M=28.966e-3, R=8.314472 の receiver 版定数による値）
        """
        temp = amdar.core.physics.calc_temperature(250.0, 0.8)
        assert temp == pytest.approx(-30.66, abs=0.05)

    def test_isa_sea_level(self) -> None:
        """ISA 海面付近の音速（約340.3m/s）で約15℃になる"""
        temp = amdar.core.physics.calc_temperature(340.3, 1.0)
        assert temp == pytest.approx(15.0, abs=1.0)

    def test_zero_mach_raises(self) -> None:
        """マッハ数ゼロは ValueError"""
        with pytest.raises(ValueError, match="mach"):
            amdar.core.physics.calc_temperature(250.0, 0.0)

    def test_negative_mach_raises(self) -> None:
        """負のマッハ数は ValueError"""
        with pytest.raises(ValueError, match="mach"):
            amdar.core.physics.calc_temperature(250.0, -0.5)


class TestCalcWind:
    """calc_wind のテスト"""

    def test_zero_wind(self) -> None:
        """対地ベクトルと対気ベクトルが一致すれば風速ゼロ

        機首方位は磁方位のため、磁気偏角分ずらして真方位を一致させる。
        """
        lat, lon = 35.5, 137.0
        declination = amdar.core.geo.calc_magnetic_declination(lat, lon)

        wind = amdar.core.physics.calc_wind(lat, lon, 90.0, 200.0, 90.0 + declination, 200.0)

        assert wind.speed == pytest.approx(0.0, abs=1e-9)

    def test_tailwind_from_west(self) -> None:
        """東進中に対地速度が対気速度より大きい場合、西からの追い風になる"""
        lat, lon = 35.5, 137.0
        declination = amdar.core.geo.calc_magnetic_declination(lat, lon)

        wind = amdar.core.physics.calc_wind(lat, lon, 90.0, 250.0, 90.0 + declination, 200.0)

        assert wind.x == pytest.approx(50.0, abs=1e-9)
        assert wind.y == pytest.approx(0.0, abs=1e-9)
        assert wind.angle == pytest.approx(270.0, abs=1e-6)  # 風が来る方向（西）
        assert wind.speed == pytest.approx(50.0, abs=1e-9)

    def test_regression_reference_values(self) -> None:
        """receiver.py 旧実装 _calc_wind と同一の結果を返す（回帰）

        磁気偏角の実装が2系統に分かれて食い違っていた問題（風向が約16°
        ずれる）の回帰テスト。物理計算を physics モジュールに一本化した際に
        旧 receiver 実装の出力値を基準値として固定している。
        """
        wind = amdar.core.physics.calc_wind(35.5, 137.0, 270.0, 220.0, 265.0, 230.0)

        assert wind.x == pytest.approx(4.17674, abs=1e-4)
        assert wind.y == pytest.approx(51.42749, abs=1e-4)
        assert wind.angle == pytest.approx(184.64315, abs=1e-4)
        assert wind.speed == pytest.approx(51.59683, abs=1e-4)

    def test_uses_gsi_magnetic_declination(self) -> None:
        """磁気偏角は amdar.core.geo.calc_magnetic_declination（国土地理院 2020 年値）を使用"""
        declination = amdar.core.geo.calc_magnetic_declination(37.0, 138.0)
        assert declination == pytest.approx(8 + 15.822 / 60, abs=1e-6)


class TestBds5060WeatherHelper:
    """modes/receiver.py の BDS 5,0/6,0 共通計算ヘルパーのテスト"""

    def test_zero_mach_discards_record(self) -> None:
        """マッハ数ゼロはレコード破棄（None）

        旧 FileAggregator._calc_temperature は -999℃ を返して温度閾値で
        捨てていた。等価な挙動として None（スキップ）を返すことを確認する。
        """
        import amdar.sources.modes.receiver as modes_receiver

        result = modes_receiver._calc_bds50_60_weather(
            35.5,
            137.0,
            (270.0, 428.0, 447.0),  # trackangle, groundspeed [kt], trueair [kt]
            (265.0, 280.0, 0.0),  # heading, indicatedair [kt], mach=0
            "TEST123",
        )
        assert result is None

    def test_valid_pair_returns_weather(self) -> None:
        """正常なペアからは (温度, 風) が返る"""
        import amdar.constants
        import amdar.sources.modes.receiver as modes_receiver

        result = modes_receiver._calc_bds50_60_weather(
            35.5,
            137.0,
            (270.0, 428.0, 447.0),
            (265.0, 280.0, 0.78),
            "TEST123",
        )
        assert result is not None
        temperature, wind = result

        expected_temp = amdar.core.physics.calc_temperature(447.0 * amdar.constants.KNOTS_TO_MS, 0.78)
        assert temperature == pytest.approx(expected_temp)
        assert wind.speed >= 0

    def test_below_threshold_discards_record(self) -> None:
        """温度が異常値閾値未満の場合はレコード破棄（None）"""
        import amdar.sources.modes.receiver as modes_receiver

        # TAS を極端に小さくして温度を閾値未満にする
        result = modes_receiver._calc_bds50_60_weather(
            35.5,
            137.0,
            (270.0, 100.0, 100.0),
            (265.0, 80.0, 0.9),
            "TEST123",
        )
        assert result is None
