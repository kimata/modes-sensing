#!/usr/bin/env python3
# ruff: noqa: S101
"""VDL2 パーサーのユニットテスト"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from amdar.sources.vdl2.parser import (
    AcarsWeatherData,
    _parse_fl_format,
    _parse_pntaf_format,
    _parse_wn_line,
    _parse_wx_format,
    convert_to_measurement_data,
    convert_to_weather_observation,
    get_icao_from_message,
    parse_acars_weather,
    parse_json_line,
    parse_xid_location,
)


class TestParseWnLine:
    """WN形式パーサーのテスト

    座標は度+分形式（緯度: DDMM.m、経度: DDDMM.m）。
    実データ（vdl2_3h_20260106.jsonl）の WN 報告を同一機の ADS-B 航跡と
    照合し、度+分解釈が正しいことを確認済み（平均誤差 0.7km）。
    """

    def test_parse_wn_pattern1(self) -> None:
        """パターン1（連結形式、P接頭辞付き高度）のパース"""
        # WN + 緯度(5桁) + E + 経度(6桁) + 時刻(6桁) + P + 高度(5桁) + M + 温度(2桁) + 風向(3桁) + 風速(3桁)
        msg = "WN35123E136555014610P24008M33260081027720"
        result = _parse_wn_line(msg)

        assert result is not None
        # 35123 → 35度12.3分, 136555 → 136度55.5分
        assert result.latitude == pytest.approx(35 + 12.3 / 60, abs=0.001)
        assert result.longitude == pytest.approx(136 + 55.5 / 60, abs=0.001)
        assert result.altitude_ft == 24008
        assert result.temperature_c == -33
        assert result.wind_dir_deg == 260
        assert result.wind_speed_kt == 81

    def test_parse_wn_pattern2(self) -> None:
        """パターン2（スペース区切り風速）のパース"""
        # 実データ例: WN34514E13729000390739998-48258119 54770
        msg = "WN34514E13729000390739998-48258119 54770"
        result = _parse_wn_line(msg)

        assert result is not None
        # 34514 → 34度51.4分, 137290 → 137度29.0分
        assert result.latitude == pytest.approx(34 + 51.4 / 60, abs=0.001)
        assert result.longitude == pytest.approx(137 + 29.0 / 60, abs=0.001)
        assert result.altitude_ft == 39998
        assert result.temperature_c == -48
        assert result.wind_dir_deg == 258
        assert result.wind_speed_kt == 119

    def test_parse_wn_no_match(self) -> None:
        """WN形式でないメッセージ"""
        msg = "Some other message"
        result = _parse_wn_line(msg)
        assert result is None

    def test_parse_wn_multiline(self) -> None:
        """複数行メッセージからWN行を抽出"""
        # 実データ例: WN35050E13655100384918002-24291044005200
        msg = "Header\r\nWN35050E13655100384918002-24291044005200\r\nFooter"
        result = _parse_wn_line(msg)

        assert result is not None
        # 35050 → 35度05.0分, 136551 → 136度55.1分
        assert result.latitude == pytest.approx(35 + 5.0 / 60, abs=0.001)
        assert result.longitude == pytest.approx(136 + 55.1 / 60, abs=0.001)
        assert result.altitude_ft == 18002
        assert result.temperature_c == -24
        assert result.wind_dir_deg == 291
        assert result.wind_speed_kt == 44

    def test_parse_wn_real_data_verified_against_adsb(self) -> None:
        """実データの WN 報告が ADS-B 航跡と一致する座標にデコードされる（回帰）

        vdl2_3h_20260106.jsonl 中の NH0929 (ICAO 86D660) の WN 報告。
        同時間帯の同一機 ADS-B 航跡（modes_3h_20260106.txt）との照合で、
        度+分解釈 (35.053, 136.783) は航跡上（誤差 0.0km）、
        旧 /1000 解釈 (35.032, 136.470) は航跡から約 5.4km ずれていた。
        """
        msg = "WN35032E13647002064539998-48258111 73710"
        result = _parse_wn_line(msg)

        assert result is not None
        assert result.latitude == pytest.approx(35 + 3.2 / 60, abs=0.001)  # 35.053
        assert result.longitude == pytest.approx(136 + 47.0 / 60, abs=0.001)  # 136.783
        assert result.altitude_ft == 39998
        assert result.temperature_c == -48
        assert result.wind_dir_deg == 258
        assert result.wind_speed_kt == 111

    def test_parse_wn_invalid_minutes_rejected(self) -> None:
        """分が 60 以上になる座標は不正としてパースしない"""
        # 経度分 75.0（>= 60）
        msg = "WN35109E13775005756140002-49257115 10800"
        result = _parse_wn_line(msg)
        assert result is None


class TestParsePntafFormat:
    """PNTAF形式パーサーのテスト"""

    def test_parse_pntaf_pattern1(self) -> None:
        """パターン1（スペース区切り）のパース"""
        msg = "N34571E137256020924001-34258 69 106"
        result = _parse_pntaf_format(msg)

        assert result is not None
        assert result.latitude == pytest.approx(34.571, abs=0.001)
        assert result.longitude == pytest.approx(137.256, abs=0.001)
        assert result.temperature_c == -34
        assert result.wind_dir_deg == 258
        assert result.wind_speed_kt == 69

    def test_parse_pntaf_pattern2(self) -> None:
        """パターン2（連続形式）のパース"""
        msg = "N35053E137022023522410M302590750086"
        result = _parse_pntaf_format(msg)

        assert result is not None
        assert result.latitude == pytest.approx(35.053, abs=0.001)
        assert result.longitude == pytest.approx(137.022, abs=0.001)
        assert result.altitude_ft == 41000  # FL410

    def test_parse_pntaf_no_match(self) -> None:
        """PNTAF形式でないメッセージ"""
        msg = "Some other message"
        result = _parse_pntaf_format(msg)
        assert result is None

    def test_parse_pntaf_pattern1_altitude_not_misinterpreted(self) -> None:
        """パターン1採用時にパターン2の高度解釈が誤適用されない（回帰）

        テキスト全体がパターン1とパターン2の両方にマッチする場合、
        旧実装（`if pattern2:`）はパターン1の未確定フィールド（410）を
        FL 高度（41000ft）と誤解釈していた。
        """
        # 1行目はパターン1のみ、2行目はパターン2にマッチする
        msg = "N34571E137256020924410-34258 69 106\r\nN35053E137022023522410M302590750086"
        result = _parse_pntaf_format(msg)

        assert result is not None
        # 採用されるのはパターン1（先頭行）なので高度は設定されない
        assert result.latitude == pytest.approx(34.571, abs=0.001)
        assert result.altitude_ft is None
        assert result.temperature_c == -34
        assert result.wind_dir_deg == 258
        assert result.wind_speed_kt == 69


class TestParseWxFormat:
    """WX形式パーサーのテスト"""

    def test_parse_wx_format(self) -> None:
        """WX形式のパース"""
        msg = "/WX02EN05RJORRJTTN35302E13630603042690M4302490750CRS 24003020)"
        result = _parse_wx_format(msg)

        assert result is not None
        assert result.latitude == pytest.approx(35.302, abs=0.001)
        assert result.longitude == pytest.approx(136.306, abs=0.001)
        assert result.temperature_c == -43
        assert result.altitude_ft == 24003

    def test_parse_wx_no_match(self) -> None:
        """WX形式でないメッセージ"""
        msg = "Some other message"
        result = _parse_wx_format(msg)
        assert result is None


class TestParseFlFormat:
    """FL形式パーサーのテスト"""

    def test_parse_fl_with_temp(self) -> None:
        """温度付きFL形式のパース"""
        msg = "FL350 M45"
        result = _parse_fl_format(msg)

        assert result is not None
        assert result.altitude_ft == 35000
        assert result.temperature_c == -45

    def test_parse_fl_slash_temp(self) -> None:
        """スラッシュ区切り温度のパース"""
        msg = "FL350/-45"
        result = _parse_fl_format(msg)

        assert result is not None
        assert result.altitude_ft == 35000
        assert result.temperature_c == -45

    def test_parse_fl_no_temp(self) -> None:
        """温度なしFL形式のパース"""
        msg = "FL350"
        result = _parse_fl_format(msg)

        assert result is not None
        assert result.altitude_ft == 35000
        assert result.temperature_c is None

    def test_parse_fl_no_match(self) -> None:
        """FL形式でないメッセージ"""
        msg = "Some other message"
        result = _parse_fl_format(msg)
        assert result is None


class TestParseJsonLine:
    """JSON 行パーサーのテスト"""

    def test_parse_valid_json(self) -> None:
        """有効なJSONのパース"""
        data = {"vdl2": {"avlc": {}}}
        result = parse_json_line(json.dumps(data))
        assert result == data

    def test_parse_valid_json_bytes(self) -> None:
        """bytes のパース"""
        data = {"vdl2": {}}
        result = parse_json_line(json.dumps(data).encode())
        assert result == data

    def test_parse_invalid_json(self) -> None:
        """無効なJSONは None"""
        assert parse_json_line("not json") is None

    def test_parse_non_dict_json(self) -> None:
        """dict でない JSON は None"""
        assert parse_json_line("[1, 2, 3]") is None


class TestParseAcarsWeather:
    """ACARS気象データ統合パーサーのテスト"""

    def test_parse_acars_wn_format(self) -> None:
        """WN形式のACARSメッセージパース"""
        data = {
            "vdl2": {
                "t": {"sec": 1704067200, "usec": 0},
                "avlc": {
                    "acars": {
                        "flight": "JAL123",
                        "reg": "JA123A",
                        "msg_text": "WN35123E136555014610P24008M33260081027720",
                    }
                },
            }
        }
        result = parse_acars_weather(data)

        assert result is not None
        assert result.flight == "JAL123"
        assert result.reg == "JA123A"
        # 座標は度+分形式（35123 → 35度12.3分, 136555 → 136度55.5分）
        assert result.latitude == pytest.approx(35 + 12.3 / 60, abs=0.001)
        assert result.longitude == pytest.approx(136 + 55.5 / 60, abs=0.001)
        assert result.altitude_ft == 24008
        assert result.temperature_c == -33
        assert result.wind_dir_deg == 260
        assert result.wind_speed_kt == 81
        assert result.timestamp == datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)

    def test_parse_acars_no_weather(self) -> None:
        """気象データのないACARSメッセージ"""
        data = {
            "vdl2": {
                "avlc": {
                    "acars": {
                        "flight": "JAL123",
                        "msg_text": "Hello World",
                    }
                },
            }
        }
        result = parse_acars_weather(data)
        assert result is None

    def test_parse_acars_no_acars(self) -> None:
        """ACARSデータのないメッセージ"""
        data = {"vdl2": {"avlc": {}}}
        result = parse_acars_weather(data)
        assert result is None


class TestParseXidLocation:
    """XID位置パーサーのテスト"""

    def test_parse_xid_with_location(self) -> None:
        """XIDメッセージから位置・高度を抽出"""
        data = {
            "vdl2": {
                "t": {"sec": 1704067200, "usec": 0},
                "avlc": {
                    "src": {"addr": "84C27A", "type": "Aircraft"},
                    "xid": {
                        "vdl_params": [
                            {
                                "name": "ac_location",
                                "value": {
                                    "loc": {"lat": 35.1, "lon": 137.2},
                                    "alt": 27000,
                                },
                            }
                        ]
                    },
                },
            }
        }
        result = parse_xid_location(data)

        assert result is not None
        assert result.icao == "84C27A"
        assert result.altitude_ft == 27000
        assert result.latitude == pytest.approx(35.1, abs=0.001)
        assert result.longitude == pytest.approx(137.2, abs=0.001)

    def test_parse_xid_no_location(self) -> None:
        """ac_locationがないXIDメッセージ"""
        data = {
            "vdl2": {
                "avlc": {
                    "src": {"addr": "84C27A"},
                    "xid": {"vdl_params": []},
                },
            }
        }
        result = parse_xid_location(data)
        assert result is None

    def test_parse_xid_no_xid(self) -> None:
        """XIDフィールドがないメッセージ"""
        data = {
            "vdl2": {
                "avlc": {
                    "src": {"addr": "84C27A"},
                    "acars": {"flight": "JAL123"},
                },
            }
        }
        result = parse_xid_location(data)
        assert result is None


class TestGetIcaoFromMessage:
    """ICAOアドレス抽出のテスト"""

    def test_get_icao(self) -> None:
        """ICAOアドレスを抽出"""
        data = {
            "vdl2": {
                "avlc": {
                    "src": {"addr": "84C27A", "type": "Aircraft"},
                },
            }
        }
        result = get_icao_from_message(data)
        assert result == "84C27A"

    def test_get_icao_no_src(self) -> None:
        """srcがない場合"""
        data = {"vdl2": {"avlc": {}}}
        result = get_icao_from_message(data)
        assert result is None


class TestConvertToMeasurementData:
    """MeasurementData変換のテスト"""

    def test_convert_with_wind(self) -> None:
        """風データ付き変換"""
        acars = AcarsWeatherData(
            flight="JAL123",
            reg="JA123A",
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            latitude=35.0,
            longitude=139.0,
            altitude_ft=35000,
            temperature_c=-50,
            wind_dir_deg=270,
            wind_speed_kt=100,
        )

        result = convert_to_measurement_data(acars, 35.0, 139.0)

        assert result is not None
        assert result.callsign == "JAL123"
        assert result.altitude == pytest.approx(35000 * 0.3048, rel=0.01)
        assert result.temperature == -50
        assert result.wind.speed == pytest.approx(100 * 0.514444, rel=0.01)
        assert result.wind.angle == 270

    def test_convert_without_wind(self) -> None:
        """風データなし変換"""
        acars = AcarsWeatherData(
            flight="ANA456",
            reg="JA456B",
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            latitude=35.0,
            longitude=139.0,
            altitude_ft=35000,
            temperature_c=-50,
            wind_dir_deg=None,
            wind_speed_kt=None,
        )

        result = convert_to_measurement_data(acars, 35.0, 139.0)

        assert result is not None
        assert result.wind.speed == 0.0

    def test_convert_missing_required(self) -> None:
        """必須データ欠落時はNone"""
        acars = AcarsWeatherData(
            flight="JAL123",
            reg=None,
            timestamp=None,
            latitude=None,
            longitude=None,
            altitude_ft=None,  # 高度なし
            temperature_c=-50,
            wind_dir_deg=None,
            wind_speed_kt=None,
        )

        result = convert_to_measurement_data(acars, 35.0, 139.0)
        assert result is None

    def test_convert_distance_calculation(self) -> None:
        """距離計算"""
        acars = AcarsWeatherData(
            flight="JAL123",
            reg="JA123A",
            timestamp=None,
            latitude=36.0,  # 1度北
            longitude=139.0,
            altitude_ft=35000,
            temperature_c=-50,
            wind_dir_deg=None,
            wind_speed_kt=None,
        )

        result = convert_to_measurement_data(acars, 35.0, 139.0)

        assert result is not None
        # 緯度1度 ≈ 111km
        assert result.distance == pytest.approx(111.0, rel=0.1)


class TestConvertToWeatherObservation:
    """WeatherObservation変換のテスト"""

    def test_convert_with_wind(self) -> None:
        """風データ付き変換"""
        acars = AcarsWeatherData(
            flight="JAL123",
            reg="JA123A",
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            latitude=35.0,
            longitude=139.0,
            altitude_ft=35000,
            temperature_c=-50,
            wind_dir_deg=270,
            wind_speed_kt=100,
        )

        result = convert_to_weather_observation(acars, 35.0, 139.0)

        assert result is not None
        assert result.callsign == "JAL123"
        assert result.altitude == pytest.approx(35000 * 0.3048, rel=0.01)
        assert result.temperature == -50
        assert result.wind is not None
        assert result.wind.speed == pytest.approx(100 * 0.514444, rel=0.01)
        assert result.wind.angle == 270
        assert result.method == "vdl2"
        assert result.data_source == "acars"
        assert result.altitude_source == "acars"

    def test_convert_without_wind(self) -> None:
        """風データなし変換"""
        acars = AcarsWeatherData(
            flight="ANA456",
            reg="JA456B",
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            latitude=35.0,
            longitude=139.0,
            altitude_ft=35000,
            temperature_c=-50,
            wind_dir_deg=None,
            wind_speed_kt=None,
        )

        result = convert_to_weather_observation(acars, 35.0, 139.0)

        assert result is not None
        assert result.wind is None

    def test_convert_missing_altitude(self) -> None:
        """高度なしはNone"""
        acars = AcarsWeatherData(
            flight="JAL123",
            reg=None,
            timestamp=None,
            latitude=35.0,
            longitude=139.0,
            altitude_ft=None,
            temperature_c=-50,
            wind_dir_deg=None,
            wind_speed_kt=None,
        )

        result = convert_to_weather_observation(acars, 35.0, 139.0)
        assert result is None

    def test_convert_missing_temperature(self) -> None:
        """温度なしはNone"""
        acars = AcarsWeatherData(
            flight="JAL123",
            reg=None,
            timestamp=None,
            latitude=35.0,
            longitude=139.0,
            altitude_ft=35000,
            temperature_c=None,
            wind_dir_deg=None,
            wind_speed_kt=None,
        )

        result = convert_to_weather_observation(acars, 35.0, 139.0)
        assert result is None

    def test_is_valid(self) -> None:
        """生成されたWeatherObservationが有効か"""
        acars = AcarsWeatherData(
            flight="JAL123",
            reg="JA123A",
            timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            latitude=35.0,
            longitude=139.0,
            altitude_ft=35000,
            temperature_c=-50,
            wind_dir_deg=270,
            wind_speed_kt=100,
        )

        result = convert_to_weather_observation(acars, 35.0, 139.0)

        assert result is not None
        assert result.is_valid() is True
