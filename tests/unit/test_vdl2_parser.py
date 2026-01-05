#!/usr/bin/env python3
# ruff: noqa: S101
"""VDL2 パーサーのユニットテスト"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from vdl2.parser import (
    AcarsWeatherData,
    _parse_fl_format,
    _parse_pntaf_format,
    _parse_wn_line,
    _parse_wx_format,
    convert_to_measurement_data,
    get_icao_from_message,
    parse_acars_weather,
    parse_xid_location,
)


class TestParseWnLine:
    """WN形式パーサーのテスト"""

    def test_parse_wn_pattern1(self) -> None:
        """パターン1（連結形式）のパース"""
        msg = "WN35123E136555014610P24008M33260081027720"
        result = _parse_wn_line(msg)

        assert result is not None
        # 正規表現は貪欲マッチングなので 351 が風向として解析される
        assert result["wind_dir_deg"] == 351
        assert result["wind_speed_kt"] == 23
        assert result["altitude_ft"] == 24008
        assert result["temperature_c"] == -33

    def test_parse_wn_pattern2(self) -> None:
        """パターン2（スペース区切り）のパース"""
        msg = "WN35 95E137163014813 24003-35261 78 10520"
        result = _parse_wn_line(msg)

        assert result is not None
        assert result["wind_dir_deg"] == 350  # 35 -> 350
        assert result["wind_speed_kt"] == 95

    def test_parse_wn_no_match(self) -> None:
        """WN形式でないメッセージ"""
        msg = "Some other message"
        result = _parse_wn_line(msg)
        assert result is None

    def test_parse_wn_multiline(self) -> None:
        """複数行メッセージからWN行を抽出"""
        msg = "Header\r\nWN35123E136555014610P24008M33260081027720\r\nFooter"
        result = _parse_wn_line(msg)

        assert result is not None
        assert result["altitude_ft"] == 24008


class TestParsePntafFormat:
    """PNTAF形式パーサーのテスト"""

    def test_parse_pntaf_pattern1(self) -> None:
        """パターン1（スペース区切り）のパース"""
        msg = "N34571E137256020924001-34258 69 106"
        result = _parse_pntaf_format(msg)

        assert result is not None
        assert result["latitude"] == pytest.approx(34.571, abs=0.001)
        assert result["longitude"] == pytest.approx(137.256, abs=0.001)
        assert result["temperature_c"] == -34
        assert result["wind_dir_deg"] == 258
        assert result["wind_speed_kt"] == 69

    def test_parse_pntaf_pattern2(self) -> None:
        """パターン2（連続形式）のパース"""
        msg = "N35053E137022023522410M302590750086"
        result = _parse_pntaf_format(msg)

        assert result is not None
        assert result["latitude"] == pytest.approx(35.053, abs=0.001)
        assert result["longitude"] == pytest.approx(137.022, abs=0.001)
        assert result["altitude_ft"] == 41000  # FL410

    def test_parse_pntaf_no_match(self) -> None:
        """PNTAF形式でないメッセージ"""
        msg = "Some other message"
        result = _parse_pntaf_format(msg)
        assert result is None


class TestParseWxFormat:
    """WX形式パーサーのテスト"""

    def test_parse_wx_format(self) -> None:
        """WX形式のパース"""
        msg = "/WX02EN05RJORRJTTN35302E13630603042690M4302490750CRS 24003020)"
        result = _parse_wx_format(msg)

        assert result is not None
        assert result["latitude"] == pytest.approx(35.302, abs=0.001)
        assert result["longitude"] == pytest.approx(136.306, abs=0.001)
        assert result["temperature_c"] == -43
        assert result["altitude_ft"] == 24003

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
        assert result["altitude_ft"] == 35000
        assert result["temperature_c"] == -45

    def test_parse_fl_slash_temp(self) -> None:
        """スラッシュ区切り温度のパース"""
        msg = "FL350/-45"
        result = _parse_fl_format(msg)

        assert result is not None
        assert result["altitude_ft"] == 35000
        assert result["temperature_c"] == -45

    def test_parse_fl_no_temp(self) -> None:
        """温度なしFL形式のパース"""
        msg = "FL350"
        result = _parse_fl_format(msg)

        assert result is not None
        assert result["altitude_ft"] == 35000
        assert result["temperature_c"] is None

    def test_parse_fl_no_match(self) -> None:
        """FL形式でないメッセージ"""
        msg = "Some other message"
        result = _parse_fl_format(msg)
        assert result is None


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
        json_line = json.dumps(data)
        result = parse_acars_weather(json_line)

        assert result is not None
        assert result.flight == "JAL123"
        assert result.reg == "JA123A"
        assert result.altitude_ft == 24008
        assert result.temperature_c == -33
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
        json_line = json.dumps(data)
        result = parse_acars_weather(json_line)
        assert result is None

    def test_parse_acars_invalid_json(self) -> None:
        """無効なJSONのパース"""
        result = parse_acars_weather("not json")
        assert result is None

    def test_parse_acars_no_acars(self) -> None:
        """ACARSデータのないメッセージ"""
        data = {"vdl2": {"avlc": {}}}
        json_line = json.dumps(data)
        result = parse_acars_weather(json_line)
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
        json_line = json.dumps(data)
        result = parse_xid_location(json_line)

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
        json_line = json.dumps(data)
        result = parse_xid_location(json_line)
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
        json_line = json.dumps(data)
        result = parse_xid_location(json_line)
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
        json_line = json.dumps(data)
        result = get_icao_from_message(json_line)
        assert result == "84C27A"

    def test_get_icao_no_src(self) -> None:
        """srcがない場合"""
        data = {"vdl2": {"avlc": {}}}
        json_line = json.dumps(data)
        result = get_icao_from_message(json_line)
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
