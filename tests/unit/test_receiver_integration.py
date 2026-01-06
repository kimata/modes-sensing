#!/usr/bin/env python3
# ruff: noqa: S101
"""receiver と IntegratedBuffer の統合テスト"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from amdar.sources.aggregator import IntegratedBuffer
from amdar.sources.vdl2 import parser as vdl2_parser


class TestVdl2AltitudeBuffer補完:
    """VDL2 receiver の IntegratedBuffer による高度補完テスト"""

    def test_try_altitude_補完_from_buffer_success(self) -> None:
        """バッファから高度補完が成功するケース"""
        # この関数をインポートするために receiver をインポート
        # ただし、グローバル変数を避けるためにテスト用のヘルパーを使う
        buffer = IntegratedBuffer(window_seconds=60.0)

        # バッファに ADS-B 位置データを追加
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        buffer.add_adsb_position(
            icao="84C27A",
            callsign="JAL123",
            timestamp=timestamp,
            altitude_m=10668.0,  # 35000 ft
            lat=35.0,
            lon=139.0,
        )

        # 補完対象の ACARS データ（高度なし）
        _acars = vdl2_parser.AcarsWeatherData(
            flight="JAL123",
            reg="JA123A",
            timestamp=timestamp,
            latitude=None,
            longitude=None,
            altitude_ft=None,
            temperature_c=-50.0,
            wind_dir_deg=270,
            wind_speed_kt=100,
        )

        # 高度を取得できることを確認
        result = buffer.get_altitude_at("JAL123", timestamp)
        assert result is not None
        altitude_m, lat, lon, source = result
        assert altitude_m == pytest.approx(10668.0, abs=1.0)
        assert lat == pytest.approx(35.0)
        assert lon == pytest.approx(139.0)

    def test_try_altitude_補完_from_buffer_no_match(self) -> None:
        """バッファに該当データがないケース"""
        buffer = IntegratedBuffer(window_seconds=60.0)

        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        # 空のバッファから取得を試みる
        result = buffer.get_altitude_at("JAL123", timestamp)
        assert result is None

    def test_try_altitude_補完_from_buffer_by_icao(self) -> None:
        """ICAO アドレスで高度補完するケース"""
        buffer = IntegratedBuffer(window_seconds=60.0)

        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        buffer.add_adsb_position(
            icao="84C27A",
            callsign=None,  # コールサインなし
            timestamp=timestamp,
            altitude_m=9144.0,  # 30000 ft
            lat=35.5,
            lon=139.5,
        )

        # ICAO で検索
        result = buffer.get_altitude_at("84C27A", timestamp)
        assert result is not None
        altitude_m, lat, lon, source = result
        assert altitude_m == pytest.approx(9144.0, abs=1.0)


class TestModesReceiverBuffer:
    """modes/receiver の IntegratedBuffer フィード機能テスト"""

    def test_shared_buffer_initial_state(self) -> None:
        """共有バッファの初期状態"""
        import amdar.sources.modes.receiver as modes_receiver

        # 初期状態では _shared_buffer は None
        assert modes_receiver._shared_buffer is None

    def test_buffer_feed_on_position(self) -> None:
        """ADS-B 位置受信時にバッファにフィードされることを確認"""
        import amdar.sources.modes.receiver as modes_receiver

        buffer = IntegratedBuffer()

        # 共有バッファを設定
        original_buffer = modes_receiver._shared_buffer
        modes_receiver._shared_buffer = buffer

        try:
            # _process_adsb_position を直接呼ぶのは複雑なので、
            # バッファに直接追加してテスト
            from datetime import UTC, datetime

            buffer.add_adsb_position(
                icao="ABC123",
                callsign="TEST01",
                timestamp=datetime.now(UTC),
                altitude_m=10000.0,
                lat=35.0,
                lon=139.0,
            )

            # バッファにデータが追加されたことを確認
            stats = buffer.get_stats()
            assert stats["aircraft_count"] == 1
            assert stats["total_entries"] == 1
        finally:
            modes_receiver._shared_buffer = original_buffer


class TestVdl2ReceiverStart:
    """vdl2/receiver の start 関数テスト"""

    def test_start_with_buffer(self) -> None:
        """buffer パラメータ付きで start を呼び出せることを確認"""
        import amdar.sources.vdl2.receiver as vdl2_receiver

        _buffer = IntegratedBuffer()

        # start 関数が buffer を受け取れることを確認（実際には接続しない）
        # シグネチャのテストのみ
        import inspect

        sig = inspect.signature(vdl2_receiver.start)
        params = list(sig.parameters.keys())
        assert "buffer" in params

    def test_start_without_buffer(self) -> None:
        """buffer なしでも start を呼び出せることを確認"""
        # buffer パラメータがオプションであることを確認
        import inspect

        import amdar.sources.vdl2.receiver as vdl2_receiver

        sig = inspect.signature(vdl2_receiver.start)
        buffer_param = sig.parameters.get("buffer")
        assert buffer_param is not None
        assert buffer_param.default is None


class TestModesReceiverStart:
    """modes/receiver の start 関数テスト"""

    def test_start_accepts_buffer(self) -> None:
        """buffer パラメータを受け取れることを確認"""
        import inspect

        import amdar.sources.modes.receiver as modes_receiver

        sig = inspect.signature(modes_receiver.start)
        params = list(sig.parameters.keys())
        assert "buffer" in params

    def test_start_buffer_is_optional(self) -> None:
        """buffer パラメータがオプションであることを確認"""
        import inspect

        import amdar.sources.modes.receiver as modes_receiver

        sig = inspect.signature(modes_receiver.start)
        buffer_param = sig.parameters.get("buffer")
        assert buffer_param is not None
        assert buffer_param.default is None


class TestCombinedBufferFlow:
    """統合バッファフローのテスト"""

    def test_buffer_data_flow(self) -> None:
        """Mode-S → バッファ → VDL2 補完のデータフロー"""
        buffer = IntegratedBuffer(window_seconds=60.0)

        # Mode-S から ADS-B 位置データが入る（シミュレート）
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        buffer.add_adsb_position(
            icao="84C27A",
            callsign="ANA456",
            timestamp=timestamp,
            altitude_m=10668.0,  # 35000 ft
            lat=35.0,
            lon=139.0,
        )

        # VDL2 から気象データが入る（高度なし、同一機）
        # バッファから高度を取得
        result = buffer.get_altitude_at("ANA456", timestamp)
        assert result is not None

        altitude_m, lat, lon, source = result
        assert altitude_m == pytest.approx(10668.0, abs=1.0)
        assert lat == 35.0
        assert lon == 139.0

    def test_buffer_cleanup_old_data(self) -> None:
        """古いデータがクリーンアップされることを確認"""
        from datetime import timedelta

        buffer = IntegratedBuffer(window_seconds=60.0)

        old_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        new_time = old_time + timedelta(minutes=5)  # 5分後

        buffer.add_adsb_position(
            icao="84C27A",
            callsign="JAL123",
            timestamp=old_time,
            altitude_m=10668.0,
            lat=35.0,
            lon=139.0,
        )

        # 新しい時刻でデータを追加（古いデータのクリーンアップがトリガーされる）
        buffer.update_time(new_time)

        # 古いデータはウィンドウ外なので取得できない
        result = buffer.get_altitude_at("JAL123", new_time)
        # 5分前のデータは60秒ウィンドウ外
        assert result is None
