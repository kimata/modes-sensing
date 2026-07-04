#!/usr/bin/env python3
# ruff: noqa: S101
"""aggregator.py のユニットテスト

IntegratedBuffer と RealtimeAggregator のテストを行います。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

import amdar.core.geo
import amdar.sources.modes.receiver
from amdar.core.types import WindData
from amdar.sources.aggregator import (
    AltitudeEntry,
    FileAggregator,
    IntegratedBuffer,
    RealtimeAggregator,
    parse_from_files,
)


class TestAltitudeEntry:
    """AltitudeEntry のテスト"""

    def test_basic_creation(self) -> None:
        """基本的な作成"""
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        entry = AltitudeEntry(
            timestamp=ts,
            altitude_m=10000.0,
            latitude=35.5,
            longitude=139.5,
            message_index=100,
        )

        assert entry.timestamp == ts
        assert entry.altitude_m == 10000.0
        assert entry.latitude == 35.5
        assert entry.longitude == 139.5
        assert entry.message_index == 100

    def test_optional_fields(self) -> None:
        """オプションフィールドのデフォルト値"""
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        entry = AltitudeEntry(timestamp=ts, altitude_m=10000.0)

        assert entry.latitude is None
        assert entry.longitude is None
        assert entry.message_index == 0


class TestIntegratedBuffer:
    """IntegratedBuffer のテスト"""

    def test_add_and_get_altitude(self) -> None:
        """高度の追加と取得"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        buffer.add_adsb_position(
            icao="84C27A",
            callsign="JAL123",
            timestamp=ts,
            altitude_m=10000.0,
            lat=35.5,
            lon=139.5,
        )

        result = buffer.get_altitude_at("84C27A", ts)
        assert result is not None
        alt, lat, lon, source = result
        assert alt == 10000.0
        assert lat == 35.5
        assert lon == 139.5
        assert source == "adsb"  # 完全一致

    def test_get_altitude_by_callsign(self) -> None:
        """コールサインで高度を取得"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        buffer.add_adsb_position(
            icao="84C27A",
            callsign="JAL123",
            timestamp=ts,
            altitude_m=10000.0,
        )

        # コールサインで取得
        result = buffer.get_altitude_at("JAL123", ts)
        assert result is not None
        alt, _, _, _ = result
        assert alt == 10000.0

    def test_get_altitude_interpolated(self) -> None:
        """補間された高度の取得"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        ts2 = datetime(2025, 1, 1, 12, 0, 30, tzinfo=UTC)  # 30秒後

        buffer.add_adsb_position(
            icao="84C27A",
            callsign=None,
            timestamp=ts1,
            altitude_m=10000.0,
        )

        # 30秒後の時刻で取得（補間扱い）
        result = buffer.get_altitude_at("84C27A", ts2)
        assert result is not None
        alt, _, _, source = result
        assert alt == 10000.0
        assert source == "interpolated"

    def test_get_altitude_outside_window(self) -> None:
        """ウィンドウ外のデータは取得できない"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        ts2 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)  # 2分後

        buffer.add_adsb_position(
            icao="84C27A",
            callsign=None,
            timestamp=ts1,
            altitude_m=10000.0,
        )

        # 2分後の時刻で取得（ウィンドウ外）
        result = buffer.get_altitude_at("84C27A", ts2)
        assert result is None

    def test_prefer_closer_time(self) -> None:
        """時刻的に近いデータを優先"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts_target = datetime(2025, 1, 1, 12, 0, 30, tzinfo=UTC)
        ts_early = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)  # 30秒前
        ts_late = datetime(2025, 1, 1, 12, 0, 50, tzinfo=UTC)  # 20秒後

        buffer.add_adsb_position(icao="84C27A", callsign=None, timestamp=ts_early, altitude_m=9000.0)
        buffer.add_adsb_position(icao="84C27A", callsign=None, timestamp=ts_late, altitude_m=11000.0)

        # 20秒後のデータが近い
        result = buffer.get_altitude_at("84C27A", ts_target)
        assert result is not None
        alt, _, _, _ = result
        assert alt == 11000.0

    def test_prefer_later_when_same_distance(self) -> None:
        """同距離の場合は後のデータを優先"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts_target = datetime(2025, 1, 1, 12, 0, 30, tzinfo=UTC)
        ts_early = datetime(2025, 1, 1, 12, 0, 10, tzinfo=UTC)  # 20秒前
        ts_late = datetime(2025, 1, 1, 12, 0, 50, tzinfo=UTC)  # 20秒後

        buffer.add_adsb_position(icao="84C27A", callsign=None, timestamp=ts_early, altitude_m=9000.0)
        buffer.add_adsb_position(icao="84C27A", callsign=None, timestamp=ts_late, altitude_m=11000.0)

        # 同距離（20秒）なので後のデータを優先
        result = buffer.get_altitude_at("84C27A", ts_target)
        assert result is not None
        alt, _, _, _ = result
        assert alt == 11000.0

    def test_resolve_icao(self) -> None:
        """コールサインから ICAO を解決"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        buffer.add_adsb_position(
            icao="84C27A",
            callsign="JAL123",
            timestamp=ts,
            altitude_m=10000.0,
        )

        assert buffer.resolve_icao("JAL123") == "84C27A"
        assert buffer.resolve_icao("jal123") == "84C27A"  # 大文字小文字を無視
        assert buffer.resolve_icao("UNKNOWN") is None

    def test_update_time_cleanup(self) -> None:
        """古いデータの自動削除"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts_old = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        ts_new = datetime(2025, 1, 1, 12, 5, 0, tzinfo=UTC)  # 5分後

        buffer.add_adsb_position(icao="84C27A", callsign=None, timestamp=ts_old, altitude_m=10000.0)

        # 5分後に update_time を呼ぶと古いデータが削除される
        buffer.update_time(ts_new)

        result = buffer.get_altitude_at("84C27A", ts_new)
        assert result is None

    def test_get_stats(self) -> None:
        """統計情報の取得"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        buffer.add_adsb_position(icao="84C27A", callsign="JAL123", timestamp=ts, altitude_m=10000.0)
        buffer.add_adsb_position(
            icao="84C27A",
            callsign="JAL123",
            timestamp=ts + timedelta(seconds=10),
            altitude_m=10100.0,
        )

        stats = buffer.get_stats()
        assert stats["aircraft_count"] == 1
        assert stats["total_entries"] == 2
        assert stats["callsign_mappings"] == 1
        assert stats["message_counter"] == 2

    def test_clear(self) -> None:
        """バッファのクリア"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        buffer.add_adsb_position(icao="84C27A", callsign="JAL123", timestamp=ts, altitude_m=10000.0)
        buffer.clear()

        assert buffer.get_altitude_at("84C27A", ts) is None
        assert buffer.resolve_icao("JAL123") is None
        stats = buffer.get_stats()
        assert stats["aircraft_count"] == 0

    def test_auto_cleanup_on_add(self) -> None:
        """auto_cleanup 有効時は位置追加だけで古いエントリが破棄される"""
        buffer = IntegratedBuffer(window_seconds=60.0, auto_cleanup=True)
        ts_old = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        # 2*window (120秒) + スロットル間隔を超えた時刻
        ts_new = ts_old + timedelta(seconds=180)

        buffer.add_adsb_position(icao="84C27A", callsign="JAL123", timestamp=ts_old, altitude_m=10000.0)
        buffer.add_adsb_position(icao="ABC123", callsign="ANA456", timestamp=ts_new, altitude_m=9000.0)

        stats = buffer.get_stats()
        assert stats["aircraft_count"] == 1  # 古い機体は破棄済み
        assert stats["total_entries"] == 1
        # 高度履歴が消えた機体のコールサインマッピングも破棄される
        assert buffer.resolve_icao("JAL123") is None
        assert buffer.resolve_icao("ANA456") == "ABC123"

    def test_no_auto_cleanup_by_default(self) -> None:
        """デフォルト（ファイル解析用）では追加時に破棄されない"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts_old = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
        ts_new = ts_old + timedelta(seconds=180)

        buffer.add_adsb_position(icao="84C27A", callsign="JAL123", timestamp=ts_old, altitude_m=10000.0)
        buffer.add_adsb_position(icao="ABC123", callsign="ANA456", timestamp=ts_new, altitude_m=9000.0)

        stats = buffer.get_stats()
        assert stats["aircraft_count"] == 2
        assert stats["total_entries"] == 2
        assert buffer.resolve_icao("JAL123") == "84C27A"

    def test_auto_cleanup_is_throttled(self) -> None:
        """スロットル間隔内の追加ではクリーンアップが走らない"""
        # 2*window (4秒) < スロットル間隔 (10秒) となるようにウィンドウを小さくする
        buffer = IntegratedBuffer(window_seconds=2.0, auto_cleanup=True)
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        # 最初の追加でクリーンアップ基準時刻が設定される
        buffer.add_adsb_position(icao="84C27A", callsign=None, timestamp=ts, altitude_m=10000.0)
        # ウィンドウ外だがスロットル間隔（10秒）未満の追加ではクリーンアップされない
        buffer.add_adsb_position(
            icao="ABC123", callsign=None, timestamp=ts + timedelta(seconds=5), altitude_m=9000.0
        )

        assert buffer.get_stats()["aircraft_count"] == 2

    def test_get_altitude_by_order(self) -> None:
        """メッセージ順序ベースでの高度取得"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        # 複数のメッセージを追加
        for i in range(10):
            buffer.add_adsb_position(
                icao="84C27A",
                callsign=None,
                timestamp=ts + timedelta(seconds=i),
                altitude_m=10000.0 + i * 100,
            )

        # インデックス 5 に近いデータを取得
        result = buffer.get_altitude_by_order("84C27A", message_index=5)
        assert result is not None
        alt, _, _, _ = result
        # インデックス 5 のデータ（メッセージ番号 5）の高度
        assert alt == 10400.0  # 10000 + 4 * 100（0-indexed）

    def test_empty_icao_ignored(self) -> None:
        """空の ICAO は無視される"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        buffer.add_adsb_position(icao="", callsign="JAL123", timestamp=ts, altitude_m=10000.0)

        stats = buffer.get_stats()
        assert stats["aircraft_count"] == 0


class TestRealtimeAggregator:
    """RealtimeAggregator のテスト"""

    def test_process_modes_position(self) -> None:
        """Mode-S 位置情報の処理"""
        aggregator = RealtimeAggregator()
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        aggregator.process_modes_position(
            icao="84C27A",
            callsign="JAL123",
            timestamp=ts,
            altitude_m=10000.0,
            lat=35.5,
            lon=139.5,
        )

        # バッファに追加されている
        result = aggregator.buffer.get_altitude_at("84C27A", ts)
        assert result is not None

    def test_process_modes_weather(self) -> None:
        """Mode-S 気象データの処理"""
        aggregator = RealtimeAggregator(ref_lat=35.0, ref_lon=139.0)
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        wind = WindData(x=-5.0, y=-8.66, angle=210.0, speed=10.0)

        obs = aggregator.process_modes_weather(
            icao="84C27A",
            callsign="JAL123",
            timestamp=ts,
            altitude_m=10000.0,
            lat=35.5,
            lon=139.5,
            temperature_c=-20.5,
            wind=wind,
            data_source="bds50_60",
        )

        assert obs is not None
        assert obs.icao == "84C27A"
        assert obs.callsign == "JAL123"
        assert obs.altitude == 10000.0
        assert obs.temperature == -20.5
        assert obs.wind == wind
        assert obs.method == "mode-s"
        assert obs.data_source == "bds50_60"
        assert obs.altitude_source == "adsb"

        # キューにも追加されている
        assert aggregator.output_queue.qsize() == 1

    def test_process_modes_weather_no_weather_data(self) -> None:
        """気象データがない場合は None"""
        aggregator = RealtimeAggregator()
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        obs = aggregator.process_modes_weather(
            icao="84C27A",
            callsign="JAL123",
            timestamp=ts,
            altitude_m=10000.0,
            temperature_c=None,
            wind=None,
        )

        assert obs is None

    def test_process_vdl2_weather_with_altitude(self) -> None:
        """高度付き VDL2 気象データの処理"""
        aggregator = RealtimeAggregator(ref_lat=35.0, ref_lon=139.0)
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        obs = aggregator.process_vdl2_weather(
            icao="84C27A",
            callsign="JAL123",
            timestamp=ts,
            altitude_m=10000.0,
            temperature_c=-20.5,
            data_source="acars_wn",
        )

        assert obs is not None
        assert obs.altitude == 10000.0
        assert obs.method == "vdl2"
        assert obs.altitude_source == "acars"

    def test_process_vdl2_weather_altitude_補完(self) -> None:
        """VDL2 の高度補完"""
        aggregator = RealtimeAggregator()
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        # まず ADS-B で位置情報を追加
        aggregator.process_modes_position(
            icao="84C27A",
            callsign="JAL123",
            timestamp=ts,
            altitude_m=10000.0,
            lat=35.5,
            lon=139.5,
        )

        # 高度なしの VDL2 データを処理
        obs = aggregator.process_vdl2_weather(
            icao="84C27A",
            callsign=None,
            timestamp=ts + timedelta(seconds=10),
            altitude_m=None,  # 高度なし
            temperature_c=-20.5,
        )

        assert obs is not None
        assert obs.altitude == 10000.0  # ADS-B から補完
        assert obs.altitude_source == "interpolated"  # 10秒の時刻差があるため

    def test_process_vdl2_weather_補完_by_callsign(self) -> None:
        """コールサインによる VDL2 高度補完"""
        aggregator = RealtimeAggregator()
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        # ADS-B で ICAO とコールサインのマッピングを登録
        aggregator.process_modes_position(
            icao="84C27A",
            callsign="JAL123",
            timestamp=ts,
            altitude_m=10000.0,
        )

        # ICAO なし、コールサインのみの VDL2
        obs = aggregator.process_vdl2_weather(
            icao=None,
            callsign="JAL123",
            timestamp=ts + timedelta(seconds=10),
            altitude_m=None,
            temperature_c=-20.5,
        )

        assert obs is not None
        assert obs.altitude == 10000.0

    def test_process_vdl2_weather_no_altitude_available(self) -> None:
        """高度補完できない場合は None"""
        aggregator = RealtimeAggregator()
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        # 高度なし、補完もできない
        obs = aggregator.process_vdl2_weather(
            icao="UNKNOWN",
            callsign=None,
            timestamp=ts,
            altitude_m=None,
            temperature_c=-20.5,
        )

        assert obs is None

    def test_calculate_distance(self) -> None:
        """距離計算"""
        aggregator = RealtimeAggregator(ref_lat=35.0, ref_lon=139.0)

        # 同じ点
        d = aggregator._calculate_distance(35.0, 139.0)
        assert d < 0.1

        # 約 111km（緯度1度）
        d = aggregator._calculate_distance(36.0, 139.0)
        assert 110 < d < 112

    def test_get_stats(self) -> None:
        """統計情報の取得"""
        aggregator = RealtimeAggregator()
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        aggregator.process_modes_weather(
            icao="84C27A",
            callsign="JAL123",
            timestamp=ts,
            altitude_m=10000.0,
            temperature_c=-20.5,
        )

        stats = aggregator.get_stats()
        assert stats.get("aircraft_count") == 1
        assert stats.get("output_queue_size") == 1

    def test_clear(self) -> None:
        """内部状態のクリア"""
        aggregator = RealtimeAggregator()
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        aggregator.process_modes_position(
            icao="84C27A",
            callsign="JAL123",
            timestamp=ts,
            altitude_m=10000.0,
        )

        aggregator.clear()

        result = aggregator.buffer.get_altitude_at("84C27A", ts)
        assert result is None


class TestIntegration:
    """統合テスト"""

    def test_realistic_scenario(self) -> None:
        """実際的なシナリオ"""
        aggregator = RealtimeAggregator(ref_lat=35.682677, ref_lon=139.762230)
        base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        # シーケンス: ADS-B 位置 → ADS-B 気象 → VDL2 気象（高度なし）
        results = []

        # 1. ADS-B 位置のみ（気象データなし）
        aggregator.process_modes_position(
            icao="84C27A",
            callsign="JAL123",
            timestamp=base_time,
            altitude_m=10000.0,
            lat=35.7,
            lon=139.8,
        )

        # 2. ADS-B 気象データ
        obs1 = aggregator.process_modes_weather(
            icao="84C27A",
            callsign="JAL123",
            timestamp=base_time + timedelta(seconds=5),
            altitude_m=10050.0,
            lat=35.71,
            lon=139.81,
            temperature_c=-25.0,
            wind=WindData(x=-5.0, y=-8.66, angle=210.0, speed=10.0),
        )
        if obs1:
            results.append(obs1)

        # 3. VDL2 気象データ（高度なし - ADS-B から補完）
        obs2 = aggregator.process_vdl2_weather(
            icao="84C27A",
            callsign=None,
            timestamp=base_time + timedelta(seconds=20),
            altitude_m=None,  # 高度なし
            temperature_c=-24.5,
            data_source="acars_wn",
        )
        if obs2:
            results.append(obs2)

        # 検証
        assert len(results) == 2

        # ADS-B 結果
        assert results[0].method == "mode-s"
        assert results[0].temperature == -25.0

        # VDL2 結果（高度補完済み）
        assert results[1].method == "vdl2"
        assert results[1].temperature == -24.5
        assert results[1].altitude == 10050.0  # ADS-B から補完
        assert results[1].altitude_source in ["adsb", "interpolated"]


class TestFileAggregator:
    """FileAggregator のテスト"""

    def test_init(self) -> None:
        """初期化"""
        agg = FileAggregator(ref_lat=35.0, ref_lon=139.0, max_index_distance=500)
        assert agg._ref_lat == 35.0
        assert agg._ref_lon == 139.0
        assert agg._max_index_distance == 500

    def test_calc_temperature(self) -> None:
        """気温計算"""
        agg = FileAggregator()
        # TAS=250m/s, Mach=0.8 の場合
        # 音速 = 250/0.8 = 312.5 m/s
        # T = 312.5^2 / (1.4 * 287) = 243.1 K = -29.9 ℃
        temp = agg._calc_temperature(250.0, 0.8)
        assert temp == pytest.approx(-30.0, abs=1.0)

    def test_calc_temperature_zero_mach(self) -> None:
        """マッハ数ゼロ"""
        agg = FileAggregator()
        temp = agg._calc_temperature(250.0, 0.0)
        assert temp == -999.0

    def test_calculate_distance(self) -> None:
        """距離計算"""
        agg = FileAggregator(ref_lat=35.0, ref_lon=139.0)
        # 約1度北
        dist = agg._calculate_distance(36.0, 139.0)
        assert dist == pytest.approx(111.0, rel=0.1)

    def test_get_stats(self) -> None:
        """統計情報"""
        agg = FileAggregator()
        stats = agg.get_stats()
        assert "aircraft_count" in stats
        assert "results_count" in stats


class TestMagneticDeclination:
    """磁気偏角計算のテスト（リアルタイム/ファイル解析経路の一貫性）"""

    def test_gsi_2020_reference_value(self) -> None:
        """基準点 (37N, 138E) で国土地理院 2020 年値（約 +8.26°、西偏正）を返す"""
        declination = amdar.core.geo.calc_magnetic_declination(37.0, 138.0)
        assert declination == pytest.approx(8 + 15.822 / 60, abs=1e-6)

    def test_wind_consistency_between_paths(self) -> None:
        """receiver と FileAggregator の風計算が同一結果を返す

        磁気偏角の実装が2系統に分かれて食い違っていた問題（風向が約16°
        ずれる）の回帰テスト。
        """
        lat, lon = 35.5, 137.0
        trackangle, groundspeed_ms, heading, trueair_ms = 270.0, 220.0, 265.0, 230.0

        wind_receiver = amdar.sources.modes.receiver._calc_wind(
            lat, lon, trackangle, groundspeed_ms, heading, trueair_ms
        )
        wind_file = FileAggregator()._calc_wind(lat, lon, trackangle, groundspeed_ms, heading, trueair_ms)

        assert wind_file.x == pytest.approx(wind_receiver.x)
        assert wind_file.y == pytest.approx(wind_receiver.y)
        assert wind_file.angle == pytest.approx(wind_receiver.angle)
        assert wind_file.speed == pytest.approx(wind_receiver.speed)


class TestParseFromFiles:
    """parse_from_files() のテスト"""

    def test_no_files(self) -> None:
        """ファイルなし"""
        results = parse_from_files(modes_file=None, vdl2_file=None)
        assert results == []

    def test_nonexistent_file(self) -> None:
        """存在しないファイル"""
        import pathlib

        results = parse_from_files(
            modes_file=pathlib.Path("/nonexistent/file.dat"),
            vdl2_file=None,
        )
        assert results == []

    def test_parse_vdl2_fixture(self) -> None:
        """VDL2 フィクスチャファイルの解析"""
        import pathlib

        vdl2_file = pathlib.Path("tests/fixtures/vdl2.dat")
        if not vdl2_file.exists():
            pytest.skip("VDL2 fixture not found")

        results = parse_from_files(vdl2_file=vdl2_file)
        # 結果の基本検証
        assert isinstance(results, list)
        for obs in results:
            assert obs.is_valid()
            assert obs.method == "vdl2"

    def test_parse_modes_fixture(self) -> None:
        """Mode-S フィクスチャファイルの解析"""
        import pathlib

        modes_file = pathlib.Path("tests/fixtures/ads-b.dat")
        if not modes_file.exists():
            pytest.skip("Mode-S fixture not found")

        results = parse_from_files(modes_file=modes_file)
        # 結果の基本検証
        assert isinstance(results, list)
        for obs in results:
            assert obs.is_valid()
            assert obs.method == "mode-s"

    def test_parse_both_fixtures(self) -> None:
        """両方のフィクスチャファイルの解析"""
        import pathlib

        modes_file = pathlib.Path("tests/fixtures/ads-b.dat")
        vdl2_file = pathlib.Path("tests/fixtures/vdl2.dat")

        if not modes_file.exists() or not vdl2_file.exists():
            pytest.skip("Fixtures not found")

        results = parse_from_files(modes_file=modes_file, vdl2_file=vdl2_file)
        # 結果の基本検証
        assert isinstance(results, list)
        for obs in results:
            assert obs.is_valid()
            assert obs.method in ["mode-s", "vdl2"]
