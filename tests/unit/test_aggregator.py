#!/usr/bin/env python3
# ruff: noqa: S101
"""aggregator.py のユニットテスト

IntegratedBuffer と FileAggregator のテストを行います。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

import amdar.core.geo
from amdar.sources.aggregator import (
    AltitudeEntry,
    FileAggregator,
    IntegratedBuffer,
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


class TestFileAggregator:
    """FileAggregator のテスト"""

    def test_init(self) -> None:
        """初期化"""
        agg = FileAggregator(ref_lat=35.0, ref_lon=139.0, max_index_distance=500)
        assert agg._ref_lat == 35.0
        assert agg._ref_lon == 139.0
        assert agg._max_index_distance == 500

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


class TestMessageIndexNumbering:
    """順序ベース高度補完の採番統一テスト

    FileAggregator の行カウンタと IntegratedBuffer の内部カウンタが
    別採番で get_altitude_by_order がほぼ常に失敗していた問題の回帰テスト。
    """

    def test_explicit_message_index_is_used(self) -> None:
        """add_adsb_position に明示した message_index がエントリに使われる"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        buffer.add_adsb_position(
            icao="84C27A",
            callsign=None,
            timestamp=ts,
            altitude_m=10000.0,
            message_index=5000,
        )

        # 明示したインデックス近傍でのみヒットする
        assert buffer.get_altitude_by_order("84C27A", message_index=5001, max_distance=10) is not None
        assert buffer.get_altitude_by_order("84C27A", message_index=1, max_distance=10) is None

    def test_default_uses_internal_counter(self) -> None:
        """message_index を省略した場合は従来通り内部カウンタを使用"""
        buffer = IntegratedBuffer(window_seconds=60.0)
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        buffer.add_adsb_position(icao="84C27A", callsign=None, timestamp=ts, altitude_m=10000.0)
        buffer.add_adsb_position(icao="84C27A", callsign=None, timestamp=ts, altitude_m=11000.0)

        result = buffer.get_altitude_by_order("84C27A", message_index=2, max_distance=0)
        assert result is not None
        assert result.altitude_m == 11000.0

    def test_file_aggregator_shares_numbering_space(self) -> None:
        """FileAggregator の行カウンタとバッファの採番が同一番号空間になる

        修正前はバッファ側が add_adsb_position の呼び出し回数で採番して
        いたため、行番号ベースの msg_index と乖離していた。
        """
        import pathlib

        modes_file = pathlib.Path("tests/fixtures/ads-b.dat")
        if not modes_file.exists():
            pytest.skip("Mode-S fixture not found")

        agg = FileAggregator()
        agg.parse_modes_file(modes_file)

        indices = [
            entry.message_index for entries in agg._buffer._altitude_by_icao.values() for entry in entries
        ]
        if not indices:
            pytest.skip("No ADS-B positions registered from fixture")

        # 全エントリのインデックスが行カウンタの範囲内（同一番号空間）
        assert max(indices) <= agg._message_index
        # 行番号採番なら位置メッセージ以外の行も数えるため、
        # インデックス最大値は登録件数（旧: 呼び出し回数採番の最大値）より大きくなる
        assert max(indices) > len(indices)


class TestMagneticDeclination:
    """磁気偏角計算のテスト"""

    def test_gsi_2020_reference_value(self) -> None:
        """基準点 (37N, 138E) で国土地理院 2020 年値（約 +8.26°、西偏正）を返す"""
        declination = amdar.core.geo.calc_magnetic_declination(37.0, 138.0)
        assert declination == pytest.approx(8 + 15.822 / 60, abs=1e-6)


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
