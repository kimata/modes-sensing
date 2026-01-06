#!/usr/bin/env python3
# ruff: noqa: S101, S110, SIM105
"""
収集した ADS-B データを使った receiver モジュールのテスト

tests/fixtures/ads-b.dat が存在する場合のみ実行されます。
データがない場合はスキップされます。
"""

from __future__ import annotations

import pathlib
import queue
from typing import TYPE_CHECKING

import pytest

from amdar.config import Config, load_from_dict
from amdar.sources.modes.receiver import (
    _fragment_list,
    _process_message,
)

if TYPE_CHECKING:
    from amdar.database.postgresql import MeasurementData

# テストデータのパス
FIXTURE_PATH = pathlib.Path(__file__).parent.parent / "fixtures" / "ads-b.dat"


@pytest.fixture
def config(config_dict: dict) -> Config:
    """Config オブジェクトを生成"""
    return load_from_dict(config_dict, pathlib.Path.cwd())


@pytest.fixture
def data_queue() -> queue.Queue:
    """データキューを生成"""
    return queue.Queue()


@pytest.fixture
def adsb_messages() -> list[str]:
    """ADS-B メッセージを読み込む"""
    if not FIXTURE_PATH.exists():
        pytest.skip(f"Fixture file not found: {FIXTURE_PATH}")

    with FIXTURE_PATH.open() as f:
        messages = [line.strip() for line in f if line.strip()]

    if not messages:
        pytest.skip("Fixture file is empty")

    return messages


class TestReceiverWithFixture:
    """収集データを使った receiver のテスト"""

    def test_process_messages_no_crash(
        self, adsb_messages: list[str], data_queue: queue.Queue, config: Config
    ) -> None:
        """全メッセージを処理してクラッシュしないことを確認"""
        # フラグメントリストをクリア
        _fragment_list.clear()

        processed_count = 0
        error_count = 0

        for message in adsb_messages:
            try:
                _process_message(message, data_queue, config.filter.area)
                processed_count += 1
            except Exception as e:
                error_count += 1
                # 最初の10個のエラーのみ表示
                if error_count <= 10:
                    print(f"Error processing message: {message[:50]}... - {e}")

        print(f"\nProcessed: {processed_count}, Errors: {error_count}")
        print(f"Data queue size: {data_queue.qsize()}")

        # エラー率が1%未満であることを確認
        error_rate = error_count / len(adsb_messages) if adsb_messages else 0
        assert error_rate < 0.01, f"Error rate too high: {error_rate:.2%}"

    def test_meteorological_data_extraction(
        self, adsb_messages: list[str], data_queue: queue.Queue, config: Config
    ) -> None:
        """気象データが抽出できることを確認"""
        # フラグメントリストをクリア
        _fragment_list.clear()

        # 全メッセージを処理
        for message in adsb_messages:
            try:
                _process_message(message, data_queue, config.filter.area)
            except Exception:
                pass

        # キューからデータを取り出して検証
        data_count = data_queue.qsize()
        print(f"\nExtracted {data_count} meteorological data points")

        if data_count == 0:
            pytest.skip("No meteorological data extracted (may need more messages)")

        # 抽出されたデータの品質をチェック
        valid_count = 0
        samples: list[MeasurementData] = []

        while not data_queue.empty():
            data: MeasurementData = data_queue.get()
            samples.append(data)

            # 基本的なデータ検証
            if (
                data.altitude is not None
                and data.temperature is not None
                and -100 < data.temperature < 50  # 妥当な温度範囲
                and 0 < data.altitude < 20000  # 妥当な高度範囲 (m)
            ):
                valid_count += 1

        valid_rate = valid_count / data_count if data_count > 0 else 0
        print(f"Valid data points: {valid_count} ({valid_rate:.1%})")

        # サンプルデータを表示
        if samples:
            print("\nSample data (first 5):")
            for i, data in enumerate(samples[:5]):
                print(
                    f"  {i + 1}. {data.callsign}: "
                    f"alt={data.altitude:.0f}m, "
                    f"temp={data.temperature:.1f}C, "
                    f"wind={data.wind.speed:.1f}m/s @ {data.wind.angle:.0f}deg"
                )

        # 有効データが50%以上あることを確認
        assert valid_rate >= 0.5, f"Valid data rate too low: {valid_rate:.1%}"

    def test_message_type_distribution(
        self, adsb_messages: list[str], data_queue: queue.Queue, config: Config
    ) -> None:
        """メッセージタイプの分布を確認"""
        import pyModeS

        df_counts: dict[int, int] = {}
        tc_counts: dict[int, int] = {}
        bds_counts: dict[str, int] = {"BDS44": 0, "BDS50": 0, "BDS60": 0, "other": 0}

        for raw_message in adsb_messages:
            if len(raw_message) < 2:
                continue

            message = raw_message[1:-1]  # 先頭・末尾を除去

            if len(message) < 22:
                continue

            try:
                df = pyModeS.df(message)
                df_counts[df] = df_counts.get(df, 0) + 1

                if df in (17, 18):
                    tc = pyModeS.typecode(message)
                    if tc is not None:
                        tc_counts[tc] = tc_counts.get(tc, 0) + 1

                elif df in (20, 21):
                    if pyModeS.bds.bds44.is44(message):
                        bds_counts["BDS44"] += 1
                    elif pyModeS.bds.bds50.is50(message):
                        bds_counts["BDS50"] += 1
                    elif pyModeS.bds.bds60.is60(message):
                        bds_counts["BDS60"] += 1
                    else:
                        bds_counts["other"] += 1

            except Exception:
                pass

        print("\n=== Message Type Distribution ===")
        print("\nDownlink Format (DF):")
        for df, count in sorted(df_counts.items()):
            df_names = {
                0: "Short ACAS",
                4: "Surveillance alt",
                5: "Surveillance id",
                11: "All-call",
                16: "Long ACAS",
                17: "ADS-B",
                18: "TIS-B/ADS-R",
                20: "Comm-B alt",
                21: "Comm-B id",
            }
            name = df_names.get(df, "Unknown")
            print(f"  DF={df} ({name}): {count}")

        print("\nTypecode (for DF=17/18):")
        for tc, count in sorted(tc_counts.items()):
            tc_names = {
                1: "ID (cat D)",
                2: "ID (cat C)",
                3: "ID (cat B)",
                4: "ID (cat A)",
                5: "Surface pos",
                9: "Airborne pos",
                19: "Velocity",
            }
            name = tc_names.get(tc, "Position/Other")
            print(f"  TC={tc} ({name}): {count}")

        print("\nBDS types (for DF=20/21):")
        for bds, count in bds_counts.items():
            print(f"  {bds}: {count}")

        # 最低限の DF=17 または DF=18 があることを確認
        adsb_count = df_counts.get(17, 0) + df_counts.get(18, 0)
        assert adsb_count > 0, "No ADS-B messages found"


class TestMessageParsing:
    """メッセージパース処理のテスト"""

    def test_message_format_validation(self, adsb_messages: list[str]) -> None:
        """メッセージフォーマットの検証"""
        valid_format = 0
        invalid_format = 0

        for message in adsb_messages:
            # dump1090 フォーマット: @....; または *....;
            if len(message) >= 2 and message[0] in ("@", "*") and message[-1] == ";":
                valid_format += 1
            else:
                invalid_format += 1

        print(f"\nValid format: {valid_format}")
        print(f"Invalid format: {invalid_format}")

        valid_rate = valid_format / len(adsb_messages) if adsb_messages else 0
        assert valid_rate >= 0.99, f"Too many invalid format messages: {1 - valid_rate:.2%}"
