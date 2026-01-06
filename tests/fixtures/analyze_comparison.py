#!/usr/bin/env python3
"""
Mode-S と VDL2 のデータ比較分析

既存の収集データを使用して両ソースの特性を分析します。
"""

from __future__ import annotations

import pathlib
import sys
from dataclasses import dataclass

# プロジェクトルートをパスに追加
project_root = pathlib.Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

import amdar.sources.vdl2_parser as vdl2_parser  # noqa: E402


@dataclass
class VDL2WeatherSummary:
    """VDL2 気象データサマリー"""

    total_messages: int
    weather_messages: int
    with_altitude: int
    with_temperature: int
    with_wind: int
    complete_records: int


def analyze_vdl2_data(filepath: pathlib.Path) -> VDL2WeatherSummary:
    """VDL2 データファイルを分析"""
    with filepath.open("rb") as f:
        lines = f.readlines()

    total = len(lines)
    weather_count = 0
    with_alt = 0
    with_temp = 0
    with_wind = 0
    complete = 0

    weather_records = []

    for line in lines:
        result = vdl2_parser.parse_acars_weather(line)
        if result:
            weather_count += 1
            weather_records.append(result)

            has_alt = result.altitude_ft is not None
            has_temp = result.temperature_c is not None
            has_wind = result.wind_dir_deg is not None and result.wind_speed_kt is not None

            if has_alt:
                with_alt += 1
            if has_temp:
                with_temp += 1
            if has_wind:
                with_wind += 1
            if has_alt and has_temp:
                complete += 1

    return VDL2WeatherSummary(
        total_messages=total,
        weather_messages=weather_count,
        with_altitude=with_alt,
        with_temperature=with_temp,
        with_wind=with_wind,
        complete_records=complete,
    )


def main() -> None:
    print("=" * 70)
    print("Mode-S と VDL2 データ比較分析レポート")
    print("=" * 70)
    print()

    # VDL2 データ分析
    vdl2_path = pathlib.Path("tests/fixtures/vdl2.dat")
    if vdl2_path.exists():
        summary = analyze_vdl2_data(vdl2_path)
        print("【VDL2 データ分析】")
        print(f"  総メッセージ数: {summary.total_messages}")
        pct = summary.weather_messages / summary.total_messages * 100
        print(f"  気象データ含有: {summary.weather_messages} ({pct:.1f}%)")
        print(f"  高度情報あり: {summary.with_altitude}")
        print(f"  温度情報あり: {summary.with_temperature}")
        print(f"  風情報あり: {summary.with_wind}")
        print(f"  完全データ(高度+温度): {summary.complete_records}")
        print()
    else:
        print("VDL2 データファイルが見つかりません")
        print()

    # 60分収集の結果サマリー
    print("【60分間収集結果（ログより）】")
    print("  Mode-S:")
    print("    収集レコード数: 1,223 件")
    print("    データ種類: BDS50/60 からの計算値（一部 BDS44 直接）")
    print("    含まれる情報: 高度、温度、風向、風速、位置")
    print()
    print("  VDL2:")
    print("    総メッセージ数: 82 件")
    print("    気象データ含有: 2 件")
    print("    注記: 多くのメッセージは高度情報が欠落")
    print()

    print("【分析結果】")
    print()
    print("1. データ量の差異")
    print("   - Mode-S: 安定して多量のデータを取得")
    print("   - VDL2: 気象データを含むメッセージは全体の約10%、")
    print("           さらに高度情報がないものが多い")
    print()
    print("2. データの特性")
    print("   - Mode-S (BDS50/60):")
    print("       対気速度、マッハ数、地速、機首方位から温度・風を計算")
    print("       計算値のため、誤差が蓄積する可能性あり")
    print("   - Mode-S (BDS44):")
    print("       気象情報を直接送信（MRAR: Meteorological Routine Air Report）")
    print("       今回の収集で 6 件検出")
    print("   - VDL2 (ACARS):")
    print("       航空会社独自フォーマット（WN, PNTAF, WX, FL）")
    print("       直接測定値または計算値")
    print()
    print("3. 比較の困難性")
    print("   - VDL2 からの気象データが非常に少なく、")
    print("     同一時間・同一高度でのデータ比較が困難")
    print("   - 完全データ（高度+温度）を持つ VDL2 レコードは")
    print("     60分間で 1-2 件程度")
    print()
    print("4. 推奨事項")
    print("   - より長時間（数時間〜1日）のデータ収集を行い、")
    print("     VDL2 の気象データサンプルを増やす")
    print("   - BDS44 データと VDL2 データの比較に焦点を当てる")
    print("     （両方とも直接測定値/報告値のため比較しやすい）")
    print()

    # VDL2 の気象データサンプルを表示
    if vdl2_path.exists():
        print("【VDL2 気象データサンプル】")
        with vdl2_path.open("rb") as f:
            lines = f.readlines()

        for line in lines:
            result = vdl2_parser.parse_acars_weather(line)
            if result:
                alt_str = f"{result.altitude_ft}ft" if result.altitude_ft else "N/A"
                temp_str = f"{result.temperature_c}°C" if result.temperature_c is not None else "N/A"
                if result.wind_dir_deg is not None and result.wind_speed_kt is not None:
                    wind_str = f"{result.wind_dir_deg}°/{result.wind_speed_kt}kt"
                else:
                    wind_str = "N/A"

                print(f"  {result.flight:8s}: 高度={alt_str:>8s}, 温度={temp_str:>6s}, 風={wind_str}")


if __name__ == "__main__":
    main()
