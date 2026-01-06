#!/usr/bin/env python3
"""
Mode-S と VDL2 の気象データ収集・比較スクリプト

両方のソースからデータを収集し、高度が近いのに気象データが
大きく異なるケースを検出します。

Usage:
    python collect_weather_comparison.py [-c CONFIG] [-d DURATION]

Options:
    -c CONFIG    : 設定ファイルパス [default: config.yaml]
    -d DURATION  : 収集時間（分） [default: 60]

"""

from __future__ import annotations

import argparse
import csv
import json
import pathlib
import queue
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime

# プロジェクトルートをパスに追加
project_root = pathlib.Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

import my_lib.config  # noqa: E402

import amdar.config  # noqa: E402
import amdar.sources.modes.receiver as modes_receiver  # noqa: E402
import amdar.sources.vdl2.parser as vdl2_parser  # noqa: E402
import amdar.sources.vdl2.receiver as vdl2_receiver  # noqa: E402


@dataclass
class WeatherRecord:
    """気象データレコード"""

    timestamp: datetime
    source: str  # "modes" or "vdl2"
    callsign: str
    altitude_m: float
    temperature_c: float
    wind_speed_ms: float
    wind_direction_deg: float
    latitude: float | None
    longitude: float | None


def collect_modes_data(
    config: amdar.config.Config,
    duration_seconds: float,
    records: list[WeatherRecord],
    lock: threading.Lock,
) -> None:
    """Mode-S データを収集する"""
    data_queue: queue.Queue = queue.Queue()

    # receiver を開始
    modes_receiver.start(config, data_queue)

    start_time = time.time()
    count = 0

    try:
        while time.time() - start_time < duration_seconds:
            try:
                data = data_queue.get(timeout=5)
                count += 1

                record = WeatherRecord(
                    timestamp=datetime.now(UTC),
                    source="modes",
                    callsign=data.callsign,
                    altitude_m=data.altitude,
                    temperature_c=data.temperature,
                    wind_speed_ms=data.wind.speed,
                    wind_direction_deg=data.wind.angle,
                    latitude=data.latitude if data.latitude != 0 else None,
                    longitude=data.longitude if data.longitude != 0 else None,
                )

                with lock:
                    records.append(record)

                if count % 100 == 0:
                    elapsed = time.time() - start_time
                    print(f"  [Mode-S] {count} records, {elapsed / 60:.1f} min elapsed", flush=True)

            except queue.Empty:
                continue
    finally:
        modes_receiver.term()
        print(f"[Mode-S] Collection complete: {count} records", flush=True)


def collect_vdl2_data(
    host: str,
    port: int,
    ref_lat: float,
    ref_lon: float,
    duration_seconds: float,
    records: list[WeatherRecord],
    lock: threading.Lock,
) -> None:
    """VDL2 データを収集する"""
    import zmq

    ctx = zmq.Context()
    socket = ctx.socket(zmq.SUB)
    socket.connect(f"tcp://{host}:{port}")
    socket.setsockopt(zmq.SUBSCRIBE, b"")
    socket.setsockopt(zmq.RCVTIMEO, 5000)

    start_time = time.time()
    count = 0
    total_messages = 0

    try:
        while time.time() - start_time < duration_seconds:
            try:
                msg = socket.recv()
                total_messages += 1

                acars_data = vdl2_parser.parse_acars_weather(msg)
                if acars_data:
                    measurement = vdl2_parser.convert_to_measurement_data(acars_data, ref_lat, ref_lon)
                    if measurement:
                        count += 1

                        record = WeatherRecord(
                            timestamp=acars_data.timestamp or datetime.now(UTC),
                            source="vdl2",
                            callsign=measurement.callsign,
                            altitude_m=measurement.altitude,
                            temperature_c=measurement.temperature,
                            wind_speed_ms=measurement.wind.speed,
                            wind_direction_deg=measurement.wind.angle,
                            latitude=measurement.latitude if measurement.latitude != 0 else None,
                            longitude=measurement.longitude if measurement.longitude != 0 else None,
                        )

                        with lock:
                            records.append(record)

                if total_messages % 500 == 0:
                    elapsed = time.time() - start_time
                    print(
                        f"  [VDL2] {total_messages} messages, {count} weather records, "
                        f"{elapsed / 60:.1f} min elapsed",
                        flush=True,
                    )

            except zmq.Again:
                continue
    finally:
        socket.close()
        ctx.term()
        print(f"[VDL2] Collection complete: {count} records from {total_messages} messages", flush=True)


def save_records(records: list[WeatherRecord], output_path: pathlib.Path) -> None:
    """レコードを CSV に保存"""
    with output_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp",
                "source",
                "callsign",
                "altitude_m",
                "temperature_c",
                "wind_speed_ms",
                "wind_direction_deg",
                "latitude",
                "longitude",
            ]
        )
        for r in sorted(records, key=lambda x: x.timestamp):
            writer.writerow(
                [
                    r.timestamp.isoformat(),
                    r.source,
                    r.callsign,
                    f"{r.altitude_m:.1f}",
                    f"{r.temperature_c:.1f}",
                    f"{r.wind_speed_ms:.1f}",
                    f"{r.wind_direction_deg:.1f}",
                    f"{r.latitude:.4f}" if r.latitude else "",
                    f"{r.longitude:.4f}" if r.longitude else "",
                ]
            )
    print(f"Saved {len(records)} records to {output_path}")


def analyze_discrepancies(records: list[WeatherRecord]) -> list[dict]:
    """高度が近いのにデータが異なるケースを検出"""
    # 時間順にソート
    sorted_records = sorted(records, key=lambda x: x.timestamp)

    # 5分間のウィンドウでデータを比較
    window_seconds = 300
    altitude_threshold_m = 500  # 高度差の閾値
    temp_threshold_c = 10  # 温度差の閾値
    wind_speed_threshold_ms = 10  # 風速差の閾値
    wind_dir_threshold_deg = 45  # 風向差の閾値

    discrepancies = []

    for i, r1 in enumerate(sorted_records):
        if r1.source != "modes":
            continue

        for r2 in sorted_records[i + 1 :]:
            if r2.source != "vdl2":
                continue

            # 時間ウィンドウをチェック
            time_diff = abs((r2.timestamp - r1.timestamp).total_seconds())
            if time_diff > window_seconds:
                break

            # 高度差をチェック
            altitude_diff = abs(r2.altitude_m - r1.altitude_m)
            if altitude_diff > altitude_threshold_m:
                continue

            # 気象データの差をチェック
            temp_diff = abs(r2.temperature_c - r1.temperature_c)
            wind_speed_diff = abs(r2.wind_speed_ms - r1.wind_speed_ms)

            # 風向差（360度の循環を考慮）
            wind_dir_diff = abs(r2.wind_direction_deg - r1.wind_direction_deg)
            if wind_dir_diff > 180:
                wind_dir_diff = 360 - wind_dir_diff

            is_discrepancy = (
                temp_diff > temp_threshold_c
                or wind_speed_diff > wind_speed_threshold_ms
                or wind_dir_diff > wind_dir_threshold_deg
            )

            if is_discrepancy:
                discrepancies.append(
                    {
                        "modes_record": r1,
                        "vdl2_record": r2,
                        "time_diff_s": time_diff,
                        "altitude_diff_m": altitude_diff,
                        "temp_diff_c": temp_diff,
                        "wind_speed_diff_ms": wind_speed_diff,
                        "wind_dir_diff_deg": wind_dir_diff,
                    }
                )

    return discrepancies


def print_discrepancy_report(discrepancies: list[dict]) -> None:
    """乖離レポートを出力"""
    print("\n" + "=" * 80)
    print("乖離検出レポート")
    print("=" * 80)

    if not discrepancies:
        print("高度が近いのに気象データが大きく異なるケースは検出されませんでした。")
        return

    print(f"\n検出された乖離: {len(discrepancies)} 件\n")
    print("閾値:")
    print("  - 時間ウィンドウ: 5分")
    print("  - 高度差: 500m 以内")
    print("  - 温度差: 10°C 以上")
    print("  - 風速差: 10 m/s 以上")
    print("  - 風向差: 45° 以上")
    print()

    for i, d in enumerate(discrepancies[:20]):  # 最大20件表示
        r1 = d["modes_record"]
        r2 = d["vdl2_record"]
        print(f"--- 乖離 #{i + 1} ---")
        print(f"  時間差: {d['time_diff_s']:.0f}秒, 高度差: {d['altitude_diff_m']:.0f}m")
        print(f"  Mode-S: {r1.callsign}")
        print(f"    高度: {r1.altitude_m:.0f}m, 温度: {r1.temperature_c:.1f}°C")
        print(f"    風速: {r1.wind_speed_ms:.1f}m/s, 風向: {r1.wind_direction_deg:.0f}°")
        print(f"  VDL2: {r2.callsign}")
        print(f"    高度: {r2.altitude_m:.0f}m, 温度: {r2.temperature_c:.1f}°C")
        print(f"    風速: {r2.wind_speed_ms:.1f}m/s, 風向: {r2.wind_direction_deg:.0f}°")
        print("  差分:")
        print(f"    温度: {d['temp_diff_c']:.1f}°C, 風速: {d['wind_speed_diff_ms']:.1f}m/s")
        print(f"    風向: {d['wind_dir_diff_deg']:.0f}°")
        print()

    if len(discrepancies) > 20:
        print(f"... 他 {len(discrepancies) - 20} 件")


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect and compare Mode-S and VDL2 weather data")
    parser.add_argument("-c", "--config", default="config.yaml", help="Config file path")
    parser.add_argument("-d", "--duration", type=int, default=60, help="Duration in minutes")
    args = parser.parse_args()

    config_path = pathlib.Path(args.config)
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    # 設定読み込み
    config_dict = my_lib.config.load(str(config_path))
    config = amdar.config.load_from_dict(config_dict, pathlib.Path.cwd())

    # VDL2 設定
    vdl2_config = config_dict.get("vdl2", {}).get("decoder", {})
    vdl2_host = vdl2_config.get("host", "192.168.0.20")
    vdl2_port = vdl2_config.get("port", 5050)

    # 基準点
    ref_lat = config.filter.area.lat.ref
    ref_lon = config.filter.area.lon.ref

    duration_seconds = args.duration * 60

    print("=== 気象データ収集開始 ===", flush=True)
    print(f"収集時間: {args.duration} 分", flush=True)
    print(f"Mode-S: {config.decoder.modes.host}:{config.decoder.modes.port}", flush=True)
    print(f"VDL2: {vdl2_host}:{vdl2_port}", flush=True)
    print(f"基準点: ({ref_lat:.4f}, {ref_lon:.4f})", flush=True)
    print(flush=True)

    records: list[WeatherRecord] = []
    lock = threading.Lock()

    # 両方のソースからデータ収集
    modes_thread = threading.Thread(
        target=collect_modes_data,
        args=(config, duration_seconds, records, lock),
    )
    vdl2_thread = threading.Thread(
        target=collect_vdl2_data,
        args=(vdl2_host, vdl2_port, ref_lat, ref_lon, duration_seconds, records, lock),
    )

    try:
        modes_thread.start()
        vdl2_thread.start()

        modes_thread.join()
        vdl2_thread.join()
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        modes_receiver.term()
        vdl2_receiver.term()
        modes_thread.join(timeout=5)
        vdl2_thread.join(timeout=5)

    # 結果を保存
    output_path = pathlib.Path("tests/fixtures/weather_comparison.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    save_records(records, output_path)

    # 統計を表示
    modes_count = sum(1 for r in records if r.source == "modes")
    vdl2_count = sum(1 for r in records if r.source == "vdl2")
    print("\n=== 収集結果 ===")
    print(f"Mode-S: {modes_count} records")
    print(f"VDL2: {vdl2_count} records")
    print(f"合計: {len(records)} records")

    # 乖離を分析
    discrepancies = analyze_discrepancies(records)
    print_discrepancy_report(discrepancies)

    # 乖離詳細を JSON で保存
    if discrepancies:
        discrepancy_path = pathlib.Path("tests/fixtures/weather_discrepancies.json")
        with discrepancy_path.open("w") as f:
            # WeatherRecord を dict に変換
            serializable = []
            for d in discrepancies:
                item = {
                    "modes": {
                        "timestamp": d["modes_record"].timestamp.isoformat(),
                        "callsign": d["modes_record"].callsign,
                        "altitude_m": d["modes_record"].altitude_m,
                        "temperature_c": d["modes_record"].temperature_c,
                        "wind_speed_ms": d["modes_record"].wind_speed_ms,
                        "wind_direction_deg": d["modes_record"].wind_direction_deg,
                    },
                    "vdl2": {
                        "timestamp": d["vdl2_record"].timestamp.isoformat(),
                        "callsign": d["vdl2_record"].callsign,
                        "altitude_m": d["vdl2_record"].altitude_m,
                        "temperature_c": d["vdl2_record"].temperature_c,
                        "wind_speed_ms": d["vdl2_record"].wind_speed_ms,
                        "wind_direction_deg": d["vdl2_record"].wind_direction_deg,
                    },
                    "diff": {
                        "time_s": d["time_diff_s"],
                        "altitude_m": d["altitude_diff_m"],
                        "temp_c": d["temp_diff_c"],
                        "wind_speed_ms": d["wind_speed_diff_ms"],
                        "wind_dir_deg": d["wind_dir_diff_deg"],
                    },
                }
                serializable.append(item)
            json.dump(serializable, f, indent=2)
        print(f"\n乖離詳細を {discrepancy_path} に保存しました")


if __name__ == "__main__":
    main()
