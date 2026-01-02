"""グラフ生成時間計測スクリプト

80パターン（8グラフ × 2高度選択 × 5期間）の生成時間を計測し、
DEFAULT_GENERATION_TIMES テーブル形式で出力する。

Usage:
    measure_graph_times.py [--url=<url>] [--no-cache]
    measure_graph_times.py -h | --help

Options:
    -h --help       ヘルプを表示
    --url=<url>     サーバーURL [default: http://localhost:5000]
    --no-cache      キャッシュを無効化して再計測
"""

from __future__ import annotations

import datetime
import json
import logging
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import requests
from docopt import docopt

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# グラフの種類
GRAPH_NAMES = [
    "scatter_2d",
    "scatter_3d",
    "contour_2d",
    "contour_3d",
    "density",
    "heatmap",
    "temperature",
    "wind_direction",
]

# 期間バケット（時間単位）
DURATION_BUCKETS = [
    (24, "24h"),  # 24時間
    (168, "7day"),  # 7日 = 168時間
    (720, "1month"),  # 30日 = 720時間
    (4320, "6month"),  # 180日 = 4320時間
    (8760, "1year"),  # 365日 = 8760時間
]


def get_data_range(base_url: str) -> tuple[datetime.datetime, datetime.datetime]:
    """データ範囲を取得"""
    resp = requests.get(f"{base_url}/modes-sensing/api/data-range", timeout=30)
    resp.raise_for_status()
    data = resp.json()

    earliest = datetime.datetime.fromisoformat(data["earliest"])
    latest = datetime.datetime.fromisoformat(data["latest"])

    return earliest, latest


def create_jobs(
    base_url: str,
    graphs: list[str],
    start: datetime.datetime,
    end: datetime.datetime,
    limit_altitude: bool,
) -> list[dict[str, str]]:
    """グラフ生成ジョブを作成"""
    payload = {
        "graphs": graphs,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "limit_altitude": limit_altitude,
    }

    resp = requests.post(
        f"{base_url}/modes-sensing/api/graph/job",
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["jobs"]


def poll_jobs(base_url: str, job_ids: list[str], timeout: float = 1200) -> dict[str, dict[str, Any]]:
    """ジョブの完了を待ち、結果を返す"""
    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            msg = f"Jobs timed out after {timeout} seconds"
            raise TimeoutError(msg)

        resp = requests.post(
            f"{base_url}/modes-sensing/api/graph/jobs/status",
            json={"job_ids": job_ids},
            timeout=30,
        )
        resp.raise_for_status()
        jobs = resp.json()["jobs"]

        # すべて完了したかチェック
        all_done = all(jobs[job_id]["status"] in ("completed", "failed", "timeout") for job_id in job_ids)

        if all_done:
            return jobs

        # 進捗表示
        completed = sum(1 for job_id in job_ids if jobs[job_id]["status"] == "completed")
        processing = sum(1 for job_id in job_ids if jobs[job_id]["status"] == "processing")
        logging.info(
            "  Progress: %d/%d completed, %d processing",
            completed,
            len(job_ids),
            processing,
        )

        time.sleep(2)


def clear_cache() -> None:
    """キャッシュをクリア（ローカルサーバーの場合のみ）"""
    cache_dir = Path("cache")
    if cache_dir.exists():
        for graph_dir in cache_dir.iterdir():
            if graph_dir.is_dir() and graph_dir.name in GRAPH_NAMES:
                shutil.rmtree(graph_dir)
                logging.info("Cleared cache: %s", graph_dir)


def measure_group(
    base_url: str,
    duration_hours: int,
    duration_label: str,
    limit_altitude: bool,
    data_range: tuple[datetime.datetime, datetime.datetime],
) -> dict[str, float]:
    """1グループ（8グラフ同時）を計測"""
    data_start, data_end = data_range

    # 期間に応じた開始・終了時刻を計算
    duration = datetime.timedelta(hours=duration_hours)

    # データの終了時刻から逆算して期間を設定
    end = data_end
    start = end - duration

    # データ範囲内に収める
    if start < data_start:
        start = data_start
        end = min(start + duration, data_end)

    logging.info(
        "Measuring: duration=%s, limit_altitude=%s, period=%s to %s",
        duration_label,
        limit_altitude,
        start.isoformat(),
        end.isoformat(),
    )

    # ジョブ作成（8グラフ同時）
    jobs = create_jobs(base_url, GRAPH_NAMES, start, end, limit_altitude)
    job_ids = [job["job_id"] for job in jobs]
    job_graph_map = {job["job_id"]: job["graph_name"] for job in jobs}

    # 完了を待つ
    results = poll_jobs(base_url, job_ids)

    # 結果を整理
    times: dict[str, float] = {}
    for job_id, status in results.items():
        graph_name = job_graph_map[job_id]
        if status["status"] == "completed":
            elapsed = status.get("elapsed_seconds", 0) or 0
            times[graph_name] = elapsed
            logging.info("  %s: %.2f sec", graph_name, elapsed)
        else:
            logging.warning("  %s: %s (error: %s)", graph_name, status["status"], status.get("error"))
            times[graph_name] = -1  # エラー時は -1

    return times


def output_python_table(all_results: dict[tuple[str, int, bool], float]) -> None:
    """Pythonテーブル形式で出力"""
    sys.stdout.write("\n" + "=" * 60 + "\n")
    sys.stdout.write("# 計測結果 - DEFAULT_GENERATION_TIMES テーブル\n")
    sys.stdout.write("=" * 60 + "\n\n")
    sys.stdout.write("DEFAULT_GENERATION_TIMES: dict[tuple[str, int, bool], float] = {\n")
    sys.stdout.write("    # (graph_name, duration_hours_bucket, limit_altitude): seconds\n")

    # グラフ名でソート
    for graph_name in GRAPH_NAMES:
        sys.stdout.write(f"    # {graph_name}\n")
        for duration_hours, _ in DURATION_BUCKETS:
            for limit_altitude in [False, True]:
                key = (graph_name, duration_hours, limit_altitude)
                if key in all_results:
                    elapsed = all_results[key]
                    line = f'    ("{graph_name}", {duration_hours}, {limit_altitude}): {elapsed:.1f},\n'
                    sys.stdout.write(line)
                else:
                    sys.stdout.write(f'    # ("{graph_name}", {duration_hours}, {limit_altitude}): MISSING\n')

    sys.stdout.write("}\n")


def save_json_results(all_results: dict[tuple[str, int, bool], float]) -> None:
    """JSON形式で保存"""
    json_results = {f"{k[0]}|{k[1]}|{str(k[2]).lower()}": v for k, v in all_results.items()}
    results_file = Path("scripts/measurement_results.json")
    results_file.write_text(json.dumps(json_results, indent=2))
    logging.info("Results saved to %s", results_file)


def run_measurement(base_url: str, no_cache: bool) -> int:
    """計測を実行"""
    # データ範囲を取得
    try:
        data_range = get_data_range(base_url)
        logging.info("Data range: %s to %s", data_range[0], data_range[1])
    except Exception:
        logging.exception("Failed to get data range")
        return 1

    # キャッシュクリア（オプション）
    if no_cache:
        clear_cache()

    # 全結果を格納
    all_results: dict[tuple[str, int, bool], float] = {}

    # 10グループを順番に計測
    for limit_altitude in [False, True]:
        for duration_hours, duration_label in DURATION_BUCKETS:
            try:
                times = measure_group(
                    base_url,
                    duration_hours,
                    duration_label,
                    limit_altitude,
                    data_range,
                )

                for graph_name, elapsed in times.items():
                    if elapsed >= 0:
                        all_results[(graph_name, duration_hours, limit_altitude)] = elapsed

            except Exception:
                logging.exception(
                    "Failed to measure: duration=%s, limit_altitude=%s",
                    duration_label,
                    limit_altitude,
                )

    output_python_table(all_results)
    save_json_results(all_results)
    return 0


def main() -> int:
    """メイン処理"""
    assert __doc__ is not None  # noqa: S101
    args = docopt(__doc__)

    base_url = args["--url"].rstrip("/")
    no_cache = args["--no-cache"]

    return run_measurement(base_url, no_cache)


if __name__ == "__main__":
    sys.exit(main())
