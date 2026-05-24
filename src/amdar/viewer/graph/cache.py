"""ファイルベースのグラフ画像キャッシュ。

キャッシュキーは ``{graph_name}_{period_seconds}_{limit}_{start_ts}_{git_commit}``
の形式で構成され、TTL と開始時刻の許容差によりヒット判定する。

ヒット条件:
    - graph_name, period_seconds, limit_altitude, git_commit が一致
    - 開始日時の差が :data:`CACHE_START_TIME_TOLERANCE_SECONDS` 以内
    - ファイル作成から :data:`CACHE_TTL_SECONDS` 以内
"""

from __future__ import annotations

import datetime
import functools
import hashlib
import logging
import pathlib
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import my_lib.git_util

from amdar.constants import (
    CACHE_START_TIME_TOLERANCE_SECONDS,
    CACHE_TTL_SECONDS,
    ETAG_TIME_ROUND_SECONDS,
    GraphName,
)

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class CacheFileInfo:
    """キャッシュファイルの情報。"""

    path: pathlib.Path
    graph_name: GraphName
    period_seconds: int
    limit_altitude: bool
    start_ts: int
    git_commit: str
    created_at: float  # ファイル作成時刻（Unix timestamp）


@functools.cache
def get_git_commit_hash() -> str:
    """現在の git commit ハッシュ（先頭12桁）を返す。"""
    try:
        revision_info = my_lib.git_util.get_revision_info()
        return revision_info.hash[:12]
    except Exception:
        logging.warning("Failed to get git commit hash")
        return "unknown"


def generate_cache_filename(
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
) -> str:
    """キャッシュファイル名を生成する。

    形式: ``{graph_name}_{period_seconds}_{limit}_{start_ts}_{git}.png``
    """
    git_commit = get_git_commit_hash()
    period_seconds = int((time_end - time_start).total_seconds())
    start_ts = int(time_start.timestamp())
    limit_str = "1" if limit_altitude else "0"
    return f"{graph_name}_{period_seconds}_{limit_str}_{start_ts}_{git_commit}.png"


def generate_etag_key(
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
) -> str:
    """ETag 用のキー（開始時刻を10分単位に丸めたもの）を生成する。"""
    git_commit = get_git_commit_hash()
    period_seconds = int((time_end - time_start).total_seconds())
    rounded_start_ts = (int(time_start.timestamp()) // ETAG_TIME_ROUND_SECONDS) * ETAG_TIME_ROUND_SECONDS
    limit_str = "1" if limit_altitude else "0"
    return f"{graph_name}_{period_seconds}_{limit_str}_{rounded_start_ts}_{git_commit}"


def generate_stable_job_id(
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
) -> str:
    """キャッシュヒット時用の安定ジョブ ID（同パラメータで常に同一）。"""
    key = generate_etag_key(graph_name, time_start, time_end, limit_altitude)
    hash_hex = hashlib.sha256(key.encode()).hexdigest()
    # UUID 風の 8-4-4-4-12 フォーマット
    return f"{hash_hex[:8]}-{hash_hex[8:12]}-{hash_hex[12:16]}-{hash_hex[16:20]}-{hash_hex[20:32]}"


def parse_cache_filename(filepath: pathlib.Path) -> CacheFileInfo | None:
    """キャッシュファイル名をパースして情報を取得する。"""
    filename = filepath.stem
    parts = filename.rsplit("_", 4)

    if len(parts) != 5:
        return None

    try:
        graph_name_str = parts[0]
        period_seconds = int(parts[1])
        limit_altitude = parts[2] == "1"
        start_ts = int(parts[3])
        git_commit = parts[4]
    except ValueError:
        return None

    # 有効なグラフ名か検証（循環依存を避けるため遅延 import）
    from amdar.viewer.graph.definitions import GRAPH_DEF_MAP

    if graph_name_str not in GRAPH_DEF_MAP:
        return None

    try:
        created_at = filepath.stat().st_mtime
    except OSError:
        return None

    return CacheFileInfo(
        path=filepath,
        graph_name=graph_name_str,  # type: ignore[arg-type]  # GRAPH_DEF_MAP に存在することを検証済み
        period_seconds=period_seconds,
        limit_altitude=limit_altitude,
        start_ts=start_ts,
        git_commit=git_commit,
        created_at=created_at,
    )


def cleanup_expired_cache(cache_dir: pathlib.Path) -> int:
    """TTL を超えたキャッシュファイルを削除し、削除件数を返す。"""
    if not cache_dir.exists():
        return 0

    deleted_count = 0
    current_time = time.time()

    for cache_file in cache_dir.glob("*.png"):
        try:
            mtime = cache_file.stat().st_mtime
            if current_time - mtime > CACHE_TTL_SECONDS:
                cache_file.unlink()
                deleted_count += 1
                logging.info(
                    "[CACHE] Deleted expired: %s (age: %.0f sec)",
                    cache_file.name,
                    current_time - mtime,
                )
        except OSError as e:
            logging.warning("[CACHE] Failed to delete %s: %s", cache_file.name, e)

    return deleted_count


def find_matching_cache(
    cache_dir: pathlib.Path,
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
) -> CacheFileInfo | None:
    """条件に合うキャッシュファイルを検索する。"""
    if not cache_dir.exists():
        return None

    git_commit = get_git_commit_hash()
    request_period = int((time_end - time_start).total_seconds())
    request_start_ts = int(time_start.timestamp())
    current_time = time.time()

    for cache_file in cache_dir.glob("*.png"):
        info = parse_cache_filename(cache_file)
        if info is None:
            continue
        if info.graph_name != graph_name:
            continue
        if info.period_seconds != request_period:
            continue
        if info.limit_altitude != limit_altitude:
            continue
        if info.git_commit != git_commit:
            continue
        if current_time - info.created_at > CACHE_TTL_SECONDS:
            continue

        start_time_diff = abs(info.start_ts - request_start_ts)
        if start_time_diff <= CACHE_START_TIME_TOLERANCE_SECONDS:
            logging.info(
                "[CACHE] HIT: %s (start_diff: %d sec, age: %.0f sec)",
                cache_file.name,
                start_time_diff,
                current_time - info.created_at,
            )
            return info

    return None


def get_cached_image(
    cache_dir: pathlib.Path,
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
) -> tuple[bytes | None, str | None]:
    """キャッシュから画像を取得する。

    Returns:
        (画像データ, キャッシュファイル名)。ヒットしなければ (None, None)。
    """
    deleted = cleanup_expired_cache(cache_dir)
    if deleted > 0:
        logging.info("[CACHE] Cleaned up %d expired files", deleted)

    cache_info = find_matching_cache(cache_dir, graph_name, time_start, time_end, limit_altitude)
    if cache_info is None:
        return None, None

    try:
        return cache_info.path.read_bytes(), cache_info.path.name
    except OSError as e:
        logging.warning("[CACHE] Failed to read %s: %s", cache_info.path.name, e)
        return None, None


def save_to_cache(
    cache_dir: pathlib.Path,
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
    image_bytes: bytes,
) -> str | None:
    """画像をキャッシュに保存する。"""
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        filename = generate_cache_filename(graph_name, time_start, time_end, limit_altitude)
        cache_file = cache_dir / filename
        cache_file.write_bytes(image_bytes)
        logging.info("[CACHE] Saved: %s (%d bytes)", filename, len(image_bytes))
        return filename
    except OSError as e:
        logging.warning("[CACHE] Failed to save: %s", e)
        return None
