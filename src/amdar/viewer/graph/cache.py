"""ファイルベースのグラフ画像キャッシュ。

キャッシュキーは ``{graph_name}_{period_seconds}_{limit}_{start_ts}_{git_commit}``
の形式で構成され、TTL と開始時刻の許容差によりヒット判定する。

ヒット条件:
    - graph_name, period_seconds, limit_altitude, git_commit が一致
    - 開始日時の差が :data:`CACHE_START_TIME_TOLERANCE_SECONDS` 以内
    - ファイル作成から :data:`CACHE_TTL_SECONDS` 以内
"""

from __future__ import annotations

import contextlib
import datetime
import functools
import hashlib
import logging
import os
import pathlib
import tempfile
import time
from dataclasses import dataclass

import my_lib.git_util

from amdar.constants import (
    CACHE_CLEANUP_INTERVAL_SECONDS,
    CACHE_START_TIME_TOLERANCE_SECONDS,
    CACHE_TTL_SECONDS,
    ETAG_TIME_ROUND_SECONDS,
    GraphName,
)

# 期限切れ掃除の前回実行時刻（頻繁なフルスキャンを避けるためのスロットル）
_last_cleanup_time: float = 0.0


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


def get_cached_image(
    cache_dir: pathlib.Path,
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
    min_remaining_ttl: float = 0.0,
) -> tuple[bytes | None, str | None]:
    """キャッシュから画像を取得する。

    1 回の走査でキャッシュ検索を行う。期限切れファイルの削除は
    前回実行から :data:`CACHE_CLEANUP_INTERVAL_SECONDS` 以内ならスキップする。

    Args:
        min_remaining_ttl: ヒットとみなす残り TTL の下限（秒）。残り TTL が
            この値以下のファイルはヒット扱いしない。事前生成が期限切れ間近の
            キャッシュを自分自身で再ヒットして空振りするのを防ぐために使う。
            デフォルト 0.0 では TTL 内なら常にヒット（従来の挙動）。

    Returns:
        (画像データ, キャッシュファイル名)。ヒットしなければ (None, None)。
    """
    global _last_cleanup_time

    if not cache_dir.exists():
        return None, None

    current_time = time.time()
    do_cleanup = current_time - _last_cleanup_time >= CACHE_CLEANUP_INTERVAL_SECONDS
    if do_cleanup:
        _last_cleanup_time = current_time

    git_commit = get_git_commit_hash()
    request_period = int((time_end - time_start).total_seconds())
    request_start_ts = int(time_start.timestamp())

    matched: CacheFileInfo | None = None
    deleted_count = 0

    for cache_file in cache_dir.glob("*.png"):
        info = parse_cache_filename(cache_file)
        if info is None:
            continue

        if current_time - info.created_at > CACHE_TTL_SECONDS:
            if do_cleanup:
                try:
                    cache_file.unlink()
                    deleted_count += 1
                except OSError as e:
                    logging.warning("[CACHE] Failed to delete %s: %s", cache_file.name, e)
            continue

        if matched is not None:
            continue
        if info.graph_name != graph_name:
            continue
        if info.period_seconds != request_period:
            continue
        if info.limit_altitude != limit_altitude:
            continue
        if info.git_commit != git_commit:
            continue
        if CACHE_TTL_SECONDS - (current_time - info.created_at) <= min_remaining_ttl:
            continue
        if abs(info.start_ts - request_start_ts) <= CACHE_START_TIME_TOLERANCE_SECONDS:
            matched = info

    if deleted_count > 0:
        logging.info("[CACHE] Cleaned up %d expired files", deleted_count)

    if matched is None:
        return None, None

    logging.info(
        "[CACHE] HIT: %s (start_diff: %d sec, age: %.0f sec)",
        matched.path.name,
        abs(matched.start_ts - request_start_ts),
        current_time - matched.created_at,
    )
    try:
        return matched.path.read_bytes(), matched.path.name
    except OSError as e:
        logging.warning("[CACHE] Failed to read %s: %s", matched.path.name, e)
        return None, None


def save_to_cache(
    cache_dir: pathlib.Path,
    graph_name: GraphName,
    time_start: datetime.datetime,
    time_end: datetime.datetime,
    limit_altitude: bool,
    image_bytes: bytes,
) -> str | None:
    """画像をキャッシュに保存する（同一ディレクトリの一時ファイル経由で原子的に書き込む）。"""
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        filename = generate_cache_filename(graph_name, time_start, time_end, limit_altitude)
        cache_file = cache_dir / filename

        fd, tmp_name = tempfile.mkstemp(dir=cache_dir, suffix=".tmp")
        tmp_path = pathlib.Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(image_bytes)
            tmp_path.replace(cache_file)
        except BaseException:
            with contextlib.suppress(OSError):
                tmp_path.unlink()
            raise

        logging.info("[CACHE] Saved: %s (%d bytes)", filename, len(image_bytes))
        return filename
    except OSError as e:
        logging.warning("[CACHE] Failed to save: %s", e)
        return None
