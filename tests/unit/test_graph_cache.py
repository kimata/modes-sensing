#!/usr/bin/env python3
# ruff: noqa: S101
"""グラフキャッシュ層 (amdar.viewer.graph.cache) のユニットテスト。"""

import datetime
import logging
import os
import pathlib
import tempfile
import time
from unittest.mock import patch

import amdar.constants
import amdar.viewer.graph.cache as cache


class TestGitCommitHash:
    """get_git_commit_hash のテスト。"""

    def test_returns_string(self):
        cache.get_git_commit_hash.cache_clear()
        git_hash = cache.get_git_commit_hash()
        assert git_hash is not None
        assert isinstance(git_hash, str)
        assert len(git_hash) > 0

    def test_cached(self):
        cache.get_git_commit_hash.cache_clear()
        a = cache.get_git_commit_hash()
        b = cache.get_git_commit_hash()
        assert a == b


class TestCacheFilename:
    """ファイル名生成・パースのテスト。"""

    @patch("amdar.viewer.graph.cache.get_git_commit_hash", return_value="abc123hash")
    def test_generate_cache_filename(self, _mock_hash):
        time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
        time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)

        filename1 = cache.generate_cache_filename("scatter_2d", time_start, time_end, False)

        # 形式: {graph_name}_{period_seconds}_{limit}_{start_ts}_{git}.png
        # 6 日間 = 518400 秒
        assert filename1.startswith("scatter_2d_518400_0_")
        assert filename1.endswith("_abc123hash.png")

        filename2 = cache.generate_cache_filename("scatter_2d", time_start, time_end, False)
        assert filename1 == filename2

        filename3 = cache.generate_cache_filename("contour_2d", time_start, time_end, False)
        assert filename1 != filename3

        filename4 = cache.generate_cache_filename("scatter_2d", time_start, time_end, True)
        assert filename1 != filename4
        assert "_1_" in filename4

    def test_parse_cache_filename(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = pathlib.Path(tmpdir) / "scatter_2d_518400_0_1735689600_abc123hash.png"
            test_file.write_bytes(b"test")

            info = cache.parse_cache_filename(test_file)
            assert info is not None
            assert info.graph_name == "scatter_2d"
            assert info.period_seconds == 518400
            assert info.limit_altitude is False
            assert info.start_ts == 1735689600
            assert info.git_commit == "abc123hash"


class TestEtagKey:
    """ETag キー生成のテスト。"""

    @patch("amdar.viewer.graph.cache.get_git_commit_hash", return_value="abc123hash")
    def test_basic(self, _mock_hash):
        # 2025-01-01 00:05:00 UTC -> 10 分単位丸めで 00:00:00 (timestamp 1735689600)
        time_start = datetime.datetime(2025, 1, 1, 0, 5, 0, tzinfo=datetime.UTC)
        time_end = datetime.datetime(2025, 1, 7, 0, 5, 0, tzinfo=datetime.UTC)

        etag_key = cache.generate_etag_key("scatter_2d", time_start, time_end, False)
        assert etag_key == "scatter_2d_518400_0_1735689600_abc123hash"

    @patch("amdar.viewer.graph.cache.get_git_commit_hash", return_value="abc123hash")
    def test_rounding(self, _mock_hash):
        base_time = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
        time_end = base_time + datetime.timedelta(days=7)

        etag1 = cache.generate_etag_key("scatter_2d", base_time, time_end, False)

        # 09:59 -> 00:00 と同じ
        t2_start = base_time + datetime.timedelta(minutes=9, seconds=59)
        t2_end = time_end + datetime.timedelta(minutes=9, seconds=59)
        etag2 = cache.generate_etag_key("scatter_2d", t2_start, t2_end, False)
        assert etag1 == etag2

        # 10:00 -> 異なる
        t3_start = base_time + datetime.timedelta(minutes=10)
        t3_end = time_end + datetime.timedelta(minutes=10)
        etag3 = cache.generate_etag_key("scatter_2d", t3_start, t3_end, False)
        assert etag1 != etag3

        # 19:59 -> 10:00 と同じ
        t4_start = base_time + datetime.timedelta(minutes=19, seconds=59)
        t4_end = time_end + datetime.timedelta(minutes=19, seconds=59)
        etag4 = cache.generate_etag_key("scatter_2d", t4_start, t4_end, False)
        assert etag3 == etag4

    def test_round_value(self):
        assert amdar.constants.ETAG_TIME_ROUND_SECONDS == 10 * 60


class TestCachedImage:
    """get_cached_image / save_to_cache のテスト。"""

    @patch("amdar.viewer.graph.cache.get_git_commit_hash", return_value="abc123hash")
    def test_not_exists(self, _mock_hash):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)
            time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)

            result, filename = cache.get_cached_image(cache_dir, "nonexistent", time_start, time_end, False)
            assert result is None
            assert filename is None

    @patch("amdar.viewer.graph.cache.get_git_commit_hash", return_value="abc123hash")
    def test_save_and_get(self, _mock_hash):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)
            time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)

            test_data = b"PNG_IMAGE_DATA"
            cache.save_to_cache(cache_dir, "scatter_2d", time_start, time_end, False, test_data)

            result, filename = cache.get_cached_image(cache_dir, "scatter_2d", time_start, time_end, False)
            assert result == test_data
            assert filename is not None

    @patch("amdar.viewer.graph.cache.get_git_commit_hash", return_value="abc123hash")
    def test_expired(self, _mock_hash):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)
            time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)

            test_data = b"PNG_IMAGE_DATA"
            filename = cache.save_to_cache(cache_dir, "scatter_2d", time_start, time_end, False, test_data)
            assert filename is not None
            cache_file = cache_dir / filename

            # mtime を TTL 超過に書き換え
            old_time = time.time() - amdar.constants.CACHE_TTL_SECONDS - 1
            os.utime(cache_file, (old_time, old_time))

            # クリーンアップのスロットルをリセット（削除を確実に実行させる）
            cache._last_cleanup_time = 0.0

            result, _ = cache.get_cached_image(cache_dir, "scatter_2d", time_start, time_end, False)
            assert result is None
            assert not cache_file.exists()  # 期限切れは削除される

    @patch("amdar.viewer.graph.cache.get_git_commit_hash", return_value="abc123hash")
    def test_expired_cleanup_throttled(self, _mock_hash):
        """クリーンアップ直後は期限切れファイルの削除がスキップされる（ヒットもしない）"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)
            time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)

            test_data = b"PNG_IMAGE_DATA"
            filename = cache.save_to_cache(cache_dir, "scatter_2d", time_start, time_end, False, test_data)
            assert filename is not None
            cache_file = cache_dir / filename

            old_time = time.time() - amdar.constants.CACHE_TTL_SECONDS - 1
            os.utime(cache_file, (old_time, old_time))

            # 直前にクリーンアップが実行された状態にする
            cache._last_cleanup_time = time.time()

            result, _ = cache.get_cached_image(cache_dir, "scatter_2d", time_start, time_end, False)
            assert result is None  # 期限切れはヒットしない
            assert cache_file.exists()  # スロットル中は削除されない

    @patch("amdar.viewer.graph.cache.get_git_commit_hash", return_value="abc123hash")
    def test_start_time_tolerance(self, _mock_hash):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)
            time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)

            test_data = b"PNG_IMAGE_DATA"
            cache.save_to_cache(cache_dir, "scatter_2d", time_start, time_end, False, test_data)

            # 29 分ずれ -> ヒット
            t_start_29 = time_start + datetime.timedelta(minutes=29)
            t_end_29 = time_end + datetime.timedelta(minutes=29)
            result, _ = cache.get_cached_image(cache_dir, "scatter_2d", t_start_29, t_end_29, False)
            assert result == test_data

            # 31 分ずれ -> ミス
            t_start_31 = time_start + datetime.timedelta(minutes=31)
            t_end_31 = time_end + datetime.timedelta(minutes=31)
            result2, _ = cache.get_cached_image(cache_dir, "scatter_2d", t_start_31, t_end_31, False)
            assert result2 is None

    @patch("amdar.viewer.graph.cache.get_git_commit_hash", return_value="abc123hash")
    def test_min_remaining_ttl(self, _mock_hash):
        """残り TTL が min_remaining_ttl 以下のキャッシュはヒット扱いしない。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)
            time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)

            test_data = b"PNG_IMAGE_DATA"
            filename = cache.save_to_cache(cache_dir, "scatter_2d", time_start, time_end, False, test_data)
            assert filename is not None
            cache_file = cache_dir / filename

            # 残り TTL が 100 秒になるよう mtime を調整（未期限切れ）
            remaining = 100
            aged = time.time() - (amdar.constants.CACHE_TTL_SECONDS - remaining)
            os.utime(cache_file, (aged, aged))

            # 下限 0（既定）: 残り 100 秒でもヒット
            result, _ = cache.get_cached_image(cache_dir, "scatter_2d", time_start, time_end, False)
            assert result == test_data

            # 下限 200 秒: 残り 100 秒 <= 200 なのでヒットしない（ファイルは残る）
            result2, _ = cache.get_cached_image(
                cache_dir, "scatter_2d", time_start, time_end, False, min_remaining_ttl=200
            )
            assert result2 is None
            assert cache_file.exists()

    @patch("amdar.viewer.graph.cache.get_git_commit_hash", return_value="abc123hash")
    def test_save_creates_directory(self, _mock_hash):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir) / "subdir"
            time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)
            test_data = b"PNG_IMAGE_DATA"

            filename = cache.save_to_cache(cache_dir, "scatter_2d", time_start, time_end, False, test_data)
            assert filename is not None
            cache_file = cache_dir / filename
            assert cache_file.exists()
            assert cache_file.read_bytes() == test_data


class TestCleanup:
    def test_removes_expired(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)

            valid_file = cache_dir / "valid.png"
            valid_file.write_bytes(b"valid")

            expired_file = cache_dir / "expired.png"
            expired_file.write_bytes(b"expired")
            old_time = time.time() - amdar.constants.CACHE_TTL_SECONDS - 1
            os.utime(expired_file, (old_time, old_time))

            deleted = cache.cleanup_expired_cache(cache_dir)

            assert deleted == 1
            assert valid_file.exists()
            assert not expired_file.exists()


class TestConstantsExposed:
    def test_ttl_value(self):
        logging.info("CACHE_TTL_SECONDS=%d", amdar.constants.CACHE_TTL_SECONDS)
        assert amdar.constants.CACHE_TTL_SECONDS == 30 * 60
