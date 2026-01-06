#!/usr/bin/env python3
# ruff: noqa: S101
"""
graph.py のユニットテスト

純粋な関数のテストを行います。
"""

import datetime
import logging
import os
import pathlib
import tempfile
import time
from unittest.mock import patch

import amdar.viewer.api.graph as graph


class TestTemperatureRange:
    """温度範囲関数のテスト"""

    def test_temperature_range_limited(self):
        """高度制限ありの温度範囲をテスト"""
        temp_min, temp_max = graph.get_temperature_range(limit_altitude=True)

        # 高度制限有り: -20°C～40°C
        assert temp_min == -20
        assert temp_max == 40

    def test_temperature_range_unlimited(self):
        """高度制限なしの温度範囲をテスト"""
        temp_min, temp_max = graph.get_temperature_range(limit_altitude=False)

        # 高度制限無し: -80°C～30°C
        assert temp_min == -80
        assert temp_max == 30

    def test_temperature_range_logging(self):
        """温度範囲のログ出力を確認"""
        temp_min_limited, temp_max_limited = graph.get_temperature_range(limit_altitude=True)
        temp_min_unlimited, temp_max_unlimited = graph.get_temperature_range(limit_altitude=False)

        logging.info(
            "Temperature ranges - Limited: %d°C～%d°C, Unlimited: %d°C～%d°C",
            temp_min_limited,
            temp_max_limited,
            temp_min_unlimited,
            temp_max_unlimited,
        )


class TestGraphCache:
    """グラフキャッシュ関連のテスト"""

    def test_get_git_commit_hash(self):
        """Git commit ハッシュを取得できること"""
        # キャッシュをクリア
        graph.get_git_commit_hash.cache_clear()

        git_hash = graph.get_git_commit_hash()

        # 何らかの値が返される
        assert git_hash is not None
        assert isinstance(git_hash, str)
        assert len(git_hash) > 0

        # キャッシュされていること（functools.cacheによる）
        git_hash2 = graph.get_git_commit_hash()
        assert git_hash == git_hash2

    @patch("amdar.viewer.api.graph.get_git_commit_hash", return_value="abc123hash")
    def test_generate_cache_filename(self, _mock_hash):
        """キャッシュファイル名が正しく生成されること"""
        time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
        time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)

        filename1 = graph.generate_cache_filename("scatter_2d", time_start, time_end, False)

        # 形式: {graph_name}_{period_seconds}_{limit}_{start_ts}_{git}.png
        # 6日間 = 518400秒
        assert filename1.startswith("scatter_2d_518400_0_")
        assert filename1.endswith("_abc123hash.png")

        # 同じパラメータなら同じファイル名
        filename2 = graph.generate_cache_filename("scatter_2d", time_start, time_end, False)
        assert filename1 == filename2

        # 異なるグラフ名なら異なるファイル名
        filename3 = graph.generate_cache_filename("contour_2d", time_start, time_end, False)
        assert filename1 != filename3

        # limit_altitude が異なれば異なるファイル名
        filename4 = graph.generate_cache_filename("scatter_2d", time_start, time_end, True)
        assert filename1 != filename4
        assert "_1_" in filename4  # limit_altitude=True なら "1"

    def test_parse_cache_filename(self):
        """キャッシュファイル名が正しくパースされること"""
        # ファイルが存在しないとパースできないので、一時ファイルを作成
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = pathlib.Path(tmpdir) / "scatter_2d_518400_0_1735689600_abc123hash.png"
            test_file.write_bytes(b"test")

            info = graph.parse_cache_filename(test_file)

            assert info is not None
            assert info.graph_name == "scatter_2d"
            assert info.period_seconds == 518400
            assert info.limit_altitude is False
            assert info.start_ts == 1735689600
            assert info.git_commit == "abc123hash"

    @patch("amdar.viewer.api.graph.get_git_commit_hash", return_value="abc123hash")
    def test_get_cached_image_not_exists(self, _mock_hash):
        """存在しないキャッシュファイルは None を返す"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)
            time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)

            result, filename = graph.get_cached_image(cache_dir, "nonexistent", time_start, time_end, False)
            assert result is None
            assert filename is None

    @patch("amdar.viewer.api.graph.get_git_commit_hash", return_value="abc123hash")
    def test_get_cached_image_valid(self, _mock_hash):
        """有効なキャッシュファイルが返される"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)
            time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)

            # まずキャッシュを保存
            test_data = b"PNG_IMAGE_DATA"
            graph.save_to_cache(cache_dir, "scatter_2d", time_start, time_end, False, test_data)

            # 同じパラメータでキャッシュを取得
            result, filename = graph.get_cached_image(cache_dir, "scatter_2d", time_start, time_end, False)
            assert result == test_data
            assert filename is not None

    @patch("amdar.viewer.api.graph.get_git_commit_hash", return_value="abc123hash")
    def test_get_cached_image_expired(self, _mock_hash):
        """TTL を超えたキャッシュファイルは削除されて None を返す"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)
            time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)

            # キャッシュを保存
            test_data = b"PNG_IMAGE_DATA"
            filename = graph.save_to_cache(cache_dir, "scatter_2d", time_start, time_end, False, test_data)
            assert filename is not None
            cache_file = cache_dir / filename

            # ファイルの更新時刻を TTL + 1秒前に設定
            old_time = time.time() - graph.CACHE_TTL_SECONDS - 1
            os.utime(cache_file, (old_time, old_time))

            # get_cached_image は期限切れファイルを削除して None を返す
            result, _ = graph.get_cached_image(cache_dir, "scatter_2d", time_start, time_end, False)
            assert result is None
            # 期限切れファイルは削除されている
            assert not cache_file.exists()

    @patch("amdar.viewer.api.graph.get_git_commit_hash", return_value="abc123hash")
    def test_get_cached_image_start_time_tolerance(self, _mock_hash):
        """開始日時の差が30分以内ならキャッシュがヒットすること"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)
            time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)

            # キャッシュを保存
            test_data = b"PNG_IMAGE_DATA"
            graph.save_to_cache(cache_dir, "scatter_2d", time_start, time_end, False, test_data)

            # 開始日時が29分ずれていてもキャッシュがヒット
            time_start_shifted = time_start + datetime.timedelta(minutes=29)
            time_end_shifted = time_end + datetime.timedelta(minutes=29)

            result, filename = graph.get_cached_image(
                cache_dir, "scatter_2d", time_start_shifted, time_end_shifted, False
            )
            assert result == test_data

            # 開始日時が31分ずれるとキャッシュがミス
            time_start_too_far = time_start + datetime.timedelta(minutes=31)
            time_end_too_far = time_end + datetime.timedelta(minutes=31)

            result2, _ = graph.get_cached_image(
                cache_dir, "scatter_2d", time_start_too_far, time_end_too_far, False
            )
            assert result2 is None

    @patch("amdar.viewer.api.graph.get_git_commit_hash", return_value="abc123hash")
    def test_save_to_cache(self, _mock_hash):
        """キャッシュに画像を保存できること"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir) / "subdir"
            time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
            time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.UTC)
            test_data = b"PNG_IMAGE_DATA"

            # ディレクトリが存在しなくても保存できる
            filename = graph.save_to_cache(cache_dir, "scatter_2d", time_start, time_end, False, test_data)

            # ファイルが作成されている
            assert filename is not None
            cache_file = cache_dir / filename
            assert cache_file.exists()
            assert cache_file.read_bytes() == test_data

    def test_cleanup_expired_cache(self):
        """期限切れキャッシュが削除されること"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)

            # 期限内のファイル
            valid_file = cache_dir / "valid.png"
            valid_file.write_bytes(b"valid")

            # 期限切れのファイル
            expired_file = cache_dir / "expired.png"
            expired_file.write_bytes(b"expired")
            old_time = time.time() - graph.CACHE_TTL_SECONDS - 1
            os.utime(expired_file, (old_time, old_time))

            # クリーンアップ実行
            deleted = graph.cleanup_expired_cache(cache_dir)

            assert deleted == 1
            assert valid_file.exists()
            assert not expired_file.exists()

    def test_cache_ttl_value(self):
        """キャッシュ TTL が30分であること"""
        assert graph.CACHE_TTL_SECONDS == 30 * 60

    @patch("amdar.viewer.api.graph.get_git_commit_hash", return_value="abc123hash")
    def test_generate_etag_key(self, _mock_hash):
        """ETagキーが正しく生成されること（開始時刻は10分単位に丸められる）"""
        # 2025-01-01 00:05:00 UTC (timestamp: 1735689900)
        # 10分単位に丸めると 00:00:00 (timestamp: 1735689600)
        time_start = datetime.datetime(2025, 1, 1, 0, 5, 0, tzinfo=datetime.UTC)
        time_end = datetime.datetime(2025, 1, 7, 0, 5, 0, tzinfo=datetime.UTC)

        etag_key = graph.generate_etag_key("scatter_2d", time_start, time_end, False)

        # 形式: {graph_name}_{period_seconds}_{limit}_{rounded_start_ts}_{git}
        # 6日間 = 518400秒, 丸められた開始時刻 = 1735689600
        assert etag_key == "scatter_2d_518400_0_1735689600_abc123hash"

    @patch("amdar.viewer.api.graph.get_git_commit_hash", return_value="abc123hash")
    def test_generate_etag_key_time_rounding(self, _mock_hash):
        """ETagキーの開始時刻が10分単位に丸められること"""
        # 基準時刻: 2025-01-01 00:00:00 UTC
        base_time = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
        time_end = base_time + datetime.timedelta(days=7)

        # 00:00:00 -> 00:00:00 に丸められる
        etag1 = graph.generate_etag_key("scatter_2d", base_time, time_end, False)

        # 00:09:59 -> 00:00:00 に丸められる（同じ結果）
        time_start_9min = base_time + datetime.timedelta(minutes=9, seconds=59)
        time_end_9min = time_end + datetime.timedelta(minutes=9, seconds=59)
        etag2 = graph.generate_etag_key("scatter_2d", time_start_9min, time_end_9min, False)
        assert etag1 == etag2

        # 00:10:00 -> 00:10:00 に丸められる（異なる結果）
        time_start_10min = base_time + datetime.timedelta(minutes=10)
        time_end_10min = time_end + datetime.timedelta(minutes=10)
        etag3 = graph.generate_etag_key("scatter_2d", time_start_10min, time_end_10min, False)
        assert etag1 != etag3

        # 00:19:59 -> 00:10:00 に丸められる（etag3と同じ結果）
        time_start_19min = base_time + datetime.timedelta(minutes=19, seconds=59)
        time_end_19min = time_end + datetime.timedelta(minutes=19, seconds=59)
        etag4 = graph.generate_etag_key("scatter_2d", time_start_19min, time_end_19min, False)
        assert etag3 == etag4

    def test_etag_time_round_value(self):
        """ETag の時刻丸め間隔が10分であること"""
        assert graph.ETAG_TIME_ROUND_SECONDS == 10 * 60
