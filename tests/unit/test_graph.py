#!/usr/bin/env python3
# ruff: noqa: S101, SLF001
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

import modes.webui.api.graph


class TestTemperatureRange:
    """温度範囲関数のテスト"""

    def test_temperature_range_limited(self):
        """高度制限ありの温度範囲をテスト"""
        temp_min, temp_max = modes.webui.api.graph.get_temperature_range(limit_altitude=True)

        # 高度制限有り: -20°C～40°C
        assert temp_min == -20
        assert temp_max == 40

    def test_temperature_range_unlimited(self):
        """高度制限なしの温度範囲をテスト"""
        temp_min, temp_max = modes.webui.api.graph.get_temperature_range(limit_altitude=False)

        # 高度制限無し: -80°C～30°C
        assert temp_min == -80
        assert temp_max == 30

    def test_temperature_range_logging(self):
        """温度範囲のログ出力を確認"""
        temp_min_limited, temp_max_limited = modes.webui.api.graph.get_temperature_range(
            limit_altitude=True
        )
        temp_min_unlimited, temp_max_unlimited = modes.webui.api.graph.get_temperature_range(
            limit_altitude=False
        )

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
        # グローバル変数をリセット
        modes.webui.api.graph._git_commit_hash = None

        git_hash = modes.webui.api.graph.get_git_commit_hash()

        # 何らかの値が返される
        assert git_hash is not None
        assert isinstance(git_hash, str)
        assert len(git_hash) > 0

        # キャッシュされていること
        git_hash2 = modes.webui.api.graph.get_git_commit_hash()
        assert git_hash == git_hash2

    def test_get_git_commit_hash_cached(self):
        """Git commit ハッシュがキャッシュされること"""
        modes.webui.api.graph._git_commit_hash = "test123hash"

        git_hash = modes.webui.api.graph.get_git_commit_hash()
        assert git_hash == "test123hash"

        # リセット
        modes.webui.api.graph._git_commit_hash = None

    def test_generate_cache_key(self):
        """キャッシュキーが生成されること"""
        time_start = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        time_end = datetime.datetime(2025, 1, 7, 0, 0, 0, tzinfo=datetime.timezone.utc)

        key1 = modes.webui.api.graph.generate_cache_key(
            "scatter_2d", time_start, time_end, False
        )

        # 32文字のハッシュ
        assert len(key1) == 32
        assert key1.isalnum()

        # 同じパラメータなら同じキー
        key2 = modes.webui.api.graph.generate_cache_key(
            "scatter_2d", time_start, time_end, False
        )
        assert key1 == key2

        # 異なるパラメータなら異なるキー
        key3 = modes.webui.api.graph.generate_cache_key(
            "contour_2d", time_start, time_end, False
        )
        assert key1 != key3

        # limit_altitude が異なれば異なるキー
        key4 = modes.webui.api.graph.generate_cache_key(
            "scatter_2d", time_start, time_end, True
        )
        assert key1 != key4

    def test_cache_file_path(self):
        """キャッシュファイルパスが正しく生成されること"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir) / "cache"
            cache_key = "abc123"

            path = modes.webui.api.graph.get_cache_file_path(cache_dir, cache_key)

            assert path == cache_dir / "abc123.png"

    def test_get_cached_image_not_exists(self):
        """存在しないキャッシュファイルは None を返す"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)

            result = modes.webui.api.graph.get_cached_image(cache_dir, "nonexistent")
            assert result is None

    def test_get_cached_image_valid(self):
        """有効なキャッシュファイルが返される"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)
            cache_key = "test_key"
            cache_file = cache_dir / f"{cache_key}.png"

            # テスト用のダミーデータ
            test_data = b"PNG_IMAGE_DATA"
            cache_file.write_bytes(test_data)

            result = modes.webui.api.graph.get_cached_image(cache_dir, cache_key)
            assert result == test_data

    def test_get_cached_image_expired(self):
        """TTL を超えたキャッシュファイルは None を返す"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir)
            cache_key = "expired_key"
            cache_file = cache_dir / f"{cache_key}.png"

            # テスト用のダミーデータ
            test_data = b"PNG_IMAGE_DATA"
            cache_file.write_bytes(test_data)

            # ファイルの更新時刻を TTL + 1秒前に設定
            old_time = time.time() - modes.webui.api.graph.CACHE_TTL_SECONDS - 1
            os.utime(cache_file, (old_time, old_time))

            result = modes.webui.api.graph.get_cached_image(cache_dir, cache_key)
            assert result is None

    def test_save_to_cache(self):
        """キャッシュに画像を保存できること"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = pathlib.Path(tmpdir) / "subdir"
            cache_key = "save_test"
            test_data = b"PNG_IMAGE_DATA"

            # ディレクトリが存在しなくても保存できる
            modes.webui.api.graph.save_to_cache(cache_dir, cache_key, test_data)

            # ファイルが作成されている
            cache_file = cache_dir / f"{cache_key}.png"
            assert cache_file.exists()
            assert cache_file.read_bytes() == test_data

    def test_cache_ttl_value(self):
        """キャッシュ TTL が30分であること"""
        assert modes.webui.api.graph.CACHE_TTL_SECONDS == 30 * 60
