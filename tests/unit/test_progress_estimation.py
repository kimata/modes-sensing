#!/usr/bin/env python3
# ruff: noqa: S101
"""
progress_estimation.py のテスト
"""

import amdar.viewer.api.progress_estimation as progress_estimation


class TestGetDurationBucket:
    """_get_duration_bucket のテスト"""

    def test_bucket_24h(self):
        """24時間以下"""
        assert progress_estimation._get_duration_bucket(12) == 24
        assert progress_estimation._get_duration_bucket(24) == 24

    def test_bucket_168h(self):
        """1週間以下"""
        assert progress_estimation._get_duration_bucket(48) == 168
        assert progress_estimation._get_duration_bucket(168) == 168

    def test_bucket_720h(self):
        """1ヶ月以下"""
        assert progress_estimation._get_duration_bucket(300) == 720
        assert progress_estimation._get_duration_bucket(720) == 720

    def test_bucket_4320h(self):
        """6ヶ月以下"""
        assert progress_estimation._get_duration_bucket(2000) == 4320
        assert progress_estimation._get_duration_bucket(4320) == 4320

    def test_bucket_8760h(self):
        """1年以上"""
        assert progress_estimation._get_duration_bucket(5000) == 8760
        assert progress_estimation._get_duration_bucket(10000) == 8760


class TestGetDefaultGenerationTime:
    """_get_default_generation_time のテスト"""

    def test_known_graph(self):
        """既知のグラフタイプ"""
        time = progress_estimation._get_default_generation_time("scatter_2d", 24, False)
        assert time == 3.0

    def test_limit_altitude(self):
        """高度制限あり"""
        time = progress_estimation._get_default_generation_time("scatter_2d", 24, True)
        assert time == 2.0

    def test_unknown_graph(self):
        """未知のグラフタイプ"""
        time = progress_estimation._get_default_generation_time("unknown_graph", 24, False)
        assert time == 30.0  # デフォルト値


class TestGenerationTimeHistory:
    """GenerationTimeHistory のテスト"""

    def test_singleton(self):
        """シングルトンパターン"""
        history1 = progress_estimation.GenerationTimeHistory()
        history2 = progress_estimation.GenerationTimeHistory()
        assert history1 is history2

    def test_make_key(self):
        """キー生成"""
        history = progress_estimation.GenerationTimeHistory()
        key = history._make_key("scatter_2d", 48, True)
        assert key == "scatter_2d|168|true"

    def test_get_estimated_time_default(self):
        """デフォルトの推定時間を取得"""
        history = progress_estimation.GenerationTimeHistory()

        # 履歴をクリア
        with history._history_lock:
            history._history.clear()

        time = history.get_estimated_time("scatter_2d", 24, False)
        assert time == 3.0

    def test_record_and_get(self, tmp_path):
        """記録と取得"""
        history = progress_estimation.GenerationTimeHistory()

        # テスト用に初期化
        history._cache_file = tmp_path / "test_times.json"
        history._initialized = True

        # 記録
        history.record("test_graph", 24, False, 5.0)

        # 取得
        time = history.get_estimated_time("test_graph", 24, False)
        assert time == 5.0

    def test_record_zero_elapsed(self, tmp_path):
        """0秒以下は記録しない"""
        history = progress_estimation.GenerationTimeHistory()

        # テスト用に初期化
        history._cache_file = tmp_path / "test_times.json"
        history._initialized = True

        # 0秒を記録（記録されない）
        history.record("zero_test", 24, False, 0)

        # デフォルト値が返る
        time = history.get_estimated_time("zero_test", 24, False)
        assert time == 30.0  # デフォルト

    def test_initialize(self, tmp_path):
        """初期化"""
        # 新しいインスタンスを強制的に作成（テスト用）
        history = progress_estimation.GenerationTimeHistory()
        history._initialized = False

        history.initialize(tmp_path)

        assert history._initialized is True
        assert history._cache_file == tmp_path / "generation_times.json"

    def test_initialize_twice(self, tmp_path):
        """二重初期化はスキップ"""
        history = progress_estimation.GenerationTimeHistory()
        history._initialized = False

        history.initialize(tmp_path)
        first_cache_file = history._cache_file

        # 別のディレクトリで初期化しても変わらない
        history.initialize(tmp_path / "other")
        assert history._cache_file == first_cache_file

    def test_load_history(self, tmp_path):
        """履歴ファイルの読み込み"""
        import json

        # 履歴ファイルを作成
        cache_file = tmp_path / "generation_times.json"
        cache_file.write_text(json.dumps({"scatter_2d|24|false": 2.5}))

        history = progress_estimation.GenerationTimeHistory()
        history._cache_file = cache_file
        history._load()

        assert "scatter_2d|24|false" in history._history
        assert history._history["scatter_2d|24|false"] == 2.5

    def test_load_corrupted_file(self, tmp_path):
        """破損した履歴ファイルの読み込み"""
        # 不正なJSONファイルを作成
        cache_file = tmp_path / "generation_times.json"
        cache_file.write_text("invalid json {{{")

        history = progress_estimation.GenerationTimeHistory()
        history._cache_file = cache_file
        history._history = {"existing": 1.0}

        # エラーでも例外なく、履歴は空になる
        history._load()
        assert history._history == {}

    def test_save_history(self, tmp_path):
        """履歴ファイルの保存"""
        import json

        cache_file = tmp_path / "generation_times.json"

        history = progress_estimation.GenerationTimeHistory()
        history._cache_file = cache_file
        history._history = {"test_key": 3.5}

        history._save()

        assert cache_file.exists()
        data = json.loads(cache_file.read_text())
        assert data["test_key"] == 3.5
