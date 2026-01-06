"""
グラフ生成時間の推定とプログレス管理

グラフ生成の推定時間を管理し、プログレスバー表示に使用する。
- デフォルト推定時間テーブル（計測結果ベース）
- 履歴管理クラス（生成時間を記録して次回の推定に使用）
"""

from __future__ import annotations

import json
import logging
import threading
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    import pathlib

# グラフ生成時間のデフォルト推定値（秒）
# キー: (graph_name, duration_hours_bucket, limit_altitude)
# 2026-01-02 計測結果（1年分は1ヶ月と6ヶ月から線形補完）
_DEFAULT_GENERATION_TIMES: dict[tuple[str, int, bool], float] = {
    # scatter_2d
    ("scatter_2d", 24, False): 3.7,
    ("scatter_2d", 24, True): 2.3,
    ("scatter_2d", 168, False): 10.5,
    ("scatter_2d", 168, True): 2.2,
    ("scatter_2d", 720, False): 16.1,
    ("scatter_2d", 720, True): 3.2,
    ("scatter_2d", 4320, False): 98.4,
    ("scatter_2d", 4320, True): 4.7,
    ("scatter_2d", 8760, False): 199.9,
    ("scatter_2d", 8760, True): 6.6,
    # scatter_3d
    ("scatter_3d", 24, False): 4.0,
    ("scatter_3d", 24, True): 3.3,
    ("scatter_3d", 168, False): 12.1,
    ("scatter_3d", 168, True): 4.2,
    ("scatter_3d", 720, False): 18.6,
    ("scatter_3d", 720, True): 3.2,
    ("scatter_3d", 4320, False): 117.9,
    ("scatter_3d", 4320, True): 5.2,
    ("scatter_3d", 8760, False): 240.4,
    ("scatter_3d", 8760, True): 7.7,
    # contour_2d
    ("contour_2d", 24, False): 3.0,
    ("contour_2d", 24, True): 2.2,
    ("contour_2d", 168, False): 8.0,
    ("contour_2d", 168, True): 3.2,
    ("contour_2d", 720, False): 14.1,
    ("contour_2d", 720, True): 3.2,
    ("contour_2d", 4320, False): 83.3,
    ("contour_2d", 4320, True): 4.2,
    ("contour_2d", 8760, False): 168.6,
    ("contour_2d", 8760, True): 5.4,
    # contour_3d
    ("contour_3d", 24, False): 5.5,
    ("contour_3d", 24, True): 3.8,
    ("contour_3d", 168, False): 10.5,
    ("contour_3d", 168, True): 4.7,
    ("contour_3d", 720, False): 15.6,
    ("contour_3d", 720, True): 3.7,
    ("contour_3d", 4320, False): 89.8,
    ("contour_3d", 4320, True): 4.7,
    ("contour_3d", 8760, False): 181.3,
    ("contour_3d", 8760, True): 5.9,
    # density
    ("density", 24, False): 3.5,
    ("density", 24, True): 2.2,
    ("density", 168, False): 8.0,
    ("density", 168, True): 2.2,
    ("density", 720, False): 17.1,
    ("density", 720, True): 2.7,
    ("density", 4320, False): 105.4,
    ("density", 4320, True): 4.2,
    ("density", 8760, False): 214.3,
    ("density", 8760, True): 6.1,
    # heatmap
    ("heatmap", 24, False): 3.5,
    ("heatmap", 24, True): 2.7,
    ("heatmap", 168, False): 7.0,
    ("heatmap", 168, True): 2.7,
    ("heatmap", 720, False): 13.0,
    ("heatmap", 720, True): 2.7,
    ("heatmap", 4320, False): 84.3,
    ("heatmap", 4320, True): 3.6,
    ("heatmap", 8760, False): 172.2,
    ("heatmap", 8760, True): 4.7,
    # temperature
    ("temperature", 24, False): 2.5,
    ("temperature", 24, True): 2.2,
    ("temperature", 168, False): 6.0,
    ("temperature", 168, True): 2.2,
    ("temperature", 720, False): 10.5,
    ("temperature", 720, True): 2.7,
    ("temperature", 4320, False): 63.7,
    ("temperature", 4320, True): 3.1,
    ("temperature", 8760, False): 129.3,
    ("temperature", 8760, True): 3.6,
    # wind_direction
    ("wind_direction", 24, False): 3.5,
    ("wind_direction", 24, True): 2.2,
    ("wind_direction", 168, False): 7.5,
    ("wind_direction", 168, True): 2.7,
    ("wind_direction", 720, False): 14.5,
    ("wind_direction", 720, True): 2.1,
    ("wind_direction", 4320, False): 79.3,
    ("wind_direction", 4320, True): 4.1,
    ("wind_direction", 8760, False): 159.2,
    ("wind_direction", 8760, True): 6.6,
}

# 期間バケット（時間）
_DURATION_BUCKETS = [24, 168, 720, 4320, 8760]


def _get_duration_bucket(hours: float) -> int:
    """期間をバケットに変換"""
    if hours <= 24:
        return 24
    if hours <= 168:
        return 168
    if hours <= 720:
        return 720
    if hours <= 4320:
        return 4320
    return 8760


def _get_default_generation_time(graph_name: str, duration_hours: float, limit_altitude: bool) -> float:
    """デフォルトの推定生成時間を取得"""
    bucket = _get_duration_bucket(duration_hours)
    key = (graph_name, bucket, limit_altitude)
    return _DEFAULT_GENERATION_TIMES.get(key, 30.0)  # デフォルト30秒


class GenerationTimeHistory:
    """グラフ生成時間の履歴管理

    生成時間を記録し、次回の推定に使用する。
    履歴はcacheディレクトリにJSONで永続化する。
    """

    _instance: GenerationTimeHistory | None = None
    _lock = threading.Lock()

    # インスタンス属性の型宣言
    _history: dict[str, float]
    _history_lock: threading.Lock
    _cache_file: pathlib.Path | None
    _initialized: bool

    def __new__(cls) -> Self:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._history = {}
                    instance._history_lock = threading.Lock()
                    instance._cache_file = None
                    instance._initialized = False
                    cls._instance = instance
        return cls._instance  # type: ignore[return-value]

    def initialize(self, cache_dir: pathlib.Path) -> None:
        """キャッシュディレクトリを設定し、履歴を読み込む"""
        if self._initialized:
            return

        with self._history_lock:
            if self._initialized:
                return

            self._cache_file = cache_dir / "generation_times.json"
            self._load()
            self._initialized = True
            logging.info("GenerationTimeHistory initialized: %s", self._cache_file)

    def _make_key(self, graph_name: str, duration_hours: float, limit_altitude: bool) -> str:
        """履歴キーを生成"""
        bucket = _get_duration_bucket(duration_hours)
        return f"{graph_name}|{bucket}|{str(limit_altitude).lower()}"

    def _load(self) -> None:
        """履歴ファイルを読み込む"""
        if self._cache_file is None or not self._cache_file.exists():
            return

        try:
            with self._cache_file.open("r", encoding="utf-8") as f:
                self._history = json.load(f)
            logging.info("Loaded generation time history: %d entries", len(self._history))
        except Exception as e:
            logging.warning("Failed to load generation time history: %s", e)
            self._history = {}

    def _save(self) -> None:
        """履歴ファイルに保存"""
        if self._cache_file is None:
            return

        try:
            self._cache_file.parent.mkdir(parents=True, exist_ok=True)
            with self._cache_file.open("w", encoding="utf-8") as f:
                json.dump(self._history, f, indent=2)
        except Exception as e:
            logging.warning("Failed to save generation time history: %s", e)

    def get_estimated_time(self, graph_name: str, duration_hours: float, limit_altitude: bool) -> float:
        """推定生成時間を取得

        履歴があればその値を、なければデフォルト値を返す。
        """
        key = self._make_key(graph_name, duration_hours, limit_altitude)

        with self._history_lock:
            if key in self._history:
                return self._history[key]

        return _get_default_generation_time(graph_name, duration_hours, limit_altitude)

    def record(self, graph_name: str, duration_hours: float, limit_altitude: bool, elapsed: float) -> None:
        """生成時間を記録"""
        if elapsed <= 0:
            return

        key = self._make_key(graph_name, duration_hours, limit_altitude)

        with self._history_lock:
            self._history[key] = elapsed
            self._save()

        logging.debug("Recorded generation time: %s = %.2f sec", key, elapsed)


# グローバルインスタンス
generation_time_history = GenerationTimeHistory()
