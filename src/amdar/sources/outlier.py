"""外れ値検出モジュール

高度-温度相関を考慮した外れ値検出を提供します。
Mode-S および VDL2 の両方のデータソースで使用可能です。

二段階アプローチ：
1. 物理的相関チェック（高度が低い→温度が高い関係を保護）
2. 高度近傍ベースの異常検知（低高度でのばらつき対応）
"""

from __future__ import annotations

import collections
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import numpy.typing as npt
import sklearn.linear_model

if TYPE_CHECKING:
    from amdar.core.types import WeatherObservation


# デフォルト設定
DEFAULT_HISTORY_SIZE: int = 30000
DEFAULT_MIN_SAMPLES: int = 100
DEFAULT_N_NEIGHBORS: int = 200
DEFAULT_DEVIATION_THRESHOLD: float = 20.0
DEFAULT_SIGMA_THRESHOLD: float = 4.0
DEFAULT_TOLERANCE_FACTOR: float = 2.5


@dataclass
class _HistoryData:
    """履歴データ（外れ値検出用）"""

    altitude: float
    temperature: float


class OutlierDetector:
    """外れ値検出器

    高度-温度相関を考慮した外れ値検出を行います。
    履歴データを蓄積し、統計的に外れ値を判定します。

    使用例:
        # リアルタイム処理
        detector = OutlierDetector()
        if not detector.is_outlier(altitude=10000, temperature=-50, callsign="JAL123"):
            # 正常値として処理
            detector.add_history(altitude=10000, temperature=-50)

        # バッチ処理
        detector = OutlierDetector()
        filtered = detector.filter_observations(observations)
    """

    def __init__(
        self,
        history_size: int = DEFAULT_HISTORY_SIZE,
        min_samples: int = DEFAULT_MIN_SAMPLES,
        n_neighbors: int = DEFAULT_N_NEIGHBORS,
        deviation_threshold: float = DEFAULT_DEVIATION_THRESHOLD,
        sigma_threshold: float = DEFAULT_SIGMA_THRESHOLD,
        tolerance_factor: float = DEFAULT_TOLERANCE_FACTOR,
    ) -> None:
        """初期化

        Args:
            history_size: 履歴データの最大サンプル数
            min_samples: 外れ値検出を開始する最小サンプル数
            n_neighbors: 高度近傍検出で使用する近傍データ数
            deviation_threshold: 絶対偏差による異常値判定閾値 [°C]
            sigma_threshold: 異常値判定のシグマ閾値
            tolerance_factor: 物理的妥当性チェックの許容範囲倍率
        """
        self._history: collections.deque[_HistoryData] = collections.deque(maxlen=history_size)
        self._min_samples = min_samples
        self._n_neighbors = n_neighbors
        self._deviation_threshold = deviation_threshold
        self._sigma_threshold = sigma_threshold
        self._tolerance_factor = tolerance_factor

    def add_history(self, altitude: float, temperature: float) -> None:
        """履歴データを追加

        Args:
            altitude: 高度 [m]
            temperature: 気温 [°C]
        """
        self._history.append(_HistoryData(altitude=altitude, temperature=temperature))

    def clear_history(self) -> None:
        """履歴データをクリア"""
        self._history.clear()

    @property
    def history_count(self) -> int:
        """履歴データ数"""
        return len(self._history)

    def is_outlier(
        self,
        altitude: float,
        temperature: float,
        callsign: str = "",
    ) -> bool:
        """外れ値かどうかを判定

        高度-温度相関を考慮した二段階アプローチで判定します。

        Args:
            altitude: 高度 [m]
            temperature: 気温 [°C]
            callsign: 航空機のコールサイン（ログ用）

        Returns:
            外れ値の場合 True、正常値の場合 False
        """
        # データが十分蓄積されていない場合は外れ値として扱わない
        if len(self._history) < self._min_samples:
            return False

        try:
            # 履歴データから特徴量を抽出
            valid_data = [
                data for data in self._history if data.altitude is not None and data.temperature is not None
            ]

            if len(valid_data) < self._min_samples:
                return False

            altitudes = np.array([[data.altitude] for data in valid_data])
            temperatures = np.array([data.temperature for data in valid_data])

            # 第一段階：線形回帰で高度-温度関係を学習
            regression_model = sklearn.linear_model.LinearRegression()
            regression_model.fit(altitudes, temperatures)

            # 物理的相関チェック
            if self._is_physically_reasonable(altitude, temperature, regression_model):
                return False  # 物理的に妥当なので外れ値ではない

            # 第二段階：高度近傍ベースの異常検知
            return self._detect_outlier_by_altitude_neighbors(
                altitude, temperature, altitudes, temperatures, callsign
            )

        except Exception as e:
            logging.warning("外れ値検出でエラーが発生しました: %s", e)
            return False

    def filter_observations(
        self,
        observations: list[WeatherObservation],
        add_to_history: bool = True,
    ) -> list[WeatherObservation]:
        """観測データリストから外れ値を除去

        Args:
            observations: WeatherObservation のリスト
            add_to_history: 正常値を履歴に追加するかどうか

        Returns:
            外れ値を除去した WeatherObservation のリスト
        """
        filtered: list[WeatherObservation] = []

        for obs in observations:
            # 温度データがない場合はスキップ
            if obs.temperature is None:
                continue

            # 温度異常値（-100°C 未満）は外れ値検出の対象外としてスキップ
            if obs.temperature < -100:
                logging.debug("温度異常値のため除外: %.1f°C", obs.temperature)
                continue

            # 外れ値判定
            if self.is_outlier(obs.altitude, obs.temperature, obs.callsign or ""):
                continue

            # 正常値として追加
            filtered.append(obs)

            # 履歴に追加
            if add_to_history:
                self.add_history(obs.altitude, obs.temperature)

        logging.info(
            "外れ値フィルタリング完了: %d -> %d (%d 件除外)",
            len(observations),
            len(filtered),
            len(observations) - len(filtered),
        )

        return filtered

    def _is_physically_reasonable(
        self,
        altitude: float,
        temperature: float,
        regression_model: sklearn.linear_model.LinearRegression,
    ) -> bool:
        """高度-温度の物理的相関が妥当かどうかを判定

        Args:
            altitude: 高度 [m]
            temperature: 気温 [°C]
            regression_model: 学習済み線形回帰モデル

        Returns:
            物理的に妥当な場合 True
        """
        try:
            # 予測温度を計算
            predicted_temp = regression_model.predict([[altitude]])[0]

            # 高度-温度の一般的な関係：高度が1000m上がると約6.5°C下がる
            # 標準大気での温度減率を考慮した許容範囲を設定
            standard_lapse_rate = 0.0065  # °C/m
            altitude_diff_threshold = 200  # m（許容する高度差）
            temp_tolerance = standard_lapse_rate * altitude_diff_threshold * self._tolerance_factor

            # 予測値との差が許容範囲内かチェック
            residual = abs(temperature - predicted_temp)

            judge = residual <= temp_tolerance

            if judge:
                logging.info(
                    "物理的に妥当な高度-温度相関のため正常値として扱います "
                    "(altitude: %.1fm, temperature: %.1f°C, predicted_temp=%.1f°C, residual=%.1f°C)",
                    altitude,
                    temperature,
                    predicted_temp,
                    residual,
                )

            return judge

        except Exception:
            return True  # エラー時は保守的に妥当とみなす

    def _detect_outlier_by_altitude_neighbors(
        self,
        altitude: float,
        temperature: float,
        altitudes: npt.NDArray[np.floating[Any]],
        temperatures: npt.NDArray[np.floating[Any]],
        callsign: str,
    ) -> bool:
        """高度近傍ベースの異常検知を実行

        高度が近いデータポイントの局所的な分布を使用して異常値を検出します。
        低高度では温度のばらつきが大きいという特性に対応できます。

        Args:
            altitude: 検査対象の高度 [m]
            temperature: 検査対象の気温 [°C]
            altitudes: 履歴データの高度配列
            temperatures: 履歴データの温度配列
            callsign: 航空機のコールサイン（ログ用）

        Returns:
            外れ値の場合 True、正常値の場合 False
        """
        # 高度差を計算
        altitude_diffs = np.abs(altitudes.flatten() - altitude)

        # 最も近い高度のインデックスを取得（最大n_neighbors個）
        n_actual = min(self._n_neighbors, len(altitude_diffs))
        nearest_indices = np.argpartition(altitude_diffs, n_actual - 1)[:n_actual]

        # 近傍データの温度を取得
        neighbor_temps = temperatures.flatten()[nearest_indices]
        neighbor_alts = altitudes.flatten()[nearest_indices]

        # 近傍データの平均高度と温度統計を計算
        mean_neighbor_alt = np.mean(neighbor_alts)
        mean_temp = np.mean(neighbor_temps)
        std_temp = np.std(neighbor_temps)

        # 温度の偏差を計算
        temp_deviation = abs(temperature - mean_temp)
        z_score = temp_deviation / std_temp if std_temp > 0 else 0

        # 異常値判定
        is_outlier = (temp_deviation > self._deviation_threshold) or (z_score > self._sigma_threshold)

        # 判定結果をログ出力
        if is_outlier:
            logging.warning(
                "%s: callsign=%s, altitude=%.1fm, temperature=%.1f°C, "
                "neighbor_mean_alt=%.1fm, neighbor_mean_temp=%.1f°C, neighbor_std=%.1f°C, "
                "deviation=%.1f°C(threshold=%.1f), z_score=%.2f (threshold=%.1f)",
                "外れ値検出（高度近傍）",
                callsign or "Unknown",
                altitude,
                temperature,
                mean_neighbor_alt,
                mean_temp,
                std_temp,
                temp_deviation,
                self._deviation_threshold,
                z_score,
                self._sigma_threshold,
            )
        else:
            logging.info(
                "%s: callsign=%s, altitude=%.1fm, temperature=%.1f°C, "
                "neighbor_mean_alt=%.1fm, neighbor_mean_temp=%.1f°C, neighbor_std=%.1f°C, "
                "deviation=%.1f°C(threshold=%.1f), z_score=%.2f (threshold=%.1f)",
                "正常値判定（高度近傍）",
                callsign or "Unknown",
                altitude,
                temperature,
                mean_neighbor_alt,
                mean_temp,
                std_temp,
                temp_deviation,
                self._deviation_threshold,
                z_score,
                self._sigma_threshold,
            )

        return bool(is_outlier)


# グローバルインスタンス（リアルタイム処理用）
_default_detector: OutlierDetector | None = None


def get_default_detector() -> OutlierDetector:
    """デフォルトの外れ値検出器を取得

    リアルタイム処理で共有される単一インスタンスを返します。
    """
    global _default_detector
    if _default_detector is None:
        _default_detector = OutlierDetector()
    return _default_detector


def reset_default_detector() -> None:
    """デフォルトの外れ値検出器をリセット

    テスト用途などで使用します。
    """
    global _default_detector
    _default_detector = None
