#!/usr/bin/env python3
# ruff: noqa: S101
"""temperature.py の 30 分ビニング（numpy ベクトル化版）のテスト。

従来の純 Python 実装と同等の出力になることを検証する。
"""

import numpy
import pytest

from amdar.viewer.graph.plotting.temperature import bin_time_series


def _reference_binning(
    sorted_times: numpy.ndarray, sorted_temps: numpy.ndarray, bin_size: float
) -> tuple[list[float], list[float]]:
    """従来の純 Python 実装（リファレンス）。"""
    unique_times: list[float] = []
    avg_temps: list[float] = []

    current_bin_start = sorted_times[0]
    current_temps: list[float] = []

    for i, time_val in enumerate(sorted_times):
        if time_val <= current_bin_start + bin_size:
            current_temps.append(sorted_temps[i])
        else:
            if current_temps:
                unique_times.append(float(current_bin_start + bin_size / 2))
                avg_temps.append(float(numpy.mean(current_temps)))
            current_bin_start = time_val
            current_temps = [sorted_temps[i]]

    if current_temps:
        unique_times.append(float(current_bin_start + bin_size / 2))
        avg_temps.append(float(numpy.mean(current_temps)))

    return unique_times, avg_temps


class TestBinTimeSeries:
    """bin_time_series のテスト。"""

    @pytest.mark.parametrize("seed", [0, 1, 42])
    def test_matches_reference_random(self, seed):
        """ランダムデータでリファレンス実装と一致する"""
        rng = numpy.random.default_rng(seed)
        n = 500
        times = numpy.sort(rng.uniform(0, 7, n))  # 7 日分
        temps = rng.uniform(-60, 30, n)

        bin_size = 0.020833
        centers, means = bin_time_series(times, temps, bin_size)
        ref_centers, ref_means = _reference_binning(times, temps, bin_size)

        assert numpy.allclose(centers, ref_centers)
        assert numpy.allclose(means, ref_means)

    def test_matches_reference_with_gaps(self):
        """データに大きな隙間がある場合（ビンの再アンカー）も一致する"""
        times = numpy.array([0.0, 0.01, 0.02, 1.0, 1.005, 3.0])
        temps = numpy.array([10.0, 12.0, 14.0, -5.0, -7.0, 20.0])

        bin_size = 0.020833
        centers, means = bin_time_series(times, temps, bin_size)
        ref_centers, ref_means = _reference_binning(times, temps, bin_size)

        assert numpy.allclose(centers, ref_centers)
        assert numpy.allclose(means, ref_means)

    def test_all_in_single_bin(self):
        """全データが 1 ビンに収まる場合"""
        times = numpy.array([0.0, 0.005, 0.01])
        temps = numpy.array([1.0, 2.0, 3.0])

        centers, means = bin_time_series(times, temps, 0.020833)

        assert len(centers) == 1
        assert numpy.isclose(means[0], 2.0)

    def test_boundary_inclusive(self):
        """アンカー + bin_size ちょうどの点は同一ビンに含める"""
        bin_size = 0.5
        times = numpy.array([0.0, 0.5, 0.500001])
        temps = numpy.array([0.0, 10.0, 20.0])

        centers, means = bin_time_series(times, temps, bin_size)
        ref_centers, ref_means = _reference_binning(times, temps, bin_size)

        assert numpy.allclose(centers, ref_centers)
        assert numpy.allclose(means, ref_means)
        # 先頭 2 点が同一ビン、3 点目が次のビン
        assert len(centers) == 2
        assert numpy.isclose(means[0], 5.0)

    def test_empty(self):
        """空データ"""
        centers, means = bin_time_series(numpy.array([]), numpy.array([]))
        assert len(centers) == 0
        assert len(means) == 0

    def test_duplicate_times(self):
        """同一時刻のデータが多数ある場合も一致する"""
        times = numpy.repeat(numpy.array([0.0, 0.1, 0.2]), 5)
        temps = numpy.arange(15, dtype=numpy.float64)

        centers, means = bin_time_series(times, temps, 0.020833)
        ref_centers, ref_means = _reference_binning(times, temps, 0.020833)

        assert numpy.allclose(centers, ref_centers)
        assert numpy.allclose(means, ref_means)
