#!/usr/bin/env python3
# ruff: noqa: S101
"""outlier.py のユニットテスト

OutlierDetector のスレッド安全性と共有インスタンス管理をテストします。
"""

from __future__ import annotations

import threading

import amdar.sources.outlier
from amdar.sources.outlier import OutlierDetector


class TestOutlierDetectorThreadSafety:
    """Mode-S / VDL2 受信スレッドからの同時アクセスに対する安全性"""

    def test_concurrent_add_and_detect(self) -> None:
        """履歴追加と外れ値判定を並行実行しても例外が発生しない

        修正前は is_outlier() の履歴走査中に他スレッドが add_history() を
        呼ぶと RuntimeError (deque mutated during iteration) が発生し、
        broad except に握り潰されて外れ値検出が無効化されていた。
        """
        detector = OutlierDetector(history_size=1000, min_samples=10, n_neighbors=5)

        # 判定が実行される程度の初期履歴を投入
        for i in range(50):
            detector.add_history(altitude=1000.0 + i * 100, temperature=15.0 - i * 0.65)

        errors: list[Exception] = []
        stop = threading.Event()

        def _writer() -> None:
            i = 0
            while not stop.is_set():
                try:
                    detector.add_history(altitude=1000.0 + (i % 100) * 100, temperature=15.0 - (i % 100))
                except Exception as e:
                    errors.append(e)
                    return
                i += 1

        def _reader() -> None:
            try:
                for i in range(50):
                    detector.is_outlier(altitude=5000.0 + i, temperature=-10.0, callsign="TEST")
            except Exception as e:
                errors.append(e)
            finally:
                stop.set()

        writer = threading.Thread(target=_writer)
        reader = threading.Thread(target=_reader)
        writer.start()
        reader.start()
        reader.join(timeout=60)
        stop.set()
        writer.join(timeout=10)

        assert errors == []

    def test_history_count_consistent_after_concurrent_add(self) -> None:
        """複数スレッドからの履歴追加後も件数が一致する"""
        detector = OutlierDetector(history_size=100000)
        n_threads = 4
        n_adds = 1000

        def _add() -> None:
            for i in range(n_adds):
                detector.add_history(altitude=float(i), temperature=float(-i % 60))

        threads = [threading.Thread(target=_add) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert detector.history_count == n_threads * n_adds


class TestDefaultDetector:
    """共有インスタンス管理のテスト"""

    def test_concurrent_get_returns_single_instance(self) -> None:
        """複数スレッドから同時取得しても単一インスタンスが返る"""
        amdar.sources.outlier.reset_default_detector()

        instances: list[OutlierDetector] = []
        barrier = threading.Barrier(8)

        def _get() -> None:
            barrier.wait()
            instances.append(amdar.sources.outlier.get_default_detector())

        threads = [threading.Thread(target=_get) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len({id(instance) for instance in instances}) == 1

        amdar.sources.outlier.reset_default_detector()
