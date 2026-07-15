#!/usr/bin/env python3
# ruff: noqa: S101
"""キャッシュ事前生成スケジューラ (amdar.viewer.api.cache_pregeneration) のユニットテスト。

グラフ生成 (generate_sync) はモックするため matplotlib を起動せず、実行は数ミリ秒で済む。
"""

import datetime
from unittest.mock import patch

import amdar.viewer.api.cache_pregeneration as cache_pregeneration
from amdar.constants import PREGENERATION_INTERVAL_SECONDS

_TIME_START = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
_TIME_END = datetime.datetime(2026, 1, 8, 0, 0, 0, tzinfo=datetime.UTC)


class TestGenerateGraphs:
    """_generate_graphs のテスト。"""

    def test_passes_ttl_threshold(self):
        """全グラフの生成が min_cache_ttl_remaining=PREGENERATION_INTERVAL_SECONDS で呼ばれる。

        これを指定しないと、事前生成が期限切れ間近の自分自身のキャッシュに許容差ヒットして
        空振り（再配信のみ）となり、キャッシュを新鮮に保てなくなる（今回の修正点の回帰テスト）。
        """
        pregenerator = cache_pregeneration.CachePregenerator()

        with patch.object(cache_pregeneration.graph_service, "generate_sync") as mock_gen:
            generated = pregenerator._generate_graphs(_TIME_START, _TIME_END, limit_altitude=False)

        assert generated == len(cache_pregeneration._PREGENERATION_GRAPHS)
        assert mock_gen.call_count == len(cache_pregeneration._PREGENERATION_GRAPHS)
        for call in mock_gen.call_args_list:
            assert call.kwargs["min_cache_ttl_remaining"] == PREGENERATION_INTERVAL_SECONDS
            # 位置引数: (graph_name, time_start, time_end, limit_altitude)
            assert call.args[1] == _TIME_START
            assert call.args[2] == _TIME_END
            assert call.args[3] is False

    def test_stops_on_stop_requested(self):
        """stop() 済みなら生成を開始しない。"""
        pregenerator = cache_pregeneration.CachePregenerator()
        pregenerator._stop_requested = True

        with patch.object(cache_pregeneration.graph_service, "generate_sync") as mock_gen:
            generated = pregenerator._generate_graphs(_TIME_START, _TIME_END, limit_altitude=False)

        assert generated == 0
        mock_gen.assert_not_called()

    def test_continues_on_error(self):
        """個別グラフの生成が失敗しても残りは継続し、成功件数のみ数える。"""
        pregenerator = cache_pregeneration.CachePregenerator()
        total = len(cache_pregeneration._PREGENERATION_GRAPHS)

        # 最初の1グラフだけ例外、残りは成功
        side_effects = [RuntimeError("boom"), *([None] * (total - 1))]
        with patch.object(
            cache_pregeneration.graph_service, "generate_sync", side_effect=side_effects
        ) as mock_gen:
            generated = pregenerator._generate_graphs(_TIME_START, _TIME_END, limit_altitude=False)

        assert mock_gen.call_count == total
        assert generated == total - 1
