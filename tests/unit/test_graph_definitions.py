#!/usr/bin/env python3
# ruff: noqa: S101
"""
GRAPH_DEF_MAP と GraphName Literal の整合性テスト
"""

import typing

from amdar.constants import GraphName
from amdar.viewer.graph.definitions import GRAPH_DEF_MAP


class TestGraphDefinitions:
    """グラフ定義の整合性テスト"""

    def test_graph_def_map_covers_all_graph_names(self):
        """GRAPH_DEF_MAP のキーが GraphName Literal と一致すること"""
        graph_names = set(typing.get_args(GraphName))
        assert set(GRAPH_DEF_MAP.keys()) == graph_names

    def test_all_definitions_have_valid_attributes(self):
        """全定義が呼び出し可能な func と正のサイズを持つこと"""
        for graph_name, graph_def in GRAPH_DEF_MAP.items():
            assert callable(graph_def.func), f"{graph_name} の func が呼び出し可能でない"
            assert len(graph_def.size) == 2
            assert all(x > 0 for x in graph_def.size)
            assert graph_def.file.endswith(".png")

    def test_temperature_profile_registered(self):
        """temperature_profile が登録されていること"""
        assert "temperature_profile" in GRAPH_DEF_MAP
        graph_def = GRAPH_DEF_MAP["temperature_profile"]
        assert graph_def.size == (1600, 1600)
        assert graph_def.file == "temperature_profile.png"

    def test_hodograph_registered(self):
        """hodograph が登録されていること"""
        assert "hodograph" in GRAPH_DEF_MAP
        graph_def = GRAPH_DEF_MAP["hodograph"]
        assert graph_def.size == (1600, 1600)
        assert graph_def.file == "hodograph.png"
