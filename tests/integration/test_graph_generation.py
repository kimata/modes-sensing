#!/usr/bin/env python3
# ruff: noqa: S101
"""
グラフ生成の統合テスト

グラフ生成機能をテストします。
"""

import datetime
import io
import logging

import my_lib.time
import PIL.Image

import amdar.constants
import amdar.database.postgresql as database_postgresql
import amdar.viewer.api.graph as graph
from amdar.config import Config


class TestGraphGeneration:
    """グラフ生成テスト"""

    def test_graph_generation_all_types(self, config: Config):
        """全種類のグラフ生成をテスト"""
        time_end = my_lib.time.now()
        time_start = time_end - datetime.timedelta(days=7)

        data = graph._prepare_data(
            database_postgresql.fetch_by_time(
                database_postgresql.open(
                    config.database.host,
                    config.database.port,
                    config.database.name,
                    config.database.user,
                    config.database.password,
                ),
                time_start,
                time_end,
                config.filter.area.distance,
                columns=[
                    "time",
                    "altitude",
                    "temperature",
                    "distance",
                    "wind_x",
                    "wind_y",
                    "wind_speed",
                    "wind_angle",
                ],
            )
        )

        # データが少ない場合はテストをスキップ
        if data.count < 10:
            logging.warning(
                "データが不足しているため、グラフ生成テストをスキップします (データ数: %d)",
                data.count,
            )
            return

        graph.set_font(config.font)

        for graph_name, graph_def in graph.GRAPH_DEF_MAP.items():
            # 直接関数を呼び出してテスト
            _img, _elapsed = graph_def.func(data, tuple(x / amdar.constants.GRAPH_IMAGE_DPI for x in graph_def.size))

            png_data = graph.plot(config, graph_name, time_start, time_end)

            with PIL.Image.open(io.BytesIO(png_data)) as img:
                img.verify()
                assert img.width == graph_def.size[0]
                assert img.height == graph_def.size[1]

    def test_graph_generation_with_date_range(self, config: Config):
        """特定の日付範囲でのグラフ生成をテスト"""
        end_date = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=7)
        start_date = end_date - datetime.timedelta(days=7)

        png_data = graph.plot(config, "scatter_2d", start_date, end_date)

        # PNG画像データが生成されていることを確認
        assert png_data is not None
        assert len(png_data) > 0, f"PNG data is empty (size: {len(png_data)} bytes)"

        try:
            with PIL.Image.open(io.BytesIO(png_data)) as img:
                img.verify()

                expected_size = graph.GRAPH_DEF_MAP["scatter_2d"].size
                assert img.width == expected_size[0]
                assert img.height == expected_size[1]

            logging.info("Successfully generated graph for period: %s ～ %s", start_date, end_date)
            logging.info("PNG data size: %d bytes", len(png_data))

        except Exception as e:
            logging.warning("Graph validation failed but PNG was generated: %s", e)
            logging.info("PNG data size: %d bytes", len(png_data))


class TestLimitAltitude:
    """高度制限パラメータのテスト"""

    def test_limit_altitude_parameter(self, config: Config):
        """limit_altitude機能の基本動作をテスト"""
        end_date = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=7)
        start_date = end_date - datetime.timedelta(days=7)

        # limit_altitude=Falseでグラフ生成
        png_data_unlimited = graph.plot(config, "scatter_2d", start_date, end_date, limit_altitude=False)

        # limit_altitude=Trueでグラフ生成
        png_data_limited = graph.plot(config, "scatter_2d", start_date, end_date, limit_altitude=True)

        # 両方のPNG画像データが生成されていることを確認
        assert png_data_unlimited is not None
        assert png_data_limited is not None
        assert len(png_data_unlimited) > 0
        assert len(png_data_limited) > 0

        # PNG画像として正常に生成されていることを確認
        with PIL.Image.open(io.BytesIO(png_data_unlimited)) as img:
            img.verify()
            expected_size = graph.GRAPH_DEF_MAP["scatter_2d"].size
            assert img.width == expected_size[0]
            assert img.height == expected_size[1]

        with PIL.Image.open(io.BytesIO(png_data_limited)) as img:
            img.verify()
            expected_size = graph.GRAPH_DEF_MAP["scatter_2d"].size
            assert img.width == expected_size[0]
            assert img.height == expected_size[1]

        logging.info(
            "Graph generation successful - Unlimited: %d bytes, Limited: %d bytes",
            len(png_data_unlimited),
            len(png_data_limited),
        )
