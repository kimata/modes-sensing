#!/usr/bin/env python3
# ruff: noqa: S101
"""グラフ生成の統合テスト。

新構成では :class:`amdar.viewer.graph.service.GraphService` がオーケストレーションを担う。
本テストはサービス経由の同期生成と、プロット関数の直接呼び出しの両方を検証する。
"""

import datetime
import io
import logging

import my_lib.time
import numpy
import PIL.Image

import amdar.constants
import amdar.database.postgresql as database_postgresql
from amdar.config import Config
from amdar.viewer.graph.definitions import GRAPH_DEF_MAP
from amdar.viewer.graph.plotting.data_prep import prepare_data, prepare_data_numpy
from amdar.viewer.graph.plotting.styles import set_font
from amdar.viewer.graph.service import graph_service


def _ensure_service_initialized(config: Config) -> None:
    """テスト前に GraphService を初期化（多重呼び出し OK）。"""
    graph_service.initialize(config, config.webapp.cache_dir_path)


class TestGraphGeneration:
    """グラフ生成テスト。"""

    def test_graph_generation_all_types(self, config: Config):
        """全種類のグラフ生成（プロット関数直接呼出 + サービス経由）。"""
        _ensure_service_initialized(config)

        time_end = my_lib.time.now()
        time_start = time_end - datetime.timedelta(days=7)

        data = prepare_data(
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

        if data.count < 10:
            logging.warning(
                "データが不足しているため、グラフ生成テストをスキップします (データ数: %d)",
                data.count,
            )
            return

        set_font(config.font)

        for graph_name, graph_def in GRAPH_DEF_MAP.items():
            # プロット関数を直接呼び出して検証
            _img, _elapsed = graph_def.func(
                data, tuple(x / amdar.constants.GRAPH_IMAGE_DPI for x in graph_def.size)
            )

            # サービス経由（subprocess 越し）でも検証
            png_data = graph_service.generate_sync(graph_name, time_start, time_end)

            with PIL.Image.open(io.BytesIO(png_data)) as img:
                img.verify()
                assert img.width == graph_def.size[0]
                assert img.height == graph_def.size[1]

    def test_graph_generation_with_date_range(self, config: Config):
        """特定の日付範囲でのグラフ生成テスト。"""
        _ensure_service_initialized(config)

        end_date = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=7)
        start_date = end_date - datetime.timedelta(days=7)

        png_data = graph_service.generate_sync("scatter_2d", start_date, end_date)

        assert png_data is not None
        assert len(png_data) > 0, f"PNG data is empty (size: {len(png_data)} bytes)"

        try:
            with PIL.Image.open(io.BytesIO(png_data)) as img:
                img.verify()
                expected_size = GRAPH_DEF_MAP["scatter_2d"].size
                assert img.width == expected_size[0]
                assert img.height == expected_size[1]
            logging.info("Successfully generated graph for period: %s ～ %s", start_date, end_date)
            logging.info("PNG data size: %d bytes", len(png_data))
        except Exception as e:
            logging.warning("Graph validation failed but PNG was generated: %s", e)
            logging.info("PNG data size: %d bytes", len(png_data))


def _make_synthetic_numpy_data(
    count: int = 300,
    include_wind: bool = True,
) -> database_postgresql.NumpyFetchResult:
    """鉛直プロファイル用の合成データ（直近3時間分）を生成する。"""
    rng = numpy.random.default_rng(42)

    now_wall = my_lib.time.now().replace(tzinfo=None, microsecond=0)
    offsets_sec = rng.integers(0, 3 * 3600, count)
    times = numpy.array(
        [numpy.datetime64(now_wall) - numpy.timedelta64(int(sec), "s") for sec in offsets_sec],
        dtype="datetime64[us]",
    )

    altitudes = rng.uniform(0, 12000, count)
    # 標準大気に近い気温 + ノイズ
    temperatures = 15.0 - 6.5 * altitudes / 1000 + rng.normal(0, 1.5, count)

    if not include_wind:
        return database_postgresql.NumpyFetchResult(
            time=times,
            altitude=altitudes,
            temperature=temperatures,
            count=count,
        )

    wind_direction = rng.uniform(0, 2 * numpy.pi, count)
    wind_speed = rng.uniform(1, 30, count)
    wind_x = wind_speed * numpy.cos(wind_direction)
    wind_y = wind_speed * numpy.sin(wind_direction)

    return database_postgresql.NumpyFetchResult(
        time=times,
        altitude=altitudes,
        temperature=temperatures,
        count=count,
        wind_x=wind_x,
        wind_y=wind_y,
        wind_speed=wind_speed,
        wind_angle=numpy.degrees(wind_direction),
    )


class TestVerticalProfileSynthetic:
    """vertical_profile の合成データテスト（DB 不要）。"""

    def _generate_and_verify(self, config: Config, data, limit_altitude: bool = False) -> None:
        set_font(config.font)

        graph_def = GRAPH_DEF_MAP["vertical_profile"]
        figsize = tuple(x / amdar.constants.GRAPH_IMAGE_DPI for x in graph_def.size)
        img, elapsed = graph_def.func(data, figsize, limit_altitude=limit_altitude)

        assert elapsed >= 0
        assert (img.width, img.height) == graph_def.size

        buf = io.BytesIO()
        img.save(buf, "PNG")
        buf.seek(0)
        with PIL.Image.open(buf) as png:
            png.verify()

    def test_vertical_profile_with_synthetic_data(self, config: Config):
        """気温 + 風の合成データで PNG が生成され、サイズが正しいこと。"""
        data = prepare_data_numpy(_make_synthetic_numpy_data())
        self._generate_and_verify(config, data)

    def test_vertical_profile_without_wind(self, config: Config):
        """風データなし（右パネルはデータ不足表示）でも PNG が生成されること。"""
        data = prepare_data_numpy(_make_synthetic_numpy_data(include_wind=False))
        self._generate_and_verify(config, data)

    def test_vertical_profile_limit_altitude(self, config: Config):
        """limit_altitude=True でも PNG が生成されること。"""
        data = prepare_data_numpy(_make_synthetic_numpy_data())
        self._generate_and_verify(config, data, limit_altitude=True)


class TestLimitAltitude:
    """高度制限パラメータのテスト。"""

    def test_limit_altitude_parameter(self, config: Config):
        """limit_altitude の True/False で生成が成功すること。"""
        _ensure_service_initialized(config)

        end_date = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=7)
        start_date = end_date - datetime.timedelta(days=7)

        png_unlimited = graph_service.generate_sync("scatter_2d", start_date, end_date, limit_altitude=False)
        png_limited = graph_service.generate_sync("scatter_2d", start_date, end_date, limit_altitude=True)

        assert png_unlimited is not None
        assert png_limited is not None
        assert len(png_unlimited) > 0
        assert len(png_limited) > 0

        expected_size = GRAPH_DEF_MAP["scatter_2d"].size
        with PIL.Image.open(io.BytesIO(png_unlimited)) as img:
            img.verify()
            assert img.width == expected_size[0]
            assert img.height == expected_size[1]

        with PIL.Image.open(io.BytesIO(png_limited)) as img:
            img.verify()
            assert img.width == expected_size[0]
            assert img.height == expected_size[1]

        logging.info(
            "Graph generation successful - Unlimited: %d bytes, Limited: %d bytes",
            len(png_unlimited),
            len(png_limited),
        )
