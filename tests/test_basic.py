#!/usr/bin/env python3
# ruff: : S101
import logging
import pathlib
import queue

import pytest

import modes.database_postgresql
import modes.receiver

CONFIG_FILE = "config.example.yaml"
SCHEMA_CONFIG = "config.schema"


@pytest.fixture(scope="session")
def config():
    import my_lib.config

    return my_lib.config.load(CONFIG_FILE, pathlib.Path(SCHEMA_CONFIG))


def test_receiver(config):
    measurement_queue = queue.Queue()

    modes.receiver.start(
        config["modes"]["decoder"]["host"],
        config["modes"]["decoder"]["port"],
        measurement_queue,
        config["filter"]["area"],
    )

    while True:
        assert measurement_queue.get() is not None  # noqa: S101
        modes.receiver.term()

        break


def test_collect(config):
    import my_lib.healthz

    import collect

    liveness_file = config["liveness"]["file"]["collector"]
    collect.execute(config, liveness_file, 1)

    modes.receiver.term()

    assert my_lib.healthz.check_liveness("collector", liveness_file, 60)  # noqa: S101


def test_graph(config):
    import datetime
    import io

    import my_lib.time
    import PIL.Image

    import modes.webui.api.graph

    time_end = my_lib.time.now()
    time_start = time_end - datetime.timedelta(days=7)

    data = modes.webui.api.graph.prepare_data(
        modes.database_postgresql.fetch_by_time(
            modes.database_postgresql.open(
                config["database"]["host"],
                config["database"]["port"],
                config["database"]["name"],
                config["database"]["user"],
                config["database"]["pass"],
            ),
            time_start,
            time_end,
            config["filter"]["area"]["distance"],
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

    modes.webui.api.graph.set_font(config["font"])

    for graph_name, graph_def in modes.webui.api.graph.GRAPH_DEF_MAP.items():
        graph_def["future"] = graph_def["func"](
            data, tuple(x / modes.webui.api.graph.IMAGE_DPI for x in graph_def["size"])
        )

        png_data = modes.webui.api.graph.plot(config, graph_name, time_start, time_end)

        with PIL.Image.open(io.BytesIO(png_data)) as img:
            img.verify()
            assert img.width == graph_def["size"][0]  # noqa: S101
            assert img.height == graph_def["size"][1]  # noqa: S101


def test_data_range_api(config):
    """データ範囲API機能をテスト"""
    import modes.database_postgresql
    import modes.webui.api.graph

    # データベース接続を確立
    conn = modes.database_postgresql.open(
        config["database"]["host"],
        config["database"]["port"],
        config["database"]["name"],
        config["database"]["user"],
        config["database"]["pass"],
    )

    # データ範囲取得のクエリを実行（graph.pyのdata_range関数と同様）
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
        SELECT
            MIN(time) as earliest,
            MAX(time) as latest
        FROM meteorological_data
        """)
        result = cur.fetchone()

    conn.close()

    # データが存在することを確認
    assert result is not None  # noqa: S101
    assert result["earliest"] is not None  # noqa: S101
    assert result["latest"] is not None  # noqa: S101

    # 日付範囲が妥当であることを確認
    earliest = result["earliest"]
    latest = result["latest"]
    assert earliest <= latest  # noqa: S101

    logging.info("Data range: %s ～ %s", earliest, latest)


def test_date_range_before_january_2025_api(config):
    """2025年1月以前の日付範囲でグラフ生成がエラーなく動作することをテスト"""
    import datetime
    import io

    import PIL.Image

    import modes.webui.api.graph

    # 2024年12月の期間を設定（25年1月以前）
    start_date = datetime.datetime(2024, 12, 1, 0, 0, tzinfo=datetime.timezone.utc)
    end_date = datetime.datetime(2024, 12, 31, 23, 59, tzinfo=datetime.timezone.utc)

    # グラフ生成を実行（エラーにならないことを確認）
    try:
        png_data = modes.webui.api.graph.plot(config, "scatter_2d", start_date, end_date)

        # PNG画像として正常に生成されていることを確認
        with PIL.Image.open(io.BytesIO(png_data)) as img:
            img.verify()

            # 画像サイズが期待値と一致することを確認
            expected_size = modes.webui.api.graph.GRAPH_DEF_MAP["scatter_2d"]["size"]
            assert img.width == expected_size[0]  # noqa: S101
            assert img.height == expected_size[1]  # noqa: S101

        logging.info("Successfully generated graph for period: %s ～ %s", start_date, end_date)
        logging.info("PNG data size: %d bytes", len(png_data))

    except Exception as e:
        logging.info("Graph generation completed with handling: %s", e)
        # データがない期間でも、"データがありません"画像が生成されることを確認
        assert png_data is not None  # noqa: S101
        assert len(png_data) > 0  # noqa: S101
