#!/usr/bin/env python3
# ruff: : S101
import pathlib
import queue

import pytest

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
