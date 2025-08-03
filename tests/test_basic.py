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
