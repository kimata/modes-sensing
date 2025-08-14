#!/usr/bin/env python3
import contextlib
import logging
import os
import pathlib
import signal
import subprocess
import time

import pytest
import requests


def pytest_addoption(parser):
    parser.addoption("--host", default="127.0.0.1")
    parser.addoption("--port", default="5000")
    parser.addoption(
        "--start-server",
        action="store_true",
        default=False,
        help="Start the web server automatically for Playwright tests",
    )


@pytest.fixture
def host(request):
    return request.config.getoption("--host")


@pytest.fixture
def port(request):
    return request.config.getoption("--port")


@pytest.fixture(scope="session")
def webserver(request):
    """Start the web server for Playwright tests if --start-server option is provided."""
    if not request.config.getoption("--start-server"):
        yield None
        return

    host = request.config.getoption("--host")
    port = request.config.getoption("--port")

    # Change to project root directory
    project_root = pathlib.Path(__file__).parent.parent
    os.chdir(project_root)

    # Start the server process in debug mode for testing
    env = os.environ.copy()
    env["TEST"] = "true"

    # Ensure PYTHONPATH includes the src directory
    current_pythonpath = env.get("PYTHONPATH", "")
    src_path = str(project_root / "src")
    if current_pythonpath:
        env["PYTHONPATH"] = f"{src_path}:{current_pythonpath}"
    else:
        env["PYTHONPATH"] = src_path

    server_process = subprocess.Popen(  # noqa: S603
        ["uv", "run", "python", "src/webui.py", "-c", "config.example.yaml", "-p", str(port), "-D"],  # noqa: S607
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        # Create new process group for proper cleanup
        preexec_fn=os.setsid,  # noqa: PLW1509
    )

    # Wait for server to start
    app_url = f"http://{host}:{port}/modes-sensing/"
    timeout_sec = 60
    start_time = time.time()

    while time.time() - start_time < timeout_sec:
        try:
            response = requests.get(app_url, timeout=5)
            if response.ok:
                logging.info("Server started successfully at %s", app_url)
                break
        except requests.exceptions.RequestException:
            pass
        time.sleep(2)
    else:
        # Server failed to start, terminate process and get logs
        server_process.terminate()
        stdout, stderr = server_process.communicate(timeout=5)
        error_msg = (
            f"Server failed to start within {timeout_sec} seconds.\nStdout: {stdout}\nStderr: {stderr}"
        )
        raise RuntimeError(error_msg)

    yield server_process

    # Cleanup: gracefully terminate the entire process group
    # Based on rasp-shutter implementation for more reliable shutdown
    try:
        current_pid = server_process.pid
        pgid = os.getpgid(current_pid)

        # Send SIGTERM to the entire process group (including Flask reloader children)
        logging.info("Terminating server process group %d", pgid)
        os.killpg(pgid, signal.SIGTERM)

        # Wait for graceful shutdown
        server_process.wait(timeout=10)
        logging.info("Server shutdown completed gracefully")
    except (subprocess.TimeoutExpired, ProcessLookupError, PermissionError):
        # If graceful shutdown fails or process already gone, try force kill
        logging.warning("Graceful shutdown failed, attempting force kill")
        with contextlib.suppress(ProcessLookupError, PermissionError):
            pgid = os.getpgid(server_process.pid)
            os.killpg(pgid, signal.SIGKILL)
        # Ensure subprocess handle is cleaned up
        with contextlib.suppress(subprocess.TimeoutExpired):
            server_process.wait(timeout=5)


@pytest.fixture
def page(page):
    from playwright.sync_api import expect

    timeout = 90000  # CI環境対応で90秒に延長
    page.set_default_navigation_timeout(timeout)
    page.set_default_timeout(timeout)
    expect.set_options(timeout=timeout)

    return page


@pytest.fixture
def browser_context_args(browser_context_args, request, worker_id):
    # 並列実行時は各ワーカーに独立したコンテキストを設定
    args = {
        **browser_context_args,
        "record_video_dir": f"tests/evidence/{request.node.name}",
        "record_video_size": {"width": 2400, "height": 1600},
    }

    # 並列実行時はキャッシュを無効化
    if worker_id != "master":
        args["bypass_csp"] = True
        args["ignore_https_errors"] = True

    return args
