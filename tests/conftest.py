#!/usr/bin/env python3
import pytest


def pytest_addoption(parser):
    parser.addoption("--host", default="127.0.0.1")
    parser.addoption("--port", default="5000")


@pytest.fixture
def host(request):
    return request.config.getoption("--host")


@pytest.fixture
def port(request):
    return request.config.getoption("--port")


@pytest.fixture
def page(playwright):
    from playwright.sync_api import expect

    browser = playwright.chromium.launch()
    context = browser.new_context()
    page = context.new_page()

    timeout = 90000  # CI環境対応で90秒に延長
    page.set_default_navigation_timeout(timeout)
    page.set_default_timeout(timeout)
    expect.set_options(timeout=timeout)

    yield page
    context.close()
    browser.close()


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
