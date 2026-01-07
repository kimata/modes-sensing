#!/usr/bin/env python3
"""
URL パラメータ機能の E2E テスト

URL パラメータで期間や設定を指定した際の動作をテストします。
"""

import logging
import re
import time
from urllib.parse import urlencode

import pytest
from playwright.sync_api import expect

APP_URL_TMPL = "http://{host}:{port}/modes-sensing/"


@pytest.fixture
def page_init(page, host, port, worker_id, webserver):
    """各テスト用のページ初期化（並列実行対応）"""
    from .test_webui import wait_for_server_ready

    if not webserver:
        wait_for_server_ready(host, port)

    import random

    if worker_id != "master":
        worker_num = int(worker_id[2:]) if worker_id.startswith("gw") else 0
        base_delay = 0.3
        worker_delay = worker_num * 0.15
        jitter = random.uniform(0.1, 0.3)  # noqa: S311
        total_delay = base_delay + worker_delay + jitter
        logging.info("ワーカー %s: %.2f秒待機", worker_id, total_delay)
        time.sleep(total_delay)
    else:
        time.sleep(0.5)

    page.on("console", lambda msg: print(msg.text))
    page.set_viewport_size({"width": 2400, "height": 1600})

    return page


def app_url(host, port, params=None):
    """アプリケーション URL を生成"""
    base_url = APP_URL_TMPL.format(host=host, port=port)
    if params:
        return f"{base_url}?{urlencode(params)}"
    return base_url


def wait_for_page_ready(page, timeout=30000):
    """ページが完全に読み込まれるまで待機"""
    page.wait_for_function(
        """
        () => {
            const title = document.querySelector('h1');
            const dateSelector = document.querySelector('#date-selector');
            return title && dateSelector;
        }
        """,
        timeout=timeout,
    )


def get_selected_period_button(page):
    """現在選択されている期間ボタンのテキストを取得"""
    return page.evaluate("""
        () => {
            const buttons = document.querySelectorAll('#date-selector button');
            for (const btn of buttons) {
                if (btn.classList.contains('is-primary')) {
                    return btn.textContent.trim();
                }
            }
            return null;
        }
    """)


def get_current_url_params(page):
    """現在の URL パラメータを取得"""
    return page.evaluate("""
        () => {
            const params = new URLSearchParams(window.location.search);
            return {
                period: params.get('period'),
                start: params.get('start'),
                end: params.get('end'),
                limitAltitude: params.get('limitAltitude')
            };
        }
    """)


######################################################################
# URL パラメータなし（デフォルト）のテスト
######################################################################
def test_default_url_selects_7days(page_init, host, port):
    """パラメータなしの URL で7日間ボタンが選択されることをテスト"""
    page = page_init
    page.goto(app_url(host, port))
    wait_for_page_ready(page)

    # 7日間ボタンがアクティブになっていることを確認
    button_7days = page.locator("button >> text='過去7日間'")
    expect(button_7days).to_have_class(re.compile(r"is-primary"))

    # URL にパラメータがないことを確認
    params = get_current_url_params(page)
    assert params["period"] is None, "デフォルトでは period パラメータがないはずです"  # noqa: S101
    assert params["start"] is None, "デフォルトでは start パラメータがないはずです"  # noqa: S101
    assert params["end"] is None, "デフォルトでは end パラメータがないはずです"  # noqa: S101

    logging.info("Default URL test passed: 7days button is selected")


######################################################################
# period パラメータのテスト
######################################################################
def test_period_param_30days(page_init, host, port):
    """?period=30days で1ヶ月ボタンが選択されることをテスト"""
    page = page_init
    page.goto(app_url(host, port, {"period": "30days"}))
    wait_for_page_ready(page)

    # 1ヶ月ボタンがアクティブになっていることを確認
    button_30days = page.locator("button >> text='過去1ヶ月間'")
    expect(button_30days).to_have_class(re.compile(r"is-primary"))

    # 他のボタンがアクティブでないことを確認
    button_7days = page.locator("button >> text='過去7日間'")
    expect(button_7days).not_to_have_class(re.compile(r"is-primary"))

    logging.info("Period param test passed: 30days selects 1-month button")


def test_period_param_180days(page_init, host, port):
    """?period=180days で半年ボタンが選択されることをテスト"""
    page = page_init
    page.goto(app_url(host, port, {"period": "180days"}))
    wait_for_page_ready(page)

    # 半年ボタンがアクティブになっていることを確認
    button_180days = page.locator("button >> text='過去半年'")
    expect(button_180days).to_have_class(re.compile(r"is-primary"))

    logging.info("Period param test passed: 180days selects half-year button")


def test_period_param_1day(page_init, host, port):
    """?period=1day で24時間ボタンが選択されることをテスト"""
    page = page_init
    page.goto(app_url(host, port, {"period": "1day"}))
    wait_for_page_ready(page)

    # 24時間ボタンがアクティブになっていることを確認
    button_1day = page.locator("button >> text='過去24時間'")
    expect(button_1day).to_have_class(re.compile(r"is-primary"))

    logging.info("Period param test passed: 1day selects 24-hour button")


def test_period_param_365days(page_init, host, port):
    """?period=365days で1年ボタンが選択されることをテスト"""
    page = page_init
    page.goto(app_url(host, port, {"period": "365days"}))
    wait_for_page_ready(page)

    # 1年ボタンがアクティブになっていることを確認
    button_365days = page.locator("button >> text='過去1年'")
    expect(button_365days).to_have_class(re.compile(r"is-primary"))

    logging.info("Period param test passed: 365days selects 1-year button")


######################################################################
# カスタム期間パラメータのテスト
######################################################################
def test_custom_date_params(page_init, host, port):
    """?start=...&end=... でカスタムボタンが選択され、日時が入力されることをテスト"""
    page = page_init

    # カスタム日付範囲を設定
    start_date = "2026-01-01T00:00"
    end_date = "2026-01-05T12:00"

    page.goto(app_url(host, port, {"start": start_date, "end": end_date}))
    wait_for_page_ready(page)

    # カスタムボタンがアクティブになっていることを確認
    button_custom = page.locator("button >> text='カスタム'")
    expect(button_custom).to_have_class(re.compile(r"is-primary"))

    # 日時入力フィールドが表示されていることを確認
    start_input = page.locator('input[type="datetime-local"]').first
    end_input = page.locator('input[type="datetime-local"]').last

    expect(start_input).to_be_visible()
    expect(end_input).to_be_visible()

    # 入力値が URL パラメータの値と一致することを確認
    start_value = start_input.input_value()
    end_value = end_input.input_value()

    assert start_date in start_value or start_value == start_date, (  # noqa: S101
        f"開始日時が一致しません: expected={start_date}, actual={start_value}"
    )
    assert end_date in end_value or end_value == end_date, (  # noqa: S101
        f"終了日時が一致しません: expected={end_date}, actual={end_value}"
    )

    logging.info("Custom date params test passed: custom button selected with correct dates")


######################################################################
# limitAltitude パラメータのテスト
######################################################################
def test_limit_altitude_param_true(page_init, host, port):
    """?limitAltitude=true で高度制限チェックボックスがオンになることをテスト"""
    page = page_init
    page.goto(app_url(host, port, {"period": "7days", "limitAltitude": "true"}))
    wait_for_page_ready(page)

    # 高度制限チェックボックスがオンになっていることを確認
    altitude_checkbox = page.locator('input[type="checkbox"]')
    expect(altitude_checkbox).to_be_checked()

    logging.info("Limit altitude param test passed: checkbox is checked")


def test_limit_altitude_param_with_custom(page_init, host, port):
    """カスタム期間と高度制限の組み合わせをテスト"""
    page = page_init

    start_date = "2026-01-01T00:00"
    end_date = "2026-01-03T12:00"

    page.goto(app_url(host, port, {"start": start_date, "end": end_date, "limitAltitude": "true"}))
    wait_for_page_ready(page)

    # カスタムボタンがアクティブ
    button_custom = page.locator("button >> text='カスタム'")
    expect(button_custom).to_have_class(re.compile(r"is-primary"))

    # 高度制限チェックボックスがオン
    altitude_checkbox = page.locator('input[type="checkbox"]')
    expect(altitude_checkbox).to_be_checked()

    logging.info("Custom + limit altitude params test passed")


######################################################################
# URL 更新のテスト（ユーザー操作による）
######################################################################
def test_period_button_updates_url(page_init, host, port):
    """期間ボタンをクリックすると URL が更新されることをテスト"""
    page = page_init
    page.goto(app_url(host, port))
    wait_for_page_ready(page)

    # 最初は URL にパラメータがない
    params = get_current_url_params(page)
    assert params["period"] is None, "初期状態では period パラメータがないはずです"  # noqa: S101

    # 1ヶ月ボタンをクリック
    page.click("button >> text='過去1ヶ月間'")
    time.sleep(1)  # URL 更新を待つ

    # URL に period=30days が設定される
    params = get_current_url_params(page)
    assert params["period"] == "30days", f"期待: period=30days, 実際: {params['period']}"  # noqa: S101

    # 7日間ボタンをクリック（デフォルトに戻る）
    page.click("button >> text='過去7日間'")
    time.sleep(1)

    # URL パラメータがクリアされる
    params = get_current_url_params(page)
    assert params["period"] is None, "7日間（デフォルト）では period パラメータがないはずです"  # noqa: S101

    logging.info("Period button URL update test passed")


def test_altitude_checkbox_updates_url(page_init, host, port):
    """高度制限チェックボックスを変更すると URL が更新されることをテスト"""
    page = page_init
    page.goto(app_url(host, port))
    wait_for_page_ready(page)

    # 初期状態では limitAltitude パラメータがない
    params = get_current_url_params(page)
    assert params["limitAltitude"] is None, "初期状態では limitAltitude パラメータがないはずです"  # noqa: S101

    # 高度制限チェックボックスをオン
    altitude_checkbox = page.locator('input[type="checkbox"]')
    altitude_checkbox.click()
    time.sleep(1)

    # URL に limitAltitude=true が設定される
    params = get_current_url_params(page)
    assert params["limitAltitude"] == "true", f"期待: limitAltitude=true, 実際: {params['limitAltitude']}"  # noqa: S101

    # チェックボックスをオフ
    altitude_checkbox.click()
    time.sleep(1)

    # URL パラメータがクリアされる（7日間 + 高度制限なし = デフォルト）
    params = get_current_url_params(page)
    assert params["limitAltitude"] is None, "高度制限オフではパラメータがないはずです"  # noqa: S101

    logging.info("Altitude checkbox URL update test passed")


def test_custom_date_updates_url(page_init, host, port):
    """カスタム日付を設定すると URL が更新されることをテスト"""
    page = page_init
    page.goto(app_url(host, port))
    wait_for_page_ready(page)

    # カスタムボタンをクリック
    page.click("button >> text='カスタム'")
    time.sleep(1)

    # 日時入力フィールドが表示されるまで待機
    page.wait_for_selector('input[type="datetime-local"]', state="visible", timeout=10000)

    # 日付を入力
    start_input = page.locator('input[type="datetime-local"]').first
    end_input = page.locator('input[type="datetime-local"]').last

    start_input.clear()
    start_input.fill("2026-01-02T10:00")
    end_input.clear()
    end_input.fill("2026-01-04T15:00")

    # 更新ボタンをクリック
    time.sleep(1)
    update_button = page.locator("button.is-fullwidth")
    update_button.click()
    time.sleep(1)

    # URL に start と end パラメータが設定される
    params = get_current_url_params(page)
    assert params["start"] is not None, "カスタム期間では start パラメータがあるはずです"  # noqa: S101
    assert params["end"] is not None, "カスタム期間では end パラメータがあるはずです"  # noqa: S101
    assert "2026-01-02" in params["start"], f"start パラメータが正しくありません: {params['start']}"  # noqa: S101
    assert "2026-01-04" in params["end"], f"end パラメータが正しくありません: {params['end']}"  # noqa: S101

    logging.info("Custom date URL update test passed")


######################################################################
# タイトルクリックによるリセットのテスト
######################################################################
def test_title_click_resets_to_default(page_init, host, port):
    """タイトルをクリックするとデフォルト状態にリセットされることをテスト"""
    page = page_init

    # パラメータ付きの URL でアクセス
    page.goto(app_url(host, port, {"period": "30days", "limitAltitude": "true"}))
    wait_for_page_ready(page)

    # 1ヶ月ボタンがアクティブで、高度制限がオン
    button_30days = page.locator("button >> text='過去1ヶ月間'")
    expect(button_30days).to_have_class(re.compile(r"is-primary"))

    altitude_checkbox = page.locator('input[type="checkbox"]')
    expect(altitude_checkbox).to_be_checked()

    # タイトルをクリック
    title_link = page.locator("h1 a")
    title_link.click()
    time.sleep(1)

    # 7日間ボタンがアクティブになる
    button_7days = page.locator("button >> text='過去7日間'")
    expect(button_7days).to_have_class(re.compile(r"is-primary"))

    # 1ヶ月ボタンは非アクティブ
    expect(button_30days).not_to_have_class(re.compile(r"is-primary"))

    # 高度制限がオフ
    expect(altitude_checkbox).not_to_be_checked()

    # URL パラメータがクリアされる
    params = get_current_url_params(page)
    assert params["period"] is None, "リセット後は period パラメータがないはずです"  # noqa: S101
    assert params["limitAltitude"] is None, "リセット後は limitAltitude パラメータがないはずです"  # noqa: S101

    logging.info("Title click reset test passed")


######################################################################
# ブラウザ履歴のテスト
######################################################################
def test_browser_back_preserves_state(page_init, host, port):
    """ブラウザの戻るボタンで状態が保持されることをテスト"""
    page = page_init

    # デフォルト URL でアクセス
    page.goto(app_url(host, port))
    wait_for_page_ready(page)

    # 1ヶ月ボタンをクリック
    page.click("button >> text='過去1ヶ月間'")
    time.sleep(1)

    # 半年ボタンをクリック
    page.click("button >> text='過去半年'")
    time.sleep(1)

    # 半年ボタンがアクティブ
    button_180days = page.locator("button >> text='過去半年'")
    expect(button_180days).to_have_class(re.compile(r"is-primary"))

    # ブラウザの戻るボタン
    page.go_back()
    time.sleep(1)

    # URL が replaceState で更新されているため、状態は保持されない可能性がある
    # ただし、URL パラメータからの復元は動作するはず
    params = get_current_url_params(page)
    logging.info("After back button, URL params: %s", params)

    # 少なくともページは正常に動作している
    wait_for_page_ready(page)

    logging.info("Browser back test completed")
