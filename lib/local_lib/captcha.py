#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pathlib
import tempfile
import time
import urllib

import logging
import pydub
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from speech_recognition import AudioFile, Recognizer

import local_lib.selenium_util
import local_lib.notify_mail


DATA_PATH = pathlib.Path(os.path.dirname(__file__)).parent / "data"
LOG_PATH = DATA_PATH / "log"

CHROME_DATA_PATH = str(DATA_PATH / "chrome")
RECORD_PATH = str(DATA_PATH / "record")
DUMP_PATH = str(DATA_PATH / "debug")


def recog_audio(audio_url):
    mp3_file = tempfile.NamedTemporaryFile(mode="wb", delete=False)
    wav_file = tempfile.NamedTemporaryFile(mode="wb", delete=False)

    try:
        urllib.request.urlretrieve(audio_url, mp3_file.name)

        pydub.AudioSegment.from_mp3(mp3_file.name).export(wav_file.name, format="wav")

        recognizer = Recognizer()
        recaptcha_audio = AudioFile(wav_file.name)
        with recaptcha_audio as source:
            audio = recognizer.record(source)

        return recognizer.recognize_google(audio, language="en-US")
    except:
        raise
    finally:
        os.unlink(mp3_file.name)
        os.unlink(wav_file.name)


def resolve_mp3(driver, wait):
    wait.until(
        EC.frame_to_be_available_and_switch_to_it((By.XPATH, '//iframe[contains(@title,"reCAPTCHA")]'))
    )
    local_lib.selenium_util.click_xpath(
        driver, '//span[contains(@class, "recaptcha-checkbox")]', is_warn=True
    )
    time.sleep(0.5)

    driver.switch_to.default_content()

    wait.until(
        EC.frame_to_be_available_and_switch_to_it((By.XPATH, '//iframe[contains(@title, "reCAPTCHA による確認")]'))
    )

    wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@id="rc-imageselect"]')))

    local_lib.selenium_util.click_xpath(driver, '//button[@id="recaptcha-audio-button"]', is_warn=True)
    time.sleep(0.5)

    if local_lib.selenium_util.xpath_exists(
        driver, '//div[contains(@class, "rc-doscaptcha-header-text") and contains(text(), "しばらくしてから")]'
    ):
        logging.warning("Could not switch to autio authentication because it was assumed to be a bot.")
        return False

    audio_url = driver.find_element(By.XPATH, '//audio[@id="audio-source"]').get_attribute("src")

    text = recog_audio(audio_url)

    input_elem = driver.find_element(By.XPATH, '//input[@id="audio-response"]')
    input_elem.send_keys(text.lower())
    input_elem.send_keys(Keys.ENTER)

    driver.switch_to.default_content()

    return True


def resolve_img_console(driver, wait, captcha_img_path):
    wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, '//iframe[@title="reCAPTCHA"]')))
    local_lib.selenium_util.click_xpath(driver, '//span[contains(@class, "recaptcha-checkbox")]')
    driver.switch_to.default_content()
    wait.until(
        EC.frame_to_be_available_and_switch_to_it((By.XPATH, '//iframe[contains(@title, "reCAPTCHA による確認")]'))
    )
    wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@id="rc-imageselect-target"]')))
    while True:
        captcha_png_data = driver.find_element(By.XPATH, "//body").screenshot_as_png

        logging.info("Save image: {path}".format(path=captcha_img_path))

        with open(captcha_img_path, "wb") as f:
            f.write(captcha_png_data)

        tile_list = driver.find_elements(
            By.XPATH,
            '//table[contains(@class, "rc-imageselect-table")]//td[@role="button"]',
        )
        tile_idx_list = list(map(lambda elem: elem.get_attribute("tabindex"), tile_list))

        # NOTE: メールを見て人間に選択するべき画像のインデックスを入力してもらう．
        # インデックスは左上を 1 として横方向に 1, 2, ... とする形．
        # 入力を簡単にするため，10以上は a, b, ..., g で指定．
        # 0 は入力の完了を意味する．
        select_str = input(
            (
                "「{img_file}」を参照して，選択すべきタイルを指定してください．\n".format(img_file=captcha_img_path)
                + "(左上を 1 として横方向に 1, 2, 3, ... として指定．0 は追加選択無し．): "
            )
        ).strip()

        if select_str == "0":
            if local_lib.selenium_util.click_xpath(
                driver, '//button[contains(text(), "スキップ")]', is_warn=False
            ):
                time.sleep(0.5)
                continue
            elif local_lib.selenium_util.click_xpath(
                driver, '//button[contains(text(), "確認")]', is_warn=False
            ):
                time.sleep(0.5)

                if local_lib.selenium_util.is_display(
                    driver, '//div[contains(text(), "新しい画像も")]'
                ) or local_lib.selenium_util.is_display(driver, '//div[contains(text(), "もう一度")]'):
                    continue
                else:
                    break
            else:
                local_lib.selenium_util.click_xpath(driver, '//button[contains(text(), "次へ")]', is_warn=False)
                time.sleep(0.5)
                continue

        for idx_char in list(select_str):
            if ord(idx_char) <= 57:
                idx = ord(idx_char) - 48
            else:
                idx = ord(idx_char) - 97 + 10

            if idx > len(tile_idx_list):
                continue

            logging.info("select {index}".format(index=idx))
            local_lib.selenium_util.click_xpath(
                driver,
                '//table[contains(@class, "rc-imageselect-table")]//td[@tabindex="{index}"]'.format(
                    index=tile_idx_list[idx - 1]
                ),
            )
        time.sleep(1)

    driver.switch_to.default_content()


def resolve_img_mail(driver, wait, config):
    wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, '//iframe[@title="reCAPTCHA"]')))
    local_lib.selenium_util.click_xpath(driver, '//span[contains(@class, "recaptcha-checkbox")]')
    driver.switch_to.default_content()
    wait.until(
        EC.frame_to_be_available_and_switch_to_it((By.XPATH, '//iframe[contains(@title, "reCAPTCHA による確認")]'))
    )
    wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@id="rc-imageselect-target"]')))
    while True:
        # NOTE: 問題画像を切り抜いてメールで送信
        local_lib.notify_mail.send(
            config,
            "reCAPTCHA",
            png_data=driver.find_element(By.XPATH, "//body").screenshot_as_png,
            is_force=True,
        )
        tile_list = driver.find_elements(
            By.XPATH,
            '//table[contains(@class, "rc-imageselect-table")]//td[@role="button"]',
        )
        tile_idx_list = list(map(lambda elem: elem.get_attribute("tabindex"), tile_list))

        # NOTE: メールを見て人間に選択するべき画像のインデックスを入力してもらう．
        # インデックスは左上を 1 として横方向に 1, 2, ... とする形．
        # 入力を簡単にするため，10以上は a, b, ..., g で指定．
        # 0 は入力の完了を意味する．
        select_str = input("選択タイル(1-9,a-g,end=0): ").strip()

        if select_str == "0":
            if local_lib.selenium_util.click_xpath(
                driver, '//button[contains(text(), "スキップ")]', is_warn=False
            ):
                time.sleep(0.5)
                continue
            elif local_lib.selenium_util.click_xpath(
                driver, '//button[contains(text(), "確認")]', is_warn=False
            ):
                time.sleep(0.5)

                if local_lib.selenium_util.is_display(
                    driver, '//div[contains(text(), "新しい画像も")]'
                ) or local_lib.selenium_util.is_display(driver, '//div[contains(text(), "もう一度")]'):
                    continue
                else:
                    break
            else:
                local_lib.selenium_util.click_xpath(driver, '//button[contains(text(), "次へ")]', is_warn=False)
                time.sleep(0.5)
                continue

        for idx in list(select_str):
            if ord(idx) <= 57:
                idx = ord(idx) - 48
            else:
                idx = ord(idx) - 97 + 10
            if idx >= len(tile_idx_list):
                continue

            local_lib.selenium_util.click_xpath(
                driver,
                '//table[contains(@class, "rc-imageselect-table")]//td[@tabindex="{index}"]'.format(
                    index=tile_idx_list[idx - 1]
                ),
            )
        time.sleep(0.5)

    driver.switch_to.default_content()
