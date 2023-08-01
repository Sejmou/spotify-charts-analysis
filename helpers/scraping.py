from selenium import webdriver
from selenium.webdriver.common.by import By
import os
from dotenv import load_dotenv
from selenium.webdriver.support.ui import WebDriverWait
import selenium.webdriver.support.expected_conditions as EC
import time
from urllib.parse import quote
import inquirer
from datetime import datetime
import json
import random
from helpers.spotify_util import get_spotify_track_link
from typing import Callable, Set
import pickle

login_page_url = "https://accounts.spotify.com/en/login"


def login_and_accept_cookies(
    driver: webdriver,
    username,
    password,
    after_login_url="https://charts.spotify.com/charts/overview/global",
):
    fill_and_submit_login_form(driver, username, password, after_login_url)

    wait = WebDriverWait(driver, 5)
    wait.until(EC.url_contains(after_login_url))

    accept_cookies(driver)


def accept_cookies(driver: webdriver):
    wait = WebDriverWait(driver, 15)
    cookie_button = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "#onetrust-accept-btn-handler"))
    )
    time.sleep(
        1
    )  # apparently, sometimes clicking immediately after the button is clickable doesn't register the click properly, so wait a while
    cookie_button.click()


def fill_and_submit_login_form(
    driver: webdriver,
    username,
    password,
    after_login_url,
):
    driver.get(login_page_url + f"?continue={quote(after_login_url)}")

    # enter credentials on login page
    username_input = driver.find_element(By.CSS_SELECTOR, "#login-username")
    username_input.clear()
    username_input.send_keys(username)

    password_input = driver.find_element(By.CSS_SELECTOR, "#login-password")
    password_input.clear()
    password_input.send_keys(password)
    time.sleep(1)

    login_btn = driver.find_element(By.CSS_SELECTOR, "#login-button")
    login_btn.click()


def get_spotify_credentials():
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(env_path)
    username = os.environ.get("SPOTIFY_USERNAME")
    password = os.environ.get("SPOTIFY_PASSWORD")

    if username is None or password is None:
        questions = []

        if username is None:
            questions.append(
                inquirer.Text("username", message="Enter your Spotify username:")
            )

        if password is None:
            questions.append(
                inquirer.Password("password", message="Enter your Spotify password:")
            )

        answers = inquirer.prompt(questions)
        if username is None:
            username = answers["username"]
        if password is None:
            password = answers["password"]

    return username, password


def save_debug_screenshot(driver: webdriver, dirpath: str, worker_id: str, desc: str):
    driver.save_screenshot(
        os.path.join(
            dirpath,
            f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_{worker_id}_{desc}.png",
        )
    )


def get_element_with_att_and_val(driver, att, val):
    wait = WebDriverWait(driver, 5)
    element = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, f'[{att}="{val}"]'))
    )
    return element


def load_cookies(driver: webdriver, cookies_path: str):
    with open(cookies_path, "rb") as f:
        cookies = pickle.load(f)

    for cookie in cookies:
        driver.add_cookie(cookie)
