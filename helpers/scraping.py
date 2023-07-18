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
from typing import List
from helpers.spotify_util import get_spotify_track_link
from typing import Callable, Set

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


# ========== stuff related to internal API requests ==============


INTERNAL_API_BASE_URL = "https://spclient.wg.spotify.com/"


def get_credits_api_url(track_id: str):
    return (
        f"{INTERNAL_API_BASE_URL}track-credits-view/v0/experimental/{track_id}/credits"
    )


def get_lyrics_api_url(track_id: str):
    return f"{INTERNAL_API_BASE_URL}color-lyrics/v2/track/{track_id}"


internal_api_endpoints = {
    "credits": {
        "url_getter": get_credits_api_url,
        "requires_login": False,
    },
    "lyrics": {
        "url_getter": get_lyrics_api_url,
        "requires_login": True,
    },
}


class InternalRequestHeadersGetter:
    """
    Class for getting the request headers for request to a given internal Spotify API.
    Supports the internal API endpoints mentioned in the `internal_api_endpoints` dict.

    The request headers are needed to make requests to the internal API endpoints.

    The request headers are obtained by using a webdriver to visit the Spotify web app and then
    triggering a request to the internal API endpoint. The request headers are then extracted from the
    webdriver's logs.
    """

    def __init__(self, resource_name: str, track_ids: Set[str]):
        """
        Args:
            resource_name: name of the internal API endpoint to get the request headers for.
            track_ids: list of Spotify track IDs to try when attemtping to get API request headers.
        """

        self.url_getter = internal_api_endpoints[resource_name]["url_getter"]
        credentials_required = internal_api_endpoints[resource_name]["requires_login"]
        if credentials_required:
            print(
                f"Spotify login required for getting request headers for '{resource_name}' endpoint"
            )
            credentials = get_spotify_credentials()
        self.driver = InternalRequestHeadersGetter.setup_webdriver(
            credentials=credentials
        )
        self.request_trigger = (
            InternalRequestHeadersGetter._internal_request_trigger_fns[resource_name]
        )
        self.track_ids = track_ids

    @staticmethod
    def setup_webdriver(headless: bool = True, credentials: tuple = None):
        while True:
            try:
                options = webdriver.ChromeOptions()
                options.set_capability(
                    "goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"}
                )
                if headless:
                    options.add_argument("--headless=new")
                driver = webdriver.Chrome(options=options)

                if credentials is not None:
                    username, password = credentials
                    login_and_accept_cookies(
                        driver=driver,
                        username=username,
                        password=password,
                    )
                    print("Logged in successfully")
                else:
                    driver.get("https://open.spotify.com")
                    accept_cookies(driver)

                return driver
            except Exception as e:
                print(
                    f"Error starting webdriver for fetching internal API request headers: {e}"
                )
                print("Retrying...")
                time.sleep(
                    1
                )  # not sure if this is necessary or would ever change anything lol

    def get_headers(self):
        """
        Gets the headers for making requests to the internal Spotify API for the resource which the RequestHeaderGetter was initialized with.
        """

        headers = None
        while headers is None:
            # This may sometimes fail, not entirely sure why, but it seems to always work if we just retry once or twice with different track IDs.
            try:
                track_id = random.choice(list(self.track_ids))
                track_page_url = get_spotify_track_link(track_id)
                self.driver.get(track_page_url)
                self.request_trigger(self.driver)

                log_entries = self.driver.get_log("performance")
                for entry in log_entries:
                    entry["message"] = json.loads(entry["message"])["message"]

                last_internal_request_log_entry = next(
                    e
                    for e in reversed(log_entries)
                    if _is_internal_api_request(
                        e, track_id=track_id, url_getter=self.url_getter
                    )
                )
                headers = last_internal_request_log_entry["message"]["params"][
                    "request"
                ]["headers"]
            except Exception as e:
                print(f"Error getting internal API request headers: {e}. Retrying...")

        return headers

    _internal_request_trigger_fns = {
        "credits": lambda driver: _open_credits_popup(driver),
        "lyrics": lambda driver: _open_lyrics_view(driver),
    }

    def __del__(self):
        if hasattr(self, "driver") and self.driver is not None:
            self.driver.quit()


def _open_credits_popup(driver: webdriver.Chrome):
    more_button = get_element_with_att_and_val(driver, "data-testid", "more-button")
    more_button.click()
    credits_button = driver.find_element(
        By.XPATH,
        f"//div[@id='context-menu']//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'credits')]",
    )
    credits_button.click()


def _open_lyrics_view(driver: webdriver.Chrome):
    lyrics_button = get_element_with_att_and_val(driver, "data-testid", "lyrics-button")
    lyrics_button.click()


def _is_internal_api_request(
    chromedriver_network_log_entry, track_id: str, url_getter: Callable[[str], str]
):
    """
    Detects whether a log entry is a request to the internal Spotify API. We can use this request's headers to make our own requests to the API.
    """
    return (
        chromedriver_network_log_entry["message"]["method"]
        == "Network.requestWillBeSent"
        and chromedriver_network_log_entry["message"]["params"]["request"][
            "url"
        ].startswith(url_getter(track_id))
        and chromedriver_network_log_entry["message"]["params"][
            "documentURL"
        ].startswith("https://open.spotify.com/track/")
    )
