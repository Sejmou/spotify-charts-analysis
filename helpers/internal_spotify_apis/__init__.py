from typing import Callable, Set
import time
from selenium import webdriver
from helpers.spotify_util import get_spotify_track_link
import json
import random
from selenium.webdriver.common.by import By
from helpers.scraping import (
    get_spotify_credentials,
    get_element_with_att_and_val,
    load_cookies,
    accept_cookies,
    login_and_accept_cookies,
)

from .credits import process_credits


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

    def __init__(
        self, resource_name: str, track_ids: Set[str], cookies_path: str = None
    ):
        """
        Args:
            resource_name: name of the internal API endpoint to get the request headers for.
            track_ids: list of Spotify track IDs to try when attemtping to get API request headers.
        """

        self.url_getter = internal_api_endpoints[resource_name]["url_getter"]
        credentials_required = internal_api_endpoints[resource_name]["requires_login"]
        if credentials_required:
            print(
                f"Warning: Spotify login required for getting request headers for '{resource_name}' endpoint"
            )
            if cookies_path is not None:
                print("Make sure provided cookies are for a logged-in Spotify account")
            print()

        self.driver = InternalRequestHeadersGetter.setup_webdriver(
            credentials_required=credentials_required,
            cookies_path=cookies_path,
        )
        self.request_trigger = (
            InternalRequestHeadersGetter._internal_request_trigger_fns[resource_name]
        )
        self.track_ids = track_ids

    @staticmethod
    def setup_webdriver(
        headless: bool = True,
        credentials_required: tuple = None,
        cookies_path: str = None,
    ):
        while True:
            try:
                options = webdriver.ChromeOptions()
                options.set_capability(
                    "goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"}
                )
                if headless:
                    options.add_argument("--headless=new")
                driver = webdriver.Chrome(options=options)
                if cookies_path is not None:
                    driver.get("https://open.spotify.com")
                    load_cookies(driver, cookies_path)
                    driver.refresh()
                    print(f"Loaded cookies from {cookies_path} successfully")
                else:
                    if credentials_required:
                        username, password = get_spotify_credentials()
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
    try:
        lyrics_button = get_element_with_att_and_val(
            driver, "data-testid", "lyrics-button"
        )
        lyrics_button.click()
    except Exception as e:
        print(f"Error opening lyrics view: {e}")
        print(
            "This might be because the webdriver is no longer logged in to Spotify. Try restarting if the script got stuck."
        )


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
