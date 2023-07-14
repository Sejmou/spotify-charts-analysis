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


# To use this, you will need the headers from a request to the internal Spotify API. You can get these by using the `get_internal_api_request_headers`.
# Discovered by checking the "Network" tab after clicking the "Credits" button on a track page.
INTERNAL_CREDITS_API_BASE_URL = (
    "https://spclient.wg.spotify.com/track-credits-view/v0/experimental/"
)


def get_credits_api_url(track_id: str):
    return f"{INTERNAL_CREDITS_API_BASE_URL}{track_id}/credits"


# Too lazy to put this into separate file, would probably make more sense
def get_internal_api_request_headers(track_id: str):
    def setup_webdriver(headless: bool = True):
        while True:
            try:
                options = webdriver.ChromeOptions()
                options.set_capability(
                    "goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"}
                )
                if headless:
                    options.add_argument("--headless=new")
                driver = webdriver.Chrome(options=options)

                track_page_url = f"https://open.spotify.com/track/{track_id}"
                driver.get(track_page_url)

                accept_cookies(driver)

                return driver
            except Exception as e:
                print(
                    f"Error starting webdriver for fetching internal API request headers: {e}"
                )
                print("Retrying in 5 seconds...")
                time.sleep(5)

    def open_credits_popup(driver: webdriver.Firefox):
        more_button = get_element_with_att_and_val(driver, "data-testid", "more-button")
        more_button.click()
        credits_button = driver.find_element(
            By.XPATH,
            f"//div[@id='context-menu']//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'credits')]",
        )
        credits_button.click()

    def get_element_with_att_and_val(driver, att, val):
        wait = WebDriverWait(driver, 5)
        element = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, f'[{att}="{val}"]'))
        )
        return element

    def write_to_json(data, filename):
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)

    def is_internal_api_request(log_entry):
        """
        Detects whether a log entry is a request to the internal Spotify API. We can use this request's headers to make our own requests to the API.
        """
        internal_api_url = (
            "https://spclient.wg.spotify.com/track-credits-view/v0/experimental/"
        )
        return (
            log_entry["message"]["method"] == "Network.requestWillBeSent"
            and log_entry["message"]["params"]["request"]["url"].startswith(
                internal_api_url
            )
            and log_entry["message"]["params"]["documentURL"].startswith(
                "https://open.spotify.com/track/"
            )
        )

    driver = setup_webdriver()
    open_credits_popup(driver)

    log_entries = driver.get_log("performance")
    for entry in log_entries:
        entry["message"] = json.loads(entry["message"])["message"]

    credits_request_log_entry = next(
        e for e in log_entries if is_internal_api_request(e)
    )
    headers = credits_request_log_entry["message"]["params"]["request"]["headers"]
    return headers
