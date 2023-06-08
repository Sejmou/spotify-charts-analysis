from selenium import webdriver
from typing import Literal
from selenium.webdriver.common.by import By
import os
from dotenv import load_dotenv
from selenium.webdriver.support.ui import WebDriverWait
import selenium.webdriver.support.expected_conditions as EC
import time

login_page_url = "https://accounts.spotify.com/en/login?continue=https%3A%2F%2Fcharts.spotify.com/charts/overview/global"
after_login_url = "https://charts.spotify.com/charts/overview/global"  # the URL that the driver will be at after successfully logging in


def setup_webdriver_for_download(download_path: str):
    """
    Create a Selenium webdriver that will download files to the specified path.

    NOTE: This function assumes that you have placed a .env file in the same folder, containing the username and password for your Spotify account as SPOTIFY_USERNAME and SPOTIFY_PASSWORD, respectively.
    This file should be placed in the same folder as the file that calls this function.

    Parameters
    ----------
    download_path : str
        The path to the directory where the webdriver will download files.
    """
    options = webdriver.FirefoxOptions()
    options.set_preference(
        "browser.download.folderList", 2
    )  # 0: download to the desktop, 1: download to the default "Downloads" directory, 2: use the directory
    options.set_preference("browser.download.dir", download_path)
    options.headless = True  # run the webdriver in headless mode (no browser window)

    load_dotenv()
    username = os.environ.get("SPOTIFY_USERNAME")
    password = os.environ.get("SPOTIFY_PASSWORD")

    driver = webdriver.Firefox(options=options)

    setup_completed = False
    while not setup_completed:
        try:
            login_and_accept_cookies(driver, username, password)
            setup_completed = True
        except Exception:
            # wait for 5 seconds and try again
            time.sleep(5)

    return driver


def login_and_accept_cookies(driver: webdriver, username, password):
    fill_and_submit_login_form(driver, username, password)

    wait = WebDriverWait(driver, 5)
    wait.until(EC.url_contains(after_login_url))
    time.sleep(5)  # no idea what I have to wait but if I don't it fails quite often lol

    cookie_button = wait.until(
        EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "#onetrust-accept-btn-handler")
        )
    )
    cookie_button.click()


def fill_and_submit_login_form(driver: webdriver, username, password):
    driver.get(login_page_url)

    # enter credentials on login page
    username_input = driver.find_element(By.CSS_SELECTOR, "#login-username")
    username_input.send_keys(username)

    password_input = driver.find_element(By.CSS_SELECTOR, "#login-password")
    password_input.send_keys(password)

    login_btn = driver.find_element(By.CSS_SELECTOR, "#login-button")
    login_btn.click()


def charts_already_downloaded(
    download_path: str,
    date: str,
    region: str,
    granularity: Literal["weekly", "daily"] = "daily",
):
    """
    Check if a file has already been downloaded to the specified path.

    NOTE: be careful that the strings provided don't contain any line breaks or other whitespace characters. This function will not strip them and might return false negatives. Took me quite some time to figure out this was the problem with some other code relying on this lol

    Parameters
    ----------
    download_path : str
        The path to the directory where the webdriver will download files.
    date : str
        The date of the chart to download, in the format YYYY-MM-DD.
    region : str
        The region code of the region to download the chart for, e.g. "global" or a two-letter country-code (like "us").
    granularity : Literal["weekly", "daily"], optional
        The granularity of the chart to download, by default "daily".

    Returns
    -------
    bool
        True if the file already exists, False otherwise.
    """
    filename = f"regional-{region}-{granularity}-{date}.csv"
    filepath = os.path.join(download_path, filename)
    return os.path.exists(filepath)


def download_region_chart_csv(
    driver: webdriver,
    date: str,
    region: str,
    granularity: Literal["weekly", "daily"] = "daily",
):
    url = (
        f"https://charts.spotify.com/charts/view/regional-{region}-{granularity}/{date}"
    )

    driver.get(url)
    wait = WebDriverWait(driver, 5)
    download_button = wait.until(
        EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "button[aria-labelledby='csv_download']")
        )
    )
    download_button.click()
