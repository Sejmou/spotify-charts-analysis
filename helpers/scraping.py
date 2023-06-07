from selenium import webdriver
from typing import Literal
from selenium.webdriver.common.by import By
import os


def create_webdriver_for_download(download_path: str, profile_path: str):
    """
    Create a Selenium webdriver that will download files to the specified path.

    NOTE: for this to work, you must have Firefox installed on your machine, and have created a Firefox profile (via about:profiles) that is logged into Spotify.
    On macOS, created profiles are located in /Users/<username>/Library/Application Support/Firefox/Profiles/

    Parameters
    ----------
    download_path : str
        The path to the directory where the webdriver will download files.
    profile_path : str
        The path to the Firefox profile that is logged into Spotify.
    """
    options = webdriver.FirefoxOptions()
    options.set_preference(
        "browser.download.folderList", 2
    )  # 0: download to the desktop, 1: download to the default "Downloads" directory, 2: use the directory
    options.set_preference("browser.download.dir", download_path)
    options.add_argument("-profile")
    options.add_argument(profile_path)

    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(
        10
    )  # poll the DOM for 10 seconds when searching for elements; throw an error if not found after 10 seconds
    return driver


def download_region_chart_csv(
    driver: webdriver,
    date: str,
    region: str,
    download_folder_path: str,
    granularity: Literal["weekly", "daily"] = "daily",
):
    url = (
        f"https://charts.spotify.com/charts/view/regional-{region}-{granularity}/{date}"
    )
    downloaded_file_path = os.path.abspath(
        os.path.join(
            download_folder_path, f"regional-{region}-{granularity}-{date}.csv"
        )
    )

    if os.path.exists(downloaded_file_path):
        print(f"File already exists at {downloaded_file_path}. Skipping download.")
        return

    driver.get(url)
    download_button = driver.find_element(
        By.CSS_SELECTOR, "button[aria-labelledby='csv_download']"
    )
    download_button.click()
