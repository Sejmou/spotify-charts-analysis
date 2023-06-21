# Downloads Spotify charts for a list of regions and a date range specified by start and end dates.
# Also checks if files already exist for a combination of date and region to avoid re-downloading.

from typing import Literal
from helpers.scraping import (
    get_spotify_credentials,
    login_and_accept_cookies,
)
from helpers.data import create_data_path
from datetime import datetime, timedelta
import argparse
import multiprocessing
from tqdm import tqdm
from itertools import product
import os
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd

all_regions_and_codes_csv_path = create_data_path("region_names_and_codes.csv")


def read_lines_from_file(path: str):
    with open(path, "r") as f:
        lines = (
            f.read().splitlines()
        )  # note to future self: DON'T use .readlines() here, it includes the line breaks in the strings
    return lines


def generate_date_strings(start_date: str, end_date: str):
    try:
        # Convert start and end dates to datetime objects
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        # Generate and store date strings in a list
        date_strings: list(str) = []
        current_date = start_date
        while current_date <= end_date:
            date_string = current_date.strftime("%Y-%m-%d")
            date_strings.append(date_string)
            current_date += timedelta(days=1)

        return date_strings

    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        exit(1)


def split_into_chunks(data: list, num_chunks: int):
    chunk_size = len(data) // num_chunks
    chunks = []
    for i in range(num_chunks):
        chunk = data[i * chunk_size : (i + 1) * chunk_size]
        chunks.append(chunk)
    return chunks


def process_chunk(
    idx: int, chunk: list, username: str, password: str, download_path: str
):
    driver = setup_webdriver(idx, username, password, download_path)
    process_chunk_with_progress(idx, driver, chunk, download_path)
    driver.quit()


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


def setup_webdriver_for_download(
    username: str, password: str, download_path: str, headless: bool = True
):
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
    options.headless = (
        headless  # if True, run the webdriver in headless mode (no browser window)
    )

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


def setup_webdriver(idx: int, username: str, password: str, download_path: str):
    print(f"Setting up WebDriver #{idx + 1}")
    driver = setup_webdriver_for_download(username, password, download_path)
    return driver


def process_chunk_with_progress(idx, driver, chunk: list, download_path: str):
    with tqdm(total=len(chunk), desc=f"driver #{idx + 1}", position=idx + 1) as pbar:
        for i, (region_code, date_str) in enumerate(chunk):
            if i > 0 and i % 128 == 0:
                # reset webdriver every 128 downloads as for some reason it starts to hang after around this number of downloads
                driver.quit()
                driver = setup_webdriver(idx, download_path)
            download_region_chart_csv(driver, date_str, region_code)
            pbar.update(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start_date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("-e", "--end_date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "-r",
        "--region_codes",
        type=str,
        help="List of regions to download charts for (two-letter country codes for countries, 'global' for global charts) - can be a path to a text file or a comma-separated list of codes",
        default=create_data_path(
            "region_names_and_codes.csv"
        ),  # path to file containing region names and codes for all regions with chart data on the Spotify Charts website; the 'code' column will be used for obtaining the region codes
        nargs="+",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        help="Absolute path to directory where charts will be downloaded to",
        default=create_data_path("scraper_downloads"),
    )
    parser.add_argument(
        "-p",
        "--processes",
        type=int,
        help="Number of parallel processes (browser instances) to use for downloading charts; defaults to the number of CPU cores on your machine",
        default=multiprocessing.cpu_count(),
    )

    args = parser.parse_args()

    date_strs = generate_date_strings(args.start_date, args.end_date)
    print(f"processing {len(date_strs)} dates")

    if args.region_codes == all_regions_and_codes_csv_path:
        print(
            f"No region codes provided. Using region codes for all countries with Spotify Chart data from '{all_regions_and_codes_csv_path}'"
        )
        region_codes = pd.read_csv(all_regions_and_codes_csv_path)["code"].tolist()
    elif os.path.isfile(args.region_codes[0]):
        region_codes = read_lines_from_file(args.region_codes)
    else:
        region_codes = args.region_codes
    print(f"processing {len(region_codes)} regions")
    regions_and_dates = list(product(region_codes, date_strs))
    print(
        f"processing {len(regions_and_dates)} charts (combinations of regions and dates)"
    )

    download_dir = args.output_dir
    print(f"Using '{download_dir}' as download directory")
    if not os.path.isdir(download_dir):
        os.makedirs(download_dir)

    already_downloaded = set(os.listdir(download_dir))
    print(f"{len(already_downloaded)} files already exist in download directory.")

    def charts_already_downloaded(region_code, date_str):
        file_name = f"regional-{region_code}-daily-{date_str}.csv"
        return file_name in already_downloaded

    data_to_download = [
        (r, d) for (r, d) in regions_and_dates if not charts_already_downloaded(r, d)
    ]

    if len(data_to_download) == 0:
        print("All charts already downloaded. Exiting.")
        exit(0)

    username, password = get_spotify_credentials()

    print(f"Saving charts to {download_dir}")
    print(
        f"{len(regions_and_dates) - len(data_to_download)} charts already downloaded."
    )
    print(f"Downloading {len(data_to_download)} charts.")

    num_processes = args.processes or multiprocessing.cpu_count()
    print(
        f"Downloading chart data using {num_processes} processes (WebDriver instances) in parallel."
    )

    chunks = split_into_chunks(data_to_download, num_processes)

    # TODO: switch to smarter approach using a queue, like in get_credits.py
    pool = multiprocessing.Pool(processes=num_processes)

    pool.starmap(
        process_chunk,
        zip(
            range(num_processes),
            chunks,
            [username] * num_processes,
            [password] * num_processes,
            [download_dir] * num_processes,
        ),
    )
