# Downloads Spotify charts for a list of regions and a date range specified by start and end dates.
# Also checks if files already exist for a combination of date and region to avoid re-downloading.
# If for a given date and region no chart exists, an empty (i.e. header-only) file is created
# It has the same filename format and headers as a file downloaded normally by clicking the download button on the URL of a chart page

column_names = [
    "rank",
    "uri",
    "artist_names",
    "track_name",
    "source",
    "peak_rank",
    "previous_rank",
    "days_on_chart",
    "streams",
]  # the column names of the downloaded files (or 'placeholder files' for non-existent charts as described above)

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


MAX_QUEUE_SIZE = 32767  # size limit for queues in MacOS X https://stackoverflow.com/a/56379621/13727176 - when trying to add more values to the queue (via .put() in a for loop), the code would hang
# processed_urls = 0  # number of items processed by the worker processes
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


def download_region_chart_csv(
    driver: webdriver,
    url: str,
    download_path: str,
):
    """
    Attempts to download a chart CSV file from a given URL.

    If the chart does not exist, a placeholder file is created.

    Parameters
    ----------
    driver : webdriver
        The Selenium webdriver that will download the file.
    url : str
        The URL of the chart page.
    download_path : str
        The path to the directory where the webdriver will download files.
    """
    driver.get(url)
    wait = WebDriverWait(driver, 5)

    download_button_or_error_page = wait.until(
        EC.any_of(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "button[aria-labelledby='csv_download']")
            ),
            EC.visibility_of_element_located(
                (By.CSS_SELECTOR, '[class^="ErrorPanel"]')
            ),
        )
    )
    if download_button_or_error_page.tag_name == "button":
        download_button = download_button_or_error_page
        download_button.click()
    else:
        # error panel for non-existent chart exists -> create placeholder file
        create_placeholder_file(download_path, url)


def setup_webdriver_for_download(
    username: str, password: str, download_path: str, headless: bool, worker_id: str
):
    """
    Create a Selenium webdriver that will download files to the specified path.

    Parameters
    ----------
    username : str
        Spotify username.
    password : str
        Spotify password.
    download_path : str
        The path to the directory where the webdriver will download files.
    headless : bool, optional
        If True, run the webdriver in headless mode (no browser window), by default True
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
            retry_wait_time = 30
            print(
                f"{worker_id}: Error in driver setup. Trying again in {retry_wait_time} seconds..."
            )
            time.sleep(retry_wait_time)

    print(f"{worker_id}: Driver setup completed")
    return driver


def worker(
    url_queue,
    result_queue,
    username: str,
    password: str,
    download_path: str,
    headless: bool,
    no_of_workers: int,
):
    worker_id = multiprocessing.current_process().name
    driver = setup_webdriver_for_download(
        username, password, download_path, headless, worker_id
    )
    completed_downloads = 0
    while True:
        url = url_queue.get()
        if url is None:
            # Received a sentinel value, no more tasks to process
            # Wait until all downloads are complete
            print("Worker finished, waiting for downloads to complete...")
            while True:
                if get_number_of_pending_downloads(download_path) == 0:
                    break
            driver.quit()
            print("Worker stopped")
            break
        url_processed = False
        while not url_processed:
            try:
                download_region_chart_csv(driver, url, download_path)

                while True:
                    # Wait until the number of pending downloads is less than the number of workers
                    # Otherwise, system might hang because of too many pending downloads
                    pending_downloads = get_number_of_pending_downloads(download_path)
                    if pending_downloads < no_of_workers:
                        break
                url_processed = True
                completed_downloads += 1
                result_queue.put(None)

            except Exception as e:
                print(f"{worker_id}: Error downloading from {url}: {e}")
                print("Retrying...")

        # for some reason, the driver runs out of memory after too many downloads
        # so we restart the driver after a certain number of downloads to avoid this
        restart_interval = 128
        if completed_downloads % restart_interval == 0:
            print(
                f"{worker_id}: Downloaded {completed_downloads} charts, restarting driver (like every {restart_interval} downloads) to avoid out of memory error..."
            )
            driver.quit()
            driver = setup_webdriver_for_download(
                username, password, download_path, headless, worker_id
            )


def remove_incomplete_downloads(path: str = "."):
    """
    Removes all files with the .part extension from the specified directory.
    """
    incomplete_downloads = [f for f in os.listdir(path) if f.endswith("part")]
    for file in incomplete_downloads:
        os.remove(os.path.join(path, file))
    print(f"Removed {len(incomplete_downloads)} incomplete downloads in '{path}'")


def get_number_of_pending_downloads(path: str = "."):
    """
    Returns a list of the paths of the files that are currently being downloaded.
    """
    pending_downloads = [f for f in os.listdir(path) if f.endswith("part")]
    return len(pending_downloads)


def url_feeder(download_urls, url_queue, result_queue, num_processes):
    # You might wonder why this is used
    # The reason is that I wanted to have some kind of 'rate-limiting' mechanism
    # If I just put all the URLs in the queue at once, the workers would start downloading them all at once
    # This would either cause the system to hang because of too many pending downloads OR
    # cause the Spotify server to block my IP because of too many requests
    urls_left_to_download = len(download_urls)
    with tqdm(total=len(download_urls), desc="downloaded charts") as pbar:
        current = time.time()
        while len(download_urls) > 0:
            downloads_current_chunk = 0
            for i in range(0, num_processes):
                if len(download_urls) > 0:
                    url_queue.put(download_urls.pop())
                    downloads_current_chunk += 1

            for i in range(0, downloads_current_chunk):
                # wait for download to complete
                result_queue.get()
                downloads_current_chunk -= 1
                pbar.update(1)
                if pbar.n % 60 == 0:
                    previous = current
                    current = time.time()
                    time_passed = current - previous
                    print(
                        "Processed last 60 URLs in {:.2f} seconds".format(time_passed)
                    )
                    print(f"That's {60/time_passed} URLs per second")
                    print(
                        f"ETA (assuming data-processing rate remains the same): {timedelta(seconds=(urls_left_to_download/(60/time_passed)))}"
                    )
                urls_left_to_download -= 1


def download_charts(
    download_urls: list,
    num_processes: int,
    username: str,
    password: str,
    download_path: str,
    headless: bool = True,
):
    url_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()

    pool = multiprocessing.Pool(
        processes=num_processes,
        initializer=worker,
        initargs=(
            url_queue,
            result_queue,
            username,
            password,
            download_path,
            headless,
            num_processes,
        ),
    )

    # Feed URLs until all were processed
    url_feeder(download_urls, url_queue, result_queue, num_processes)

    # Send a sentinel value for each worker to tell the workers to stop
    for _ in range(num_processes):
        url_queue.put(None)

    # Wait for all workers to finish and exit
    pool.close()
    pool.join()


def get_chart_url(region: str, date: str):
    return f"https://charts.spotify.com/charts/view/regional-{region}-daily/{date}"


def get_date_and_region_code(chart_url: str):
    split_url = chart_url.split("/")
    date = split_url[-1]
    region_code = split_url[-2].split("-")[1]
    return date, region_code


def create_placeholder_file(download_path: str, chart_url: str):
    """
    Creates an empty file with the same name as a chart that does not exist.

    Used for charts that do not exist for a given date and region.

    This approach makes merging incomplete charts easier later on.

    For example, chart data for Belarus is not available for January 2022, but data exists from February onwards.

    When using empty placeholder files, the `combine_charts.py` script doesn't have to be adapted to handle non-existent files. An additional advantage is that we then also know that we already tried downloading charts for a given date if the 'empty' file already exists.

    Parameters
    ----------
    download_path : str
        The path to the directory where the webdriver downloads files.
    chart_url : str
        The URL of the chart that does not exist.
    """
    date, region_code = get_date_and_region_code(chart_url)
    filename = create_chart_filename(region_code, date)
    filepath = os.path.join(download_path, filename)
    df = pd.DataFrame(columns=column_names)
    df.to_csv(filepath, index=False)
    # print(
    #     f"Created placeholder file for non-existent chart for URL '{chart_url}' at '{filepath}'"
    # )


def create_chart_filename(region_code: str, date: str):
    """
    This function is used to create a placeholder filename for a chart that does not exist.

    It uses the same file name format as 'regularly downloaded files':

    `regional-{region_code}-daily-{date}.csv` where `{region_code}` is the region code and `{date}` is the date in YYYY-MM-DD format

    Parameters
    ----------
    region_code : str
        The code used by Spotify for a given region ('global' for global region, else lowercase two-letter ISO code for a given country).
    date : str
        The date in YYYY-MM-DD format.
    """
    return f"regional-{region_code}-daily-{date}.csv"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--start_date",
        type=str,
        help="Start date (YYYY-MM-DD)",
        default="2017-01-01",
    )
    parser.add_argument(
        "-e",
        "--end_date",
        type=str,
        help="End date (YYYY-MM-DD)",
        default=datetime.today().strftime("%Y-%m-%d"),
    )
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
        help="Path to directory where charts will be downloaded to",
        default=create_data_path("scraper_downloads"),
    )
    parser.add_argument(
        "-p",
        "--processes",
        type=int,
        help="Number of parallel processes (browser instances) to use for downloading charts",
        default=min(
            8, multiprocessing.cpu_count() - 1
        ),  # using more than 8 processes never worked for me on my M1 Macbook Pro
    )
    parser.add_argument(
        "--no-headless",
        help="If set, the browser windows will be visible while downloading charts",
        action="store_true",
    )

    args = parser.parse_args()

    date_strs = generate_date_strings(args.start_date, args.end_date)
    print(f"Fetching data for date range [{args.start_date}, {args.end_date}]")
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

    region_codes = [
        "global" if r == "ww" else r for r in region_codes
    ]  # replace region code 'ww' with 'global' for global charts (ww = worldwide; used in file under all_regions_and_codes_csv_path)
    print(f"processing {len(region_codes)} regions")
    regions_and_dates = list(product(region_codes, date_strs))
    print(
        f"processing {len(regions_and_dates)} charts (combinations of regions and dates)"
    )

    download_dir = os.path.join(os.getcwd(), args.output_dir)
    print(f"Using '{download_dir}' as download directory")
    if not os.path.isdir(download_dir):
        os.makedirs(download_dir)

    already_downloaded = set(os.listdir(download_dir))
    print(f"{len(already_downloaded)} files already exist in download directory.")

    def charts_already_downloaded(region_code, date_str):
        file_name = f"regional-{region_code}-daily-{date_str}.csv"
        return file_name in already_downloaded

    download_urls = [
        f"https://charts.spotify.com/charts/view/regional-{r}-daily/{d}"
        for (r, d) in regions_and_dates
        if not charts_already_downloaded(r, d)
    ]

    if len(download_urls) == 0:
        print("All charts already downloaded. Exiting.")
        exit(0)

    print(f"Saving charts to {download_dir}")
    print(f"{len(regions_and_dates) - len(download_urls)} charts already downloaded.")
    print(f"Downloading {len(download_urls)} charts.")
    remove_incomplete_downloads(download_dir)

    num_processes = args.processes or multiprocessing.cpu_count()
    print(
        f"Downloading chart data using {num_processes} processes (WebDriver instances) in parallel."
    )

    username, password = get_spotify_credentials()

    download_charts(
        download_urls,
        num_processes,
        username,
        password,
        download_dir,
        headless=not args.no_headless,
    )
