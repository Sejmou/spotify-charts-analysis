# Downloads Spotify charts for a list of regions and a date range specified by start and end dates.
# Also checks if files already exist for a combination of date and region to avoid re-downloading.

from helpers import (
    download_region_chart_csv,
    setup_webdriver_for_download,
    create_data_path,
)
from helpers.scraping import charts_already_downloaded
from datetime import datetime, timedelta
import argparse
import multiprocessing
from tqdm import tqdm
from itertools import product


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


def process_chunk(idx: int, chunk: list, download_path: str):
    driver = setup_webdriver(idx, download_path)
    process_chunk_with_progress(idx, driver, chunk, download_path)
    driver.quit()


def setup_webdriver(idx, download_path):
    print(f"Setting up WebDriver #{idx + 1}")
    driver = setup_webdriver_for_download(download_path)
    return driver


def process_chunk_with_progress(idx, driver, chunk: list, download_path: str):
    with tqdm(total=len(chunk), desc=f"driver #{idx + 1}", position=idx + 1) as pbar:
        for region_code, date_str in chunk:
            download_region_chart_csv(driver, date_str, region_code)
            pbar.update(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--region_codes",  # -- marks optional arguments
        type=str,
        help="Path to a text file with a list of regions to download charts for (two-letter country codes for countries, 'global' for global charts)",
        default=create_data_path("region_codes.txt"),
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        help="Absolute path to directory where charts will be saved",
        default=create_data_path("scraper_downloads"),
    )
    parser.add_argument(
        "--parallel_browser_count",
        type=int,
        help="Number of parallel browser instances to use for downloading charts",
        default=6,
    )

    args = parser.parse_args()

    date_strs = generate_date_strings(args.start_date, args.end_date)
    print(f"processing {len(date_strs)} dates")
    region_codes = read_lines_from_file(args.region_codes)
    print(f"processing {len(region_codes)} regions")
    regions_and_dates = list(product(region_codes, date_strs))
    print(
        f"processing {len(regions_and_dates)} charts (combinations of regions and dates)"
    )

    download_dir = create_data_path(args.output_dir)
    data_to_download = [
        (r, d)
        for (r, d) in regions_and_dates
        if not charts_already_downloaded(download_dir, d, r)
    ]

    if len(data_to_download) == 0:
        print("All charts already downloaded. Exiting.")
        exit(0)

    print(f"Saving charts to {download_dir}")
    print(
        f"{len(regions_and_dates) - len(data_to_download)} charts already downloaded."
    )
    print(f"Downloading {len(data_to_download)} charts.")

    # Number of processes to run in parallel
    num_processes = args.parallel_browser_count
    print(
        f"Downloading chart data using {num_processes} WebDriver instances in parallel."
    )

    chunks = split_into_chunks(regions_and_dates, num_processes)

    pool = multiprocessing.Pool(processes=num_processes)

    pool.starmap(
        process_chunk,
        zip(
            range(num_processes),
            chunks,
            [download_dir] * num_processes,
        ),
    )
