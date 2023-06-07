# Description: Fetch Spotify charts for a given region and date range.
# Usage: python fetch-region-charts.py <start_date> <end_date>
# Example: python fetch-region-charts.py 2020-01-01 2020-01-31

from helpers import (
    create_data_path,
    download_region_chart_csv,
    create_webdriver_for_download,
)
from datetime import datetime, timedelta
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "region",
    type=str,
    help="region (either 'global' or two-letter country code for a specific country, e.g. 'us')",
)
parser.add_argument("start_date", type=str, help="Start date (YYYY-MM-DD)")
parser.add_argument("end_date", type=str, help="End date (YYYY-MM-DD)")
args = parser.parse_args()


def generate_date_strings(start_date: str, end_date: str):
    # Convert start and end dates to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Generate and store date strings in a list
    date_strings = []
    current_date = start_date
    while current_date <= end_date:
        date_string = current_date.strftime("%Y-%m-%d")
        date_strings.append(date_string)
        current_date += timedelta(days=1)

    return date_strings


date_strs = generate_date_strings(args.start_date, args.end_date)
region = args.region

download_folder_path = create_data_path("scraper_downloads")
profile_path = (
    "/Users/Sejmou/Library/Application Support/Firefox/Profiles/q27nge3i.spotify-charts"
)

driver = create_webdriver_for_download(download_folder_path, profile_path)

for date_str in date_strs:
    download_region_chart_csv(driver, date_str, region, download_folder_path)

driver.quit()
