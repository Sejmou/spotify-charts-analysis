# Fetches credits information for tracks on Spotify which is not available via Spotify's API.
# Uses Selenium to scrape the Spotify web player for the data.

from datetime import datetime, timedelta
import argparse
import multiprocessing
from tqdm import tqdm
import os
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from helpers.scraping import login_and_accept_cookies
import multiprocessing
from typing import List
import time
from helpers.scraping import get_spotify_credentials

login_page_url = "https://open.spotify.com/"
output_columns = ["track_id", "performers", "songwriters", "producers", "sources"]


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
        date_strings: List[str] = []
        current_date = start_date
        while current_date <= end_date:
            date_string = current_date.strftime("%Y-%m-%d")
            date_strings.append(date_string)
            current_date += timedelta(days=1)

        return date_strings

    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        exit(1)


def split_into_chunks_of_size(list_to_split: List[str], chunk_size: int):
    return [
        list_to_split[i : i + chunk_size]
        for i in range(0, len(list_to_split), chunk_size)
    ]


def get_element_matching_css_selector(driver, css_selector):
    wait = WebDriverWait(driver, 5)
    element = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
    )
    return element


def get_element_with_att_and_val(driver, att, val):
    wait = WebDriverWait(driver, 5)
    element = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, f'[{att}="{val}"]'))
    )
    return element


def extract_data_from_element_in_credits_container(element, field_name):
    result = {}
    text = element.text
    if text == field_name:
        return None

    result["name"] = text
    if element.tag_name == "a":
        result["href"] = element.get_attribute("href")

    return result


def get_text_of_sibling_elements_of_dialog_p_element_with_text(driver, text):
    wait = WebDriverWait(driver, 5)

    # Find the container which contains the <p> tag with the specified text
    xpath_expression = f"//div[@role='dialog']//p[contains(text(), '{text}')]/parent::*"
    container = wait.until(EC.presence_of_element_located((By.XPATH, xpath_expression)))

    # Find all child elements
    elements = container.find_elements(By.XPATH, ".//*")
    data = [
        extract_data_from_element_in_credits_container(element, text)
        for element in elements
    ]
    return [d for d in data if d is not None]


def get_credits(driver: webdriver.Firefox, track_id: str):
    track_page_url = f"https://open.spotify.com/track/{track_id}"
    driver.get(track_page_url)

    more_button = get_element_with_att_and_val(driver, "data-testid", "more-button")
    more_button.click()

    credits_button = WebDriverWait(driver, 5).until(
        EC.presence_of_element_located(
            (
                By.XPATH,
                f"//div[@id='context-menu']//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'credits')]",
            )
        )
    )
    credits_button.click()

    performers = get_text_of_sibling_elements_of_dialog_p_element_with_text(
        driver, "Performed by"
    )
    songwriters = get_text_of_sibling_elements_of_dialog_p_element_with_text(
        driver, "Written by"
    )
    producers = get_text_of_sibling_elements_of_dialog_p_element_with_text(
        driver, "Produced by"
    )

    sources = (
        WebDriverWait(driver, 5)
        .until(
            EC.presence_of_all_elements_located((By.XPATH, "//div[@role='dialog']//p"))
        )[-1]
        .text.strip()
        .split("Source: ")[1]
        .split(", ")
    )

    return track_id, performers, songwriters, producers, sources


def setup_webdriver(username: str, password: str):
    while True:
        try:
            options = webdriver.FirefoxOptions()
            options.set_preference("intl.accept_languages", "en-GB")
            options.add_argument("-headless")
            driver = webdriver.Firefox(options=options)

            driver.get(login_page_url)
            after_login_url = "https://open.spotify.com/"
            login_and_accept_cookies(driver, username, password, after_login_url)

            wait = WebDriverWait(driver, 5)
            wait.until(EC.url_contains(after_login_url))

            return driver
        except Exception as e:
            print(f"Error starting webdriver: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)


def flatten_list_of_lists(list_of_lists: List[List[str]]):
    flattened_list = []
    for sublist in list_of_lists:
        if sublist is not None:
            flattened_list.extend(sublist)
    return flattened_list


def worker(track_id_queue, result_queue, username, password):
    driver = setup_webdriver(username, password)
    try:
        while True:
            track_id = track_id_queue.get()
            if track_id is None:
                # Received a sentinel value, no more tasks to process
                driver.quit()
                break
            track_credits = get_credits(driver, track_id)
            result_queue.put(track_credits)
    except Exception as e:
        print(f"Error in worker: {e}")
        driver.quit()


def get_credits_for_track_ids(
    track_ids, num_processes, username, password, output_file, chunk_size=100
):
    # Create a task queue
    track_id_queue = multiprocessing.Queue()
    credits_queue = multiprocessing.Queue()

    with tqdm(total=len(track_ids), desc="track_ids") as pbar:
        # Create a process pool with the specified number of processes
        pool = multiprocessing.Pool(
            processes=num_processes,
            initializer=worker,
            initargs=(track_id_queue, credits_queue, username, password),
        )

        # Add tasks to the task queue
        for track_id in track_ids:
            track_id_queue.put(track_id)

        # Add sentinel values to indicate the end of tasks for each process
        for _ in range(num_processes):
            track_id_queue.put(None)

        # Close the input queue to prevent further task addition
        track_id_queue.close()

        # Read results from the result queue
        intermediate_results = []
        finished_processes = 0
        while finished_processes < num_processes:
            result = credits_queue.get()
            if result is None:
                # Received a sentinel value, one process has finished
                finished_processes += 1
                print(
                    f"Process finished, {num_processes - finished_processes} remaining"
                )
                continue
            intermediate_results.append(result)
            pbar.update(1)
            if (
                len(intermediate_results) == chunk_size
                or finished_processes == num_processes
            ):
                # Write results to file
                write_track_credits(intermediate_results, output_file)
                intermediate_results = []

    credits_queue.close()

    # Wait for all processes to finish
    pool.close()
    pool.join()


def get_existing_data(path):
    if os.path.exists(path):
        existing_data = (
            pd.read_parquet(path) if output_format == "parquet" else pd.read_csv(path)
        )
    else:
        existing_data = pd.DataFrame(columns=output_columns)
        output_dir = os.path.dirname(path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    return existing_data


def write_track_credits(credits, path):
    existing_data = get_existing_data(path)
    output = pd.concat(
        [
            existing_data,
            pd.DataFrame(
                credits,
                columns=output_columns,
            ),
        ],
    )
    if path.endswith(".parquet"):
        output.to_parquet(path, index=False)
    else:
        output.to_csv(path, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_path",
        type=str,
        help="Path to a .csv or .parquet file containing Spotify track IDs (in a column named 'track_id'))",
    )
    parser.add_argument(
        "output_path",
        type=str,
        help="Path where output file with track credits will be stored. If it already exists, it will also be used to resume the data fetching process (skipping track_ids that are already contained in it).",
    )
    parser.add_argument(
        "--processes",
        type=int,
        help="Number of parallel processes (browser instances) to use for scraping the data. Defaults to number of available CPU cores.",
        default=multiprocessing.cpu_count(),
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        help="Number of track_ids to process in each chunk. After a chunk is done processing, intermediate results will be written to the output file. Defaults to 100.",
        default=100,
    )

    args = parser.parse_args()

    input_path = args.input_path
    if input_path.endswith(".csv"):
        input_df = pd.read_csv(input_path)
        input_format = "csv"
    elif input_path.endswith(".parquet"):
        input_df = pd.read_parquet(input_path)
        input_format = "parquet"
    else:
        raise ValueError(
            f"Input file '{input_path}' must be either a .csv or .parquet file"
        )

    track_ids = (input_df)["track_id"].unique().tolist()
    print(f"Found {len(track_ids)} unique track IDs in '{input_path}'")

    output_path = args.output_path
    if output_path.endswith(".csv"):
        output_format = "csv"
    elif output_path.endswith(".parquet"):
        output_format = "parquet"
    else:
        raise ValueError(
            f"Output file '{output_path}' must be either a .csv or .parquet file"
        )

    existing_data = get_existing_data(output_path)

    if len(existing_data) > 0:
        track_ids = [
            track_id
            for track_id in track_ids
            if track_id not in existing_data["track_id"].unique().tolist()
        ]
        print(
            f"Skipping {len(existing_data)} track IDs already contained in '{output_path}'"
        )

    if len(track_ids) == 0:
        print("All track IDs already contained in output file. Nothing to do.")
        exit(0)

    username, password = get_spotify_credentials()

    num_processes = min(args.processes, len(track_ids))
    pool = multiprocessing.Pool(num_processes)
    print(f"Using {num_processes} webdrivers (browser instances) in parallel")
    chunk_size = args.chunk_size

    print(f"Fetching credits for {len(track_ids)} tracks...")
    print(
        f"Data will be processed in chunks of {chunk_size}; intermediate results are written back to '{output_path}' after each chunk has been processed"
    )

    get_credits_for_track_ids(
        track_ids, num_processes, username, password, output_path, chunk_size
    )
