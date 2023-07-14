# Fetches credits information for tracks on Spotify which is not available via Spotify's API.
# Uses Selenium to scrape the Spotify web player for the data.
# NOTE: This script occacionally crashes for larger numbers of tracks. I don't know why.

import argparse
import multiprocessing
from tqdm import tqdm
import os
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from helpers.scraping import accept_cookies, save_debug_screenshot
import multiprocessing
from typing import List
import time
from datetime import timedelta
import psutil

start_page_url = "https://open.spotify.com/"
output_columns = ["track_id", "performers", "songwriters", "producers", "sources"]


def split_into_chunks_of_size(list_to_split: List[str], chunk_size: int):
    return [
        list_to_split[i : i + chunk_size]
        for i in range(0, len(list_to_split), chunk_size)
    ]


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
        EC.element_to_be_clickable(
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

    try:
        sources = (
            WebDriverWait(driver, 5)
            .until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, "//div[@role='dialog']//p")
                )
            )[-1]
            .text.strip()
            .split("Source: ")[1]
            .split(", ")
        )
    except IndexError:
        sources = []  # No sources exist - possible!

    return track_id, performers, songwriters, producers, sources


def setup_webdriver(headless: bool = True):
    while True:
        try:
            options = webdriver.FirefoxOptions()
            options.set_preference("intl.accept_languages", "en-GB")
            if headless:
                options.add_argument("-headless")
            driver = webdriver.Firefox(options=options)

            driver.get(start_page_url)
            accept_cookies(driver)

            return driver
        except Exception as e:
            print(f"Error starting webdriver: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)


import multiprocessing
import psutil


import multiprocessing
import psutil


def worker(track_id_queue, result_queue, headless: bool = True):
    worker_id = multiprocessing.current_process().name
    print(f"{worker_id} started")
    driver = setup_webdriver(headless)
    processed_count = 0  # Counter for processed track IDs
    memory_check_interval = 10  # Interval for memory usage check
    while True:
        track_id = track_id_queue.get()
        if track_id is None:
            # Received a sentinel value, no more tasks to process
            driver.quit()
            result_queue.put(None)
            break

        track_processed = False
        while not track_processed:
            try:
                track_credits = get_credits(driver, track_id)
                result_queue.put(track_credits)
                track_processed = True
            except Exception as e:
                print(f"Error in {worker_id}: {e}")
                save_debug_screenshot(driver, "data/screenshots", worker_id, track_id)

        processed_count += 1

        # Check memory usage after every 'memory_check_interval' processed track IDs
        if processed_count % memory_check_interval == 0:
            memory_usage = psutil.Process().memory_percent()
            print(
                f"{worker_id} - Processed {processed_count} track IDs.\nMemory Usage: {memory_usage}%"
            )
            if memory_usage > 70:
                print("Memory usage is too high, restarting webdriver...")
                driver.quit()
                driver = setup_webdriver(headless)

    print(f"{worker_id} finished")


def get_credits_for_track_ids(
    track_ids: list,
    num_processes: int,
    output_file: str,
    storage_interval: int,
    headless: bool = True,
):
    track_id_queue = multiprocessing.Queue()
    credits_queue = multiprocessing.Queue()
    pool = multiprocessing.Pool(
        processes=num_processes,
        initializer=worker,
        initargs=(track_id_queue, credits_queue, headless),
    )

    track_id_chunks = split_into_chunks_of_size(track_ids, num_processes)

    with tqdm(total=len(track_ids), desc="track_ids") as pbar:
        intermediate_results = []
        current = time.time()
        for chunk in track_id_chunks:
            for track_id in chunk:
                track_id_queue.put(track_id)

            # Read results from the result queue

            results_left_to_process = len(chunk)
            while results_left_to_process > 0:
                result = credits_queue.get()
                intermediate_results.append(result)
                pbar.update(1)
                results_left_to_process -= 1

                if len(intermediate_results) % storage_interval == 0:
                    # Write results to file
                    write_track_credits(intermediate_results, output_file)
                    intermediate_results = []
                    previous = current
                    current = time.time()
                    processing_time = current - previous
                    print(
                        f"Processing of last {storage_interval} track IDs took {processing_time} seconds"
                    )
                    print(f"Stored intermediate results")
                    credits_per_second = storage_interval / processing_time
                    print(f"Processing rate: {credits_per_second} credits per second")
                    print(
                        f"ETA (assuming data-processing rate stays the same): {timedelta(seconds=(len(track_ids) - pbar.n) / credits_per_second)}"
                    )

    # Add sentinel values to indicate the end of tasks for each process
    for _ in range(num_processes):
        track_id_queue.put(None)
    write_track_credits(intermediate_results, output_file)

    # Wait for all processes to finish
    pool.close()
    pool.join()


def get_existing_data(path):
    if os.path.exists(path):
        existing_data = pd.read_parquet(path)
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
    output.to_parquet(path, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input_path",
        type=str,
        help="Path to a .parquet file containing Spotify track IDs (in a column named 'track_id'))",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_path",
        type=str,
        help="Path where output file with track credits will be stored. If it already exists, it will also be used to resume the data fetching process (skipping track_ids that are already contained in it).",
        required=True,
    )
    parser.add_argument(
        "-p",
        "--processes",
        type=int,
        help="Number of parallel processes (browser instances) to use for scraping the data. Defaults to number of available CPU cores.",
        default=multiprocessing.cpu_count(),
    )
    parser.add_argument(
        "-s",
        "--storage_interval",
        type=int,
        help="Number of track_ids after which intermediate results will be written to the output file.",
        default=60,
    )
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        help="If set, the browser will be visible during scraping. Useful for debugging.",
    )

    args = parser.parse_args()

    input_path = args.input_path
    try:
        input_df = pd.read_parquet(input_path)
    except Exception:
        raise ValueError(f"Input file '{input_path}' must be a .parquet file")

    try:
        track_ids = (input_df)["track_id"].unique().tolist()
        print(f"Found {len(track_ids)} unique track IDs in '{input_path}'")
    except Exception:
        raise ValueError(
            f"Input file '{input_path}' must contain a column named 'track_id'"
        )

    output_path = args.output_path

    existing_data = get_existing_data(output_path)
    existing_track_ids = set(existing_data["track_id"].unique().tolist())

    if len(existing_data) > 0:
        track_ids = [
            track_id for track_id in track_ids if track_id not in existing_track_ids
        ]
        print(
            f"Skipping {len(existing_data)} track IDs already contained in '{output_path}'"
        )

    if len(track_ids) == 0:
        print("All track IDs already contained in output file. Nothing to do.")
        exit(0)

    num_processes = min(args.processes, len(track_ids))
    pool = multiprocessing.Pool(num_processes)
    print(f"Using {num_processes} webdrivers (browser instances) in parallel")

    storage_interval = args.storage_interval
    print(f"Fetching credits for {len(track_ids)} tracks...")
    print(
        f"Intermediate results will be written to '{output_path}' after every {storage_interval} tracks"
    )

    get_credits_for_track_ids(
        track_ids, num_processes, output_path, storage_interval, headless=args.headless
    )
