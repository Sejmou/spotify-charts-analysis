# Fetches lyrics for tracks on Spotify which is not available via Spotify's API.
# Uses Selenium to scrape the Spotify web player for the data.

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
output_columns = ["track_id", "lyrics"]


def split_into_chunks_of_size(list_to_split: List[str], chunk_size: int):
    return [
        list_to_split[i : i + chunk_size]
        for i in range(0, len(list_to_split), chunk_size)
    ]


def get_element_matching_xpath(driver, xpath):
    wait = WebDriverWait(driver, 5)
    element = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
    return element


def get_lyrics(driver: webdriver.Firefox, track_id: str):
    track_page_url = f"https://open.spotify.com/track/{track_id}"
    driver.get(track_page_url)

    h2_element = get_element_matching_xpath(driver, "//h2[contains(text(), 'Lyrics')]")
    parent_element = h2_element.find_element(By.XPATH, "..")
    elements = parent_element.find_elements(By.XPATH, "./*")[
        1:
    ]  # skip the first element, which is the h2 element
    sibling_texts = [element.text for element in elements]

    lyrics_str = "\n".join(sibling_texts)

    return track_id, lyrics_str


def setup_webdriver(username: str, password: str, headless: bool = True):
    while True:
        try:
            options = webdriver.FirefoxOptions()
            options.set_preference("intl.accept_languages", "en-GB")
            if headless:
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


def worker(track_id_queue, result_queue, username, password):
    driver = setup_webdriver(username, password)
    try:
        while True:
            track_id = track_id_queue.get()
            if track_id is None:
                # Received a sentinel value, no more tasks to process
                driver.quit()
                break
            lyrics = get_lyrics(driver, track_id)
            result_queue.put(lyrics)
    except Exception as e:
        print(f"Error in worker: {e}")
        driver.quit()
        print("Restarting worker...")
        worker(track_id_queue, result_queue, username, password)


def get_lyrics_for_track_ids(
    track_ids, num_processes, username, password, output_file, chunk_size=100
):
    max_queue_size = 32767  # size limit for queues in MacOS X https://stackoverflow.com/a/56379621/13727176 - when trying to add more values to the queue (via .put() in a for loop), the code would hang

    track_id_queue = multiprocessing.Queue(max_queue_size)
    lyrics_queue = multiprocessing.Queue(max_queue_size)

    with tqdm(total=len(track_ids), desc="track_ids") as pbar:
        # Create a process pool with the specified number of processes
        pool = multiprocessing.Pool(
            processes=num_processes,
            initializer=worker,
            initargs=(track_id_queue, lyrics_queue, username, password),
        )

        track_id_chunks = split_into_chunks_of_size(
            track_ids, max_queue_size - num_processes
        )

        for chunk in track_id_chunks:
            # Add tasks to the task queue
            for track_id in chunk:
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
                result = lyrics_queue.get()
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
                    write_track_lyrics(intermediate_results, output_file)
                    intermediate_results = []

    lyrics_queue.close()

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


def write_track_lyrics(lyrics, path):
    existing_data = get_existing_data(path)
    output = pd.concat(
        [
            existing_data,
            pd.DataFrame(
                lyrics,
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
        "-i",
        "--input_path",
        type=str,
        help="Path to a .csv or .parquet file containing Spotify track IDs (in a column named 'track_id'))",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_path",
        type=str,
        help="Path where output file with track lyrics will be stored. If it already exists, it will also be used to resume the data fetching process (skipping track_ids that are already contained in it).",
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
        "-c",
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

    username, password = get_spotify_credentials()

    num_processes = min(args.processes, len(track_ids))
    pool = multiprocessing.Pool(num_processes)
    print(f"Using {num_processes} webdrivers (browser instances) in parallel")
    chunk_size = args.chunk_size

    print(f"Fetching lyrics for {len(track_ids)} tracks...")
    print(
        f"Data will be processed in chunks of {chunk_size}; intermediate results are written back to '{output_path}' after each chunk has been processed"
    )

    get_lyrics_for_track_ids(
        track_ids, num_processes, username, password, output_path, chunk_size
    )
