# Fetches credits information for tracks on Spotify which is not available via Spotify's API.
# Uses Selenium to scrape the Spotify web player for the data.

from datetime import datetime, timedelta
import argparse
import multiprocessing
from tqdm import tqdm
import os
from selenium import webdriver
import inquirer
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from dotenv import load_dotenv
from helpers.scraping import login_and_accept_cookies
import multiprocessing
from typing import List
import time

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


def split_into_chunks(data: list, num_chunks: int):
    chunk_size = len(data) // num_chunks
    chunks = []
    for i in range(num_chunks):
        chunk = data[i * chunk_size : (i + 1) * chunk_size]
        chunks.append(chunk)
    return chunks


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


def extract_data(element):
    result = {}
    result["name"] = element.text
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
    return [extract_data(element) for element in elements]


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


def setup_webdriver(idx: int, username: str, password: str):
    print(f"Setting up WebDriver #{idx + 1}")
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


def flatten_list_of_lists(list_of_lists: List[List[str]]):
    flattened_list = []
    for sublist in list_of_lists:
        if sublist is not None:
            flattened_list.extend(sublist)
    return flattened_list


def process_chunk_with_progress(
    process_idx: int, username: str, password: str, chunk: List[str]
):
    driver_created = False
    while not driver_created:
        try:
            driver = setup_webdriver(process_idx, username, password)
            driver_created = True
        except Exception as e:
            print(f"Error starting webdriver #{process_idx + 1}: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
    track_credits = [None] * len(chunk)

    try:
        with tqdm(
            total=len(chunk),
            desc=f"driver #{process_idx + 1}",
            position=process_idx + 1,
        ) as pbar:
            for i, track_id in enumerate(chunk):
                track_credits[i] = get_credits(driver, track_id)
                pbar.update(1)

        driver.quit()

        return track_credits
    except Exception as e:
        print(f"Exception in process #{process_idx + 1}: {e}")
        driver.quit()
        return None


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

    if os.path.exists(output_path):
        existing_data = (
            pd.read_parquet(output_path)
            if output_format == "parquet"
            else pd.read_csv(output_path)
        )
    else:
        existing_data = pd.DataFrame(columns=output_columns)
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

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

    load_dotenv()
    username = os.environ.get("SPOTIFY_USERNAME")
    password = os.environ.get("SPOTIFY_PASSWORD")

    if username is None or password is None:
        questions = [
            inquirer.Text("username", message="Enter your username:"),
            inquirer.Password("password", message="Enter your password:"),
        ]

        answers = inquirer.prompt(questions)

        username = answers["username"]
        password = answers["password"]

    num_processes = min(args.processes, len(track_ids))
    pool = multiprocessing.Pool(num_processes)
    print(f"Using {num_processes} webdrivers (browser instances) in parallel")
    chunks = split_into_chunks(track_ids, num_processes)

    track_credits = pool.starmap(
        process_chunk_with_progress,
        zip(
            range(num_processes),
            [username] * num_processes,
            [password] * num_processes,
            chunks,
        ),
    )

    pool.close()
    pool.join()

    track_credits = flatten_list_of_lists(track_credits)

    output = pd.concat(
        [
            existing_data,
            pd.DataFrame(
                track_credits,
                columns=output_columns,
            ),
        ],
    )
    if output_path.endswith(".parquet"):
        output.to_parquet(output_path, index=False)
    else:
        output.to_csv(output_path, index=False)

    print(f"Saved output to '{output_path}'")
