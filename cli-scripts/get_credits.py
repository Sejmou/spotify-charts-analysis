"""
Gets credits for track IDs contained in a .parquet file via an internal Spotify API and saves the results to a .jsonl file.

You might have to rerun this script a few times to get all the data, as sometimes the Spotify API returns an error response if too many requests are sent at once.

Could try switching from parallel to sequential requests if the number of tracks to process is small
(just comment out the multiprocessing part and uncomment the sequential part).
"""

import json
import asyncio
import aiohttp
import argparse
from helpers.scraping import get_credits_api_url, get_internal_api_request_headers
from helpers.util import (
    split_into_chunks_of_size,
    append_line_to_file,
    append_list_to_file,
)
import pandas as pd
from tqdm import tqdm
import os
import random


# simpler, but slower synchronous version
import requests
import time


def sync_main(urls: list, headers: dict, error_log_path: str):
    valid_responses = []
    for url in tqdm(urls):
        try:
            response = requests.get(url, headers=headers)
            response_json = response.json()
            if "error" in response_json:
                # received response in a form like {"error": {"status": 502, "message": "Backend respond with 500"}}
                append_line_to_file(
                    f"{url}: {json.dumps(response_json)}", error_log_path
                )
            else:
                valid_responses.append(response_json)

        except Exception:
            # backend threw some other error that is not in the form of a JSON object
            # usually this is java.lang.RuntimeException: Invalid response from entity service: [SERVICE_UNAVAILABLE]
            append_line_to_file(f"{url}: {response.text}", error_log_path)

    return valid_responses


async def send_request(session, url, headers):
    async with session.get(url, headers=headers) as response:
        # for some reason, internal API returns response with text content type, even though the response is JSON
        return await response.text()


async def async_main(urls, headers, error_log_path):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            task = asyncio.create_task(send_request(session, url, headers))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)

        for i, response in enumerate(responses):
            try:
                responses[i] = json.loads(response)
                if "error" in responses[i]:
                    # received response in a form like {"error": {"status": 502, "message": "Backend respond with 500"}}
                    responses[i] = {}
                    append_line_to_file(
                        f"{urls[i]}: {json.dumps(response)}", error_log_path
                    )

            except Exception:
                # backend threw some other error that is not in the form of a JSON object
                # usually this is java.lang.RuntimeException: Invalid response from entity service: [SERVICE_UNAVAILABLE]
                responses[i] = {}
                append_line_to_file(f"{urls[i]}: {response}", error_log_path)

        valid_responses = [r for r in responses if r != {}]
        return valid_responses


def get_existing_track_ids(jsonl_file_path: str):
    """
    Returns a set of track IDs that are already contained in the JSONL file.
    They are extracted from the "trackUri" field of each line in the JSONL file.
    """
    df = pd.read_json(jsonl_file_path, lines=True)
    track_ids = set(
        df["trackUri"].apply(lambda uri: uri.split(":")[2]).unique().tolist()
    )  # probably some more efficient way to do this exists
    return track_ids


def parse_error_log_file(error_log_path: str):
    """
    Parses the error log file and returns a list of dictionaries, each containing the following fields:
    - track_id: the ID of the track that caused the error
    - url: the URL that was requested
    - error: the error message
    - error_message_type: either "json" or "string", depending on whether the error message is a JSON object or a string
    """
    with open(error_log_path, "r") as f:
        lines = f.readlines()
        lines = [line.strip() for line in lines if line.strip() != ""]

        def get_error_details(line):
            parts = line.split(
                ": ", 1
            )  # split ONLY on the first colon followed by a space https://stackoverflow.com/a/6903597/13727176
            url = parts[0]
            error = parts[1]
            track_id = url.split("/")[
                -2
            ]  # the track ID is the second-to-last part of the URL
            try:
                error = json.loads(error)
                error_message_type = "json"
            except Exception:
                error_message_type = "string"
            return {
                "track_id": track_id,
                "url": url,
                "error": error,
                "error_message_type": error_message_type,
            }

        error_details = [get_error_details(line) for line in lines]
        return error_details


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
        help="Path where the JSONL file containing the credits data will be saved (this file path will also be used to resume the data fetching process (skipping track_ids that are already contained in it).",
        required=True,
    )

    args = parser.parse_args()

    input_path = args.input_path
    try:
        input_df = pd.read_parquet(input_path)
    except Exception:
        raise ValueError(f"Input file '{input_path}' must be a .parquet file")

    try:
        track_ids = (input_df)["track_id"].unique().tolist()
        print(f"Found {len(track_ids)} track IDs in input file '{input_path}'")
    except Exception:
        raise ValueError(
            f"Input file '{input_path}' must contain a column named 'track_id'"
        )

    output_path = args.output_path
    if os.path.exists(output_path):
        # load ids from the existing JSON objects in the file
        with open(output_path, "r") as f:
            existing_track_ids = get_existing_track_ids(output_path)
            print(
                f"Found {len(existing_track_ids)} existing track IDs in output file '{output_path}'"
            )
            track_ids = [
                track_id for track_id in track_ids if track_id not in existing_track_ids
            ]
            if len(track_ids) == 0:
                print("No track IDs left to fetch credits data for!")
                exit(0)
            else:
                print(f"Data is still missing for {len(track_ids)} track IDs")
    else:
        # create output file and required subdirectories if they don't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        open(output_path, "w").close()

    error_log_path = output_path.replace(".jsonl", "_errors.log")

    print(f"Fetching credits data for {len(track_ids)} track IDs")

    max_requests = (
        2048  # cannot send too many requests at once (internal API will fail too often)
    )
    id_chunks = split_into_chunks_of_size(track_ids, max_requests)

    def pick_random_track_id_for_request_header_fetching():
        # sometimes credits pages for a given track can be broken (e.g. https://open.spotify.com/track/6lip7aGS7oXloGk1XFCqWH -> click on three dots -> click 'Show credits' ->  app crashes with 'Error loading page')
        # that is probably also the reason why some internal API requests fail
        # So, we pick a random track ID to fetch the request headers from, hoping that it will be a valid one
        # If this doesn't work, we just try again
        return random.choice(track_ids)

    print(f"Processing {len(id_chunks)} ID chunks of (maximum) size {max_requests}")
    with tqdm(total=len(id_chunks), desc="Total progress") as pbar_total:
        for ids in id_chunks:
            remaining_ids = ids
            while not len(remaining_ids) == 0:
                previous_number_of_ids_to_process = len(remaining_ids)
                urls = [get_credits_api_url(t_id) for t_id in ids]
                headers = None
                while headers is None:
                    try:
                        headers = get_internal_api_request_headers(
                            pick_random_track_id_for_request_header_fetching()
                        )
                    except Exception:
                        print("Failed to get request headers. Trying again...")

                # run requests in parallel
                valid_responses = asyncio.run(
                    async_main(
                        urls=urls, headers=headers, error_log_path=error_log_path
                    )
                )

                # run requests sequentially
                # valid_responses = sync_main(
                #     urls=urls, headers=headers, error_log_path=error_log_path
                # )

                processed_ids = [
                    response["trackUri"].split(":")[2] for response in valid_responses
                ]
                # print(f"Processed {len(processed_ids)} IDs")
                remaining_ids = [t_id for t_id in ids if t_id not in processed_ids]
                if len(remaining_ids) == previous_number_of_ids_to_process:
                    print(
                        f"Failed to process {len(remaining_ids)} IDs. Skipping them. Check '{error_log_path}' for errors that occurred."
                    )
                    remaining_ids = []

            append_list_to_file([json.dumps(r) for r in valid_responses], output_path)
            pbar_total.update(1)
