"""
Gets data for track IDs contained in a .parquet file via an internal Spotify API and saves the results to a .jsonl file.

Supports both parallel and sequential requests. However, there's a few caveats with parallel requests:
 - They are MUCH less reliable (fail quite often), especially when fetching lyrics rather than credits. Using them should be fine for credits, but for lyrics it's probably best to stick to sequential requests.
 - They are buggy, producing empty lines in some cases (haven't found the error yet). Still, loading the resulting .jsonl file into a DataFrame works fine with pandas so it's not a big deal. If you're bothered by this, just let ChatGPT write a script to remove empty lines lol

Could try switching from parallel to sequential requests if the number of tracks to process is small

Also, there's currently still an issue with the lyrics API: 
it requires you to login with your Spotify account to get the necessary API request headers,
it can happen that a particular IP/account/User Agent can get blocked if too many requests are sent

however, if you find a way around that issue of obtaining the necessary headers yourself, you can also provide them in a JSON file via the --json_headers argument
"""

import json
import asyncio
import aiohttp
import argparse
from helpers.scraping import (
    get_credits_api_url,
    get_lyrics_api_url,
    get_internal_api_request_headers,
)
from helpers.spotify_util import get_track_uri_from_id
from helpers.util import (
    split_into_chunks_of_size,
    append_line_to_file,
    append_list_to_file,
)
import pandas as pd
from tqdm import tqdm
import os
from helpers.scraping import get_spotify_credentials
import requests
import random
import multiprocessing


# simpler, but slower synchronous version of main function
def sync_main(
    urls: list,
    headers: dict,
    output_path: str,
    error_log_path: str,
    track_uris: list = None,
):
    for i, url in enumerate(tqdm(urls)):
        response = process_url_with_requests(
            url=url,
            headers=headers,
            track_uri=track_uris[i] if track_uris is not None else None,
        )
        print(response)
        if "error" in response:
            append_line_to_file(
                line=json.dumps(response),
                file_path=error_log_path,
            )
        else:
            append_line_to_file(
                line=json.dumps(response),
                file_path=output_path,
            )


def process_url_with_requests(
    url: str,
    headers: dict,
    track_uri: str = None,
):
    print(url)
    print(json.dumps(headers))
    print(track_uri)
    try:
        # print("Making request to url: ", url)
        response = requests.get(url, headers=headers)
        # print("Got response: ", response)
        response_dict = response.json()
        # print("Got response dict: ", response_dict)
        if track_uri is not None:
            response_dict["trackUri"] = track_uri
        # NOTE: this may contain an error! check for the "error" key
        return response_dict

    except Exception:
        # backend threw some other error that is not in the form of a JSON object
        # usually this is java.lang.RuntimeException: Invalid response from entity service: [SERVICE_UNAVAILABLE]
        print(f"Error while processing {url}")
        result = {
            "error": response.text,
            "status_code": response.status_code,
        }
        if track_uri is not None:
            result["trackUri"] = track_uri
        return result


def worker(
    input_queue: multiprocessing.Queue,
    results_queue: multiprocessing.Queue,
    headers: dict,
):
    while True:
        url, track_uri = input_queue.get()
        if url is None:
            break
        response = process_url_with_requests(
            url=url,
            headers=headers,
            track_uri=track_uri,
        )
        results_queue.put(response)


async def send_request(session, url, headers):
    async with session.get(url, headers=headers) as response:
        # for some reason, internal API returns response with text content type (at least for credits), even though the response is JSON
        asyncio.sleep(random.uniform(0, 10))
        return await response.text()


async def async_main(urls, headers, error_log_path, track_uris: list = None):
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
                else:
                    if track_uris is not None:
                        responses[i]["trackUri"] = track_uris[i]

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
    if df.shape[0] == 0:
        # file is empty
        return set()
    track_ids = set(
        df["trackUri"].apply(lambda uri: uri.split(":")[2]).unique().tolist()
    )  # probably some more efficient way to do this exists
    return track_ids


def read_json(file_path: str):
    with open(file_path, "r") as f:
        return json.load(f)


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
        help="Path where the JSONL file containing the data will be saved (this file path will also be used to resume the data fetching process (skipping track_ids that are already contained in it). Defaults to '<resource>.jsonl' where <resource> is the value of the --resource argument), located in same directory as the input file.",
    )
    parser.add_argument(
        "-r",
        "--resource",
        type=str,
        help='The type of resource to fetch. Must be either "credits" or "lyrics".',
        required=True,
    )
    parser.add_argument(
        "-j",
        "--json_headers_path",
        type=str,
        help="Path to a JSON file containing the headers to be used for the requests.",
    )

    args = parser.parse_args()

    resource = args.resource
    api_url_getters = {
        "credits": get_credits_api_url,
        "lyrics": get_lyrics_api_url,
    }

    url_getter = api_url_getters[resource]
    if url_getter is None:
        raise ValueError(
            f"Invalid resource '{resource}'. Must be either 'credits' or 'lyrics'."
        )

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

    output_path = (
        args.output_path
        if args.output_path
        else os.path.join(os.path.dirname(input_path), f"{args.resource}.jsonl")
    )
    print(f"Output file path: {output_path}")
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
                print("No track IDs left to fetch data for!")
                exit(0)
            else:
                print(f"Data is still missing for {len(track_ids)} track IDs")
    else:
        # create output file and required subdirectories if they don't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        open(output_path, "w").close()

    error_log_path = output_path.replace(".jsonl", "_errors.log")

    print(f"Fetching data for {len(track_ids)} track IDs")

    headers_file_path = args.json_headers_path
    headers = None
    if headers_file_path is not None:
        headers = read_json(headers_file_path)
        print(f"Using headers from file '{headers_file_path}'")
        print(
            "IMPORTANT: Assuming that the headers are of a session where a user is logged in."
        )

    credentials_required = {
        "credits": False,
        "lyrics": True,
    }

    if headers is None:
        headers = get_internal_api_request_headers(
            track_ids=track_ids,
            credentials=get_spotify_credentials()
            if credentials_required[resource]
            else None,
        )

    run_in_parallel = False
    # run_in_parallel = (
    #     resource == "credits"
    # )  # for some reason, lyrics API fails VERY frequently when run in parallel, so for now I go with this solution

    if not run_in_parallel:
        # run requests sequentially - much simpler to reason about lol
        valid_responses = sync_main(
            urls=[url_getter(t_id) for t_id in track_ids],
            headers=headers,
            # lyrics API responses don't contain track URIs, so we need to add them manually after receiving the responses
            track_uris=[get_track_uri_from_id(t_id) for t_id in track_ids]
            if resource != "credits"
            else None,
            output_path=output_path,
            error_log_path=error_log_path,
        )

    else:
        if (
            resource == "credits"
        ):  # this only (more or less) works for credits API, I couldn't figure out how to make it work for lyrics API
            max_requests = 512  # cannot send too many requests at once (internal API will fail too often) - this number seems to work well for credits (atm, on my machine, lyrics API fails too often even with 1 request at a time)
            id_chunks = split_into_chunks_of_size(track_ids, max_requests)
            print(
                f"Processing {len(id_chunks)} ID chunks of (maximum) size {max_requests}"
            )
            with tqdm(total=len(track_ids)) as pbar_total:
                for current_inputs in id_chunks:
                    processed_ids = []
                    remaining_ids = current_inputs
                    valid_responses = []
                    while not len(remaining_ids) == 0:
                        previous_number_of_ids_to_process = len(remaining_ids)
                        # print(f"Processing {len(remaining_ids)} IDs")
                        urls = [url_getter(t_id) for t_id in remaining_ids]

                        valid_responses = valid_responses + asyncio.run(
                            async_main(
                                urls=urls,
                                headers=headers,
                                error_log_path=error_log_path,
                                # lyrics API responses don't contain track URIs, so we need to add them manually after receiving the responses
                                track_uris=[
                                    get_track_uri_from_id(t_id)
                                    for t_id in current_inputs
                                ]
                                if resource != "credits"
                                else None,
                            )
                        )

                        processed_ids = processed_ids + (
                            [
                                response["trackUri"].split(":")[2]
                                for response in valid_responses
                            ]
                        )
                        remaining_ids = [
                            t_id for t_id in current_inputs if t_id not in processed_ids
                        ]
                        if len(remaining_ids) == previous_number_of_ids_to_process:
                            print(
                                f"Failed to process {len(remaining_ids)} IDs. Skipping them. Check '{error_log_path}' for errors that occurred."
                            )
                            remaining_ids = []

                    append_list_to_file(
                        [json.dumps(r) for r in valid_responses], output_path
                    )
                    processed = len(processed_ids)
                    pbar_total.update(processed)
        else:
            num_processes = 1
            # num_processes = min(8, os.cpu_count() - 1)
            worker_input_queue = multiprocessing.Queue()
            results_queue = multiprocessing.Queue()
            pool = multiprocessing.Pool(
                processes=num_processes,
                initializer=worker,
                initargs=(
                    worker_input_queue,
                    results_queue,
                    headers,
                ),
            )

            inputs = [
                (
                    url_getter(t_id),
                    get_track_uri_from_id(t_id) if resource != "credits" else None,
                )
                for t_id in track_ids
            ]

            chunks = split_into_chunks_of_size(inputs, num_processes)
            with tqdm(total=len(track_ids)) as pbar_total:
                for current_inputs in chunks:
                    for wi in current_inputs:
                        worker_input_queue.put(wi)
                        # print("sent input to worker")
                    received_results = 0
                    while received_results < len(current_inputs):
                        # print("waiting for result")
                        result = results_queue.get()
                        # print("got result", result)
                        if result is not None:
                            append_line_to_file(
                                json.dumps(result),
                                error_log_path if "error" in result else output_path,
                            )
                            received_results += 1
                            pbar_total.update(1)

                # send sentinel values to stop the workers
                for _ in range(num_processes):
                    worker_input_queue.put(None)
