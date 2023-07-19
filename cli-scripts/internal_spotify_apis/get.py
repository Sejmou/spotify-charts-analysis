"""
Gets data for track IDs from internal Spotify APIs.
Currently supports:
- Credits
- Lyrics

How it works:
The target endpoint (together with a .parquet file containing the track IDs to fetch data for in a 'track_id' column)
has to be specified as a command line argument.

Output is a .jsonl file with the API responses (one JSON object per track ID).
Another .jsonl file is created for logging errors.

The exact usage is a bit more complicated and more parameters/input args are supported, check the script help
(`python use_internal_spotify_apis.py --help`) and the implementation for more information.

Features both a synchronous and an asynchronous implementation.

Known issues: 
 - The login (required for the lyrics script) can get blocked if the script is rerun too often without specifying
   cookies belonging to a logged in Spotify account (obtained via `save_cookies.py`).
 - If the lyrics script runs for a while (approximately two to three hours), it can happen that it gets stuck
   because the webdriver is no longer logged in. This can be fixed by running the save_cookies.py script again,
   logging into Spotify and replacing the old cookies file with the new one.
"""

import json
import argparse
import aiohttp
import asyncio
import pandas as pd
from tqdm import tqdm
import os
import requests
from typing import Callable, Set
import random
import time
from collections import defaultdict
import datetime

from helpers.util import (
    split_into_chunks_of_size,
    append_line_to_file,
)
from helpers.scraping import internal_api_endpoints, InternalRequestHeadersGetter


def sync_main(
    url_getter: Callable[[str], str],
    headers: dict,
    output_path: str,
    error_log_path: str,
    track_ids: Set[str],
    new_headers_getter: Callable[[], dict],
):
    """
    Makes requests to an internal Spotify API for a list of track IDs and saves the results to the .jsonl output file path if no error was detected.
    If something went wrong, the error is saved to the .jsonl error log file path.

    Parameters
    ----------
    url_getter: Callable[[str], str]
        A function that takes a track ID and returns the URL to request data for that track from an internal Spotify API
    headers: dict
        The headers to use for the requests
    output_path: str
        The path to the .jsonl file to save the results to
    error_log_path: str
        The path to the .jsonl file to save errors to
    track_ids: List[str]
        The track IDs to request data for
    new_headers_getter: Callable[[], dict]
        A function that returns new request headers. Used if a 401 status code is received
    """

    requests_without_429 = 0
    status_code_counts = defaultdict(int)
    processed_count = 0

    for t_id in tqdm(track_ids):
        if processed_count > 0 and processed_count % 100 == 0:
            print(f"Status code stats after {processed_count} processed tracks:")
            print(status_code_counts)
        while True:
            status_code = process_track_id_sync(
                track_id=t_id,
                request_url=url_getter(t_id),
                request_headers=headers,
                output_path=output_path,
                error_log_path=error_log_path,
            )
            if status_code != 429:
                requests_without_429 += 1
                if status_code != 401:
                    processed_count += 1
                    status_code_counts[status_code] += 1
                else:
                    print("Got 401 status code, getting new headers")
                    headers = new_headers_getter()
                break
            else:
                print(f"Got 429 status code after {requests_without_429} requests")
                requests_without_429 = 0
                wait_time = random.uniform(1, 5)
                print(f"Waiting {wait_time} seconds before trying again")
                time.sleep(wait_time)


def process_url_with_requests(url: str, headers: dict):
    response = requests.get(url, headers=headers)
    status_code = response.status_code
    content = response.text
    return status_code, content


def process_track_id_sync(
    track_id: str,
    request_url: str,
    request_headers: dict,
    output_path: str,
    error_log_path: str,
    max_request_wait_time=0.3,
):
    random_wait_time = random.uniform(0, max_request_wait_time)
    time.sleep(random_wait_time)
    status_code, content = process_url_with_requests(
        url=request_url, headers=request_headers
    )
    result = create_response_dict(
        status_code=status_code,
        content=content,
        url=request_url,
        track_id=track_id,
    )
    process_response_dict(
        result=result,
        output_path=output_path,
        error_log_path=error_log_path,
    )
    return result["status_code"]


async def async_main(
    url_getter: Callable[[str], str],
    headers: dict,
    output_path: str,
    error_log_path: str,
    track_ids: Set[str],
    new_headers_getter: Callable[[], dict],
    parallel_requests: int = 1,
):
    # limit number of simultaneous requests to same endpoint: https://stackoverflow.com/a/43857526/13727176
    connector = aiohttp.TCPConnector(limit_per_host=parallel_requests)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        chunk_size = parallel_requests
        id_chunks = split_into_chunks_of_size(list(track_ids), chunk_size)
        print(
            f"Splitting track IDs into {len(id_chunks)} chunks of size {chunk_size} (IDs within each chunk will be processed in parallel)"
        )
        print("Data will be written to disk after every chunk is processed")
        chunks_without_429 = 0
        status_code_counts = defaultdict(int)
        processed_count = 0

        for id_chunk in tqdm(id_chunks):
            urls = [url_getter(t_id) for t_id in id_chunk]
            tasks = [
                asyncio.create_task(
                    get_data_async(
                        track_id=t_id,
                        request_url=url,
                        request_headers=headers,
                        session=session,
                        time_before_sending_request=i
                        * random.uniform(
                            0.05, 0.1
                        ),  # don't send each request at the exact same time
                    )
                )
                for i, (url, t_id) in enumerate(zip(urls, id_chunk))
            ]
            results = await asyncio.gather(*tasks)
            chunk_status_code_counts = defaultdict(int)

            for result in results:
                process_response_dict(
                    result=result,
                    output_path=output_path,
                    error_log_path=error_log_path,
                )
                chunk_status_code_counts[result["status_code"]] += 1

            if chunk_status_code_counts[429] > 0:
                print(
                    f"Received 429 status code {chunk_status_code_counts[429]} times in current chunk (after {chunks_without_429} chunks without any)"
                )
                chunks_without_429 = 0
                wait_time = random.uniform(1, 5)
                print(f"Waiting {wait_time} seconds before proceeding")
                await asyncio.sleep(wait_time)
            if chunk_status_code_counts[401] > 0:
                print(
                    f"Got 401 status code for {chunk_status_code_counts[401]} requests, getting new headers"
                )
                headers = new_headers_getter()

            results_with_401_or_429 = [
                r for r in results if r["status_code"] == 401 or r["status_code"] == 429
            ]
            ids_to_refetch = [r["url"] for r in results_with_401_or_429]

            if len(ids_to_refetch) > 0:
                print(
                    f"Fetching missing data for {len(ids_to_refetch)} track IDs synchronously"
                )
                start_time = time.time()
                for url in ids_to_refetch:
                    while True:
                        status_code = process_track_id_sync(
                            track_id=result["track_id"],
                            request_url=url,
                            request_headers=headers,
                            output_path=output_path,
                            error_log_path=error_log_path,
                        )
                        chunk_status_code_counts[status_code] += 1
                        if status_code != 401 and status_code != 429:
                            break
                        else:
                            if status_code == 401:
                                print("Got 401 status code, getting new headers")
                                headers = new_headers_getter()
                            elif status_code == 429:
                                print("Got 429 status code, waiting a second")
                                time.sleep(1)
                duration = time.time() - start_time
                print(f"Took {duration} seconds to fetch missing data")

            # add chunk status code counts to overall status code counts
            for status_code, count in chunk_status_code_counts.items():
                status_code_counts[status_code] += count

            processed_count += len(results)

            print(f"Status code counts after processing {processed_count} track IDs:")
            print(status_code_counts)


async def get_data_async(
    track_id: str,
    request_url: str,
    request_headers: dict,
    session: aiohttp.ClientSession,
    time_before_sending_request=0.0,
):
    await asyncio.sleep(time_before_sending_request)
    async with session.get(request_url, headers=request_headers) as response:
        status_code = response.status
        content = await response.text()

    response_data = create_response_dict(
        status_code=status_code,
        content=content,
        url=request_url,
        track_id=track_id,
    )
    return response_data


def create_response_dict(status_code: int, content: str, url: str, track_id: str):
    """
    Creates a dictionary containing information about an API response to an internal Spotify API.

    Parameters
    ----------
    status_code: int
        The status code of the response
    content: str
        The content of the response (usually a JSON string, but we cannot be sure - some APIs don't return JSON)
    url: str
        The URL that was requested
    track_id: str
        The ID of the track that was requested

    Returns
    -------
    dict
        A dictionary containing the information about the response with the following fields:
        - status_code: int
            The status code of the response
        - content: str or dict
            The content of the response
        - content_type: str
            The content type of the response (either 'json' or 'text')
        - url: str
            The URL that was requested
        - track_id: str
            The ID of the track for which data was requested
        - timestamp: str
            The timestamp when the response was received (in UTC time)
    """
    try:
        content = json.loads(content)
        content_type = "json"
    except Exception:
        content = content
        content_type = "text"
    return {
        "status_code": status_code,
        "content": content,
        "content_type": content_type,
        "url": url,
        "track_id": track_id,
        "timestamp": datetime.datetime.utcnow().strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ),  # this timestamp should be easily parsable; using UTC time to avoid issues with timezones
    }


def process_response_dict(result: dict, output_path: str, error_log_path: str):
    """
    Processes a dictionary with information about an API response
    and writes it to the appropriate file.

    Parameters
    ----------
    result: dict
        The result dictionary as returned by create_result_dict
    output_path: str
        The path to the file where responses with usable response data should be written
    error_log_path: str
        The path to the file where responses with errors should be written
    """
    if (result["status_code"] != 200) or "error" in result["content"]:
        # something went wrong
        # print(f'Got status code {result["status_code"]}')
        append_line_to_file(
            line=json.dumps(result),
            file_path=error_log_path,
        )
    else:
        append_line_to_file(
            line=json.dumps(result),
            file_path=output_path,
        )


def get_existing_track_ids(jsonl_file_path: str):
    """
    Returns a set of track IDs that are already contained in the JSONL file, reading the IDs from the 'trackId' field of each JSON in each line.
    """
    df = pd.read_json(jsonl_file_path, lines=True)
    if df.shape[0] == 0:
        # file is empty
        return set()
    track_ids = set(
        df["track_id"].unique().tolist()
    )  # probably some more efficient way to do this exists
    return track_ids


def read_json(file_path: str):
    with open(file_path, "r") as f:
        return json.load(f)


def ip_info(addr=""):
    """
    Fetches IP information from ipinfo.io.

    Args:
        addr (str): IP address to fetch information for. If not provided, the information for the current IP is fetched.

    Returns:
        None

    Prints the retrieved IP information, including various attributes and their corresponding values.
    If an error occurs during the request, an appropriate error message is printed.
    """

    url = f"https://ipinfo.io/{addr}/json" if addr else "https://ipinfo.io/json"
    response = requests.get(url)

    if response.ok:
        data = response.json()
        return data
    else:
        raise Exception("Error occurred while fetching IP information.")


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
        "-m",
        "--markets_path",
        type=str,
        help="Path to a .parquet file containing the markets the provided track IDs are available in (in a column named 'markets'). If provided, only tracks that are available in the market associated with the current IP address will be fetched. This seems to be necessary for the lyrics API (as all tracks not available in a user's market return a 400 error).",
    )
    parser.add_argument(
        "-p",
        "--parallel_requests",
        type=int,
        help="The number of parallel requests to send. A value of 1 is synonymous with synchronous processing. If not provided, a sensible default value will be used depending on the endpoint.",
    )
    parser.add_argument(
        "-c",
        "--cookies-path",
        type=str,
        help="Path to a Pickle file containing the cookies of a Spotify 'session' (where cookies were accepted and user is logged in (if login required by endpoint)). Used for getting the request headers. If you don't have a file yet, created one with `save_cookies.py`).",
    )

    args = parser.parse_args()

    endpoint = internal_api_endpoints[args.resource]
    if endpoint is None:
        raise ValueError(
            f"Invalid resource '{args.resource}'. Must be one of {list(internal_api_endpoints.keys())}."
        )

    input_path = args.input_path
    try:
        tracks_df = pd.read_parquet(input_path)
        track_ids = set(tracks_df["track_id"].unique())
        print(f'Found {len(track_ids)} track IDs in input file "{input_path}".')
        print()
    except Exception:
        raise ValueError(
            f"Input file '{input_path}' must be a .parquet file with a column named 'track_id'."
        )

    markets_path = args.markets_path
    if markets_path is not None:
        data = ip_info()
        market = data["country"]
        print(f"Markets file provided: '{markets_path}'")
        print(
            "filtering track IDs based on country/market code associated with location of IP address making request:",
            market,
        )
        try:
            markets_df = pd.read_parquet(markets_path)
            markets_df = markets_df[markets_df.market == market]
            ids_for_market = set(markets_df.index)
            if len(ids_for_market) == 0:
                raise ValueError(
                    f"No track IDs found for '{market}' in markets file '{markets_path}'. Are you sure this is a valid Spotify market code?"
                )
        except Exception:
            raise ValueError(
                f"Markets file '{markets_path}' must be a .parquet file with a column named 'market'"
            )
        try:
            track_ids = track_ids.intersection(ids_for_market)
            print(
                f"Proceeding with {len(track_ids)} tracks available in market '{market}'"
            )
            print()
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
            track_ids = track_ids.difference(existing_track_ids)
            if len(track_ids) == 0:
                print("No track IDs left to fetch data for!")
                exit(0)
            else:
                print(f"{len(track_ids)} track IDs remain")
                print()
    else:
        # create output file and required subdirectories if they don't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        open(output_path, "w").close()

    error_log_path = output_path.replace(".jsonl", "_errors.jsonl")
    print(f"Error log path: {error_log_path}")

    if os.path.exists(error_log_path):
        errors = pd.read_json(error_log_path, lines=True)
        if errors.shape[0] == 0:
            # file is empty
            print("No errors found in error log file")
        else:
            # identify 403 or 404 errors in logs and skip associated track IDs
            errors = errors[errors["status_code"].isin([403, 404])]
            error_ids = set(errors["track_id"].unique())
            if len(error_ids) > 0:
                print(
                    f"Found {len(error_ids)} track IDs with 403 or 404 errors (will be skipped)"
                )
                track_ids = track_ids.difference(error_ids)
            print()
    else:
        print(os.path.dirname(error_log_path))
        os.makedirs(os.path.dirname(error_log_path), exist_ok=True)

    if len(track_ids) == 0:
        print("No track IDs left to fetch data for!")
        exit(0)
    else:
        print(f"Fetching data for {len(track_ids)} track IDs")

    cookies_path = args.cookies_path

    get_headers = InternalRequestHeadersGetter(
        resource_name=args.resource,
        track_ids=track_ids,
        cookies_path=cookies_path,
    ).get_headers

    headers = get_headers()
    url_getter = endpoint["url_getter"]

    # I am not even sure if varying the number of parallel requests is even that beneficial for performance lol
    # for the lyrics endpoint, we surely cannot send too much at once, as we will get 429 errors
    parallel_request_defaults = {
        "credits": 100,
        "lyrics": 50,
    }
    parallel_requests = args.parallel_requests or parallel_request_defaults.get(
        args.resource, 1
    )

    print(
        f"Sending {parallel_requests} parallel requests"
        if parallel_requests > 1
        else "Sending synchronous requests"
    )

    if parallel_requests > 1:
        asyncio.run(
            async_main(
                url_getter=url_getter,
                track_ids=track_ids,
                headers=headers,
                output_path=output_path,
                error_log_path=error_log_path,
                new_headers_getter=get_headers,
                parallel_requests=parallel_requests,
            )
        )
    else:
        sync_main(
            url_getter=url_getter,
            track_ids=track_ids,
            headers=headers,
            output_path=output_path,
            error_log_path=error_log_path,
            new_headers_getter=get_headers,
        )
