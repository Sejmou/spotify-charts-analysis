"""
Gets data for track IDs from internal Spotify APIs.
Currently supports:
- Credits
- Lyrics

Usage is a bit complicated, check the script help (`python use_internal_spotify_apis.py --help`) and the implementation for more information.

Features both a synchronous and an asynchronous implementation. The async implementation doesn't work properly atm (probably mainly because of rate limiting), especially for the lyrics.
"""

import json
import argparse
import aiohttp
import asyncio
import pandas as pd
from tqdm import tqdm
import os
import requests
from typing import Callable, List
import random
import time
from collections import defaultdict

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
    track_ids: List[str],
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
    result = create_result_dict(
        status_code=status_code,
        content=content,
        url=request_url,
        track_id=track_id,
    )
    process_result_dict(
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
    track_ids: List[str],
    parallel_requests: int = 1,
):
    # limit number of simultaneous requests to same endpoint: https://stackoverflow.com/a/43857526/13727176
    connector = aiohttp.TCPConnector(limit_per_host=parallel_requests)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        chunk_size = parallel_requests
        id_chunks = split_into_chunks_of_size(track_ids, chunk_size)
        print(
            f"Splitting track IDs into {len(id_chunks)} chunks of size {chunk_size} (IDs within each chunk will be processed in parallel)"
        )
        print("Data will be written to disk after every chunk is processed")
        request_without_429 = 0
        for id_chunk in tqdm(id_chunks):
            urls = [url_getter(t_id) for t_id in id_chunk]
            tasks = [
                asyncio.create_task(
                    process_url_with_aiohttp(
                        session=session, url=url, headers=headers, track_id=t_id
                    )
                )
                for (url, t_id) in zip(urls, id_chunk)
            ]
            results = await asyncio.gather(*tasks)
            for result in results:
                process_result_dict(
                    result=result,
                    output_path=output_path,
                    error_log_path=error_log_path,
                )
                if result["status_code"] != 429:
                    request_without_429 += 1
                else:
                    print(f"Got 429 status code after {request_without_429} requests")
                    request_without_429 = 0
                    print("Waiting 60 seconds")
                    await asyncio.sleep(60)


async def process_url_with_aiohttp(
    session: aiohttp.ClientSession, url: str, headers: dict, track_id: str
):
    async with session.get(url, headers=headers) as response:
        status_code = response.status
        content = await response.text()
        return create_result_dict(
            status_code=status_code,
            content=content,
            url=url,
            track_id=track_id,
        )


def create_result_dict(status_code: int, content: str, url: str, track_id: str):
    """
    Creates a dictionary containing information about the result of request to an internal Spotify API.

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
        A JSON object containing information about the API response with the following fields:
        - status_code: int
            The status code of the response
        - content: dict
            The content of the response, together with a "content_type" field that indicates whether the response was JSON or text (if it was text, there is a single "content" field, if it was JSON, all other fields of the response are included)
        - url: str
            The URL that was requested
        - track_id: str
            The ID of the track that was requested
    """
    try:
        content = json.loads(content)
        content["content_type"] = "json"
    except Exception:
        content = {"content_type": "text", "content": content}
    return {
        "status_code": status_code,
        "content": {**content, "trackId": track_id},
        "url": url,
        "track_id": track_id,
    }


def process_result_dict(result: dict, output_path: str, error_log_path: str):
    """
    Processes a result dictionary and writes it to the appropriate file.

    Parameters
    ----------
    result: dict
        The result dictionary as returned by create_result_dict
    output_path: str
        The path to the file where the results should be written
    error_log_path: str
        The path to the file where non-recoverable errors should be written
    """
    if (
        result["status_code"] != 200 and result["status_code"] != 429
    ) or "error" in result["content"]:
        # something went wrong
        # print(f'Got status code {result["status_code"]}')
        append_line_to_file(
            line=json.dumps(result),
            file_path=error_log_path,
        )
    else:
        append_line_to_file(
            line=json.dumps(result["content"]),
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
        df["trackId"].unique().tolist()
    )  # probably some more efficient way to do this exists
    return track_ids


def read_json(file_path: str):
    with open(file_path, "r") as f:
        return json.load(f)


import requests


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
        "-j",
        "--json_headers_path",
        type=str,
        help="Path to a JSON file containing the headers to be used for the requests. Not recommended, as headers can expire, causing the script to fail.",
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
        help="The number of parallel requests to send. A value of 1 is synonymous with synchronous processing.",
        default=10,
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
    else:
        # create output file and required subdirectories if they don't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        open(output_path, "w").close()

    error_log_path = output_path.replace(".jsonl", "_errors.jsonl")
    print(f"Error log path: {error_log_path}")
    errors = (
        pd.read_json(error_log_path, lines=True)
        if os.path.exists(error_log_path)
        else pd.DataFrame()
    )

    # identify 403 or 404 errors in logs and skip associated track IDs
    errors = errors[errors["status_code"].isin([403, 404])]
    error_ids = set(errors["track_id"].unique())
    print(f"Found {len(error_ids)} track IDs with 403 or 404 errors (will be skipped)")
    track_ids = track_ids.difference(error_ids)

    print(f"Fetching data for {len(track_ids)} track IDs")

    headers_file_path = args.json_headers_path
    if headers_file_path is not None:
        get_headers = lambda: read_json(headers_file_path)  # just read from path again
        print(
            f"Using headers from file '{headers_file_path}' (will also 'refetch' from there in case of 401 error)"
        )
        if endpoint["requires_login"]:
            print(
                "WARNING: provided headers must belong to a session where the user is logged in, otherwise the requests will fail."
            )
    else:
        get_headers = InternalRequestHeadersGetter(
            resource_name=args.resource, track_ids=track_ids
        ).get_headers

    headers = get_headers()
    url_getter = endpoint["url_getter"]

    parallel_requests = args.parallel_requests
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
