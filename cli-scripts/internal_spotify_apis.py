"""
Gets data for track IDs from internal Spotify APIs.
Currently supports:
- Credits
- Lyrics

Usage is a bit complicated, check the script help (`python use_internal_spotify_apis.py --help`) and the implementation for more information.
"""

import json
import argparse
from helpers.scraping import (
    get_credits_api_url,
    get_lyrics_api_url,
    get_internal_api_request_headers,
)
from helpers.util import (
    split_into_chunks_of_size,
    append_line_to_file,
    append_list_to_file,
)
import pandas as pd
from tqdm import tqdm
import os
import requests
from typing import Callable, List
from helpers.scraping import get_spotify_credentials


def sync_main(
    url_getter: Callable[[str], str],
    headers: dict,
    output_path: str,
    error_log_path: str,
    track_ids: List[str],
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
    """
    for t_id in tqdm(track_ids):
        response = process_url_with_requests(
            url=url_getter(t_id), headers=headers, track_id=t_id
        )
        process_result_dict(
            result=response,
            output_path=output_path,
            error_log_path=error_log_path,
        )


def create_result_dict(status_code: int, content: str, url: str, track_id: str):
    """
    Creates a dictionary containing information about the result of request to an internal Spotify API.

    Parameters
    ----------
    status_code: int
        The status code of the response
    content: str
        The content of the response (usually a JSON string, but we cannot be sure, some APIs don't return JSON)
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
        The path to the file where errors should be written
    """
    if result["status_code"] != 200 or "error" in result["content"]:
        # something went wrong
        append_line_to_file(
            line=json.dumps(result),
            file_path=error_log_path,
        )
    else:
        append_line_to_file(
            line=json.dumps(result["content"]),
            file_path=output_path,
        )


def process_url_with_requests(url: str, headers: dict, track_id: str):
    response = requests.get(url, headers=headers)
    status_code = response.status_code
    content = response.text
    return create_result_dict(
        status_code=status_code,
        content=content,
        url=url,
        track_id=track_id,
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
        help="Path to a JSON file containing the headers to be used for the requests.",
    )
    parser.add_argument(
        "-m",
        "--markets_path",
        type=str,
        help="Path to a .parquet file containing the markets the provided track IDs are available in (in a column named 'markets'). If provided, only tracks that are available in the market associated with the current IP address will be fetched. This seems to be necessary for the lyrics API (as all tracks not available in a user's market return a 400 error).",
    )

    args = parser.parse_args()

    resources = {
        "credits": {
            "output_filename": "credits",
            "url_getter": get_credits_api_url,
            "requires_login": False,
        },
        "lyrics": {
            "output_filename": "lyrics",
            "url_getter": get_lyrics_api_url,
            "requires_login": True,
        },
    }

    resource = resources[args.resource]
    if resource is None:
        raise ValueError(
            f"Invalid resource '{args.resource}'. Must be one of {list(resources.keys())}."
        )

    input_path = args.input_path
    try:
        tracks_df = pd.read_parquet(input_path)
    except Exception:
        raise ValueError(f"Input file '{input_path}' must be a .parquet file")

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
            if markets_df.shape[0] == 0:
                raise ValueError(
                    f"No markets found for '{market}' in markets file '{markets_path}'. Are you sure this is a valid Spotify market code?"
                )
        except Exception:
            raise ValueError(
                f"Markets file '{markets_path}' must be a .parquet file with a column named 'market'"
            )
        try:
            tracks_df = tracks_df[tracks_df["track_id"].isin(markets_df.index)]
            track_ids = tracks_df["track_id"].unique().tolist()
            print(f"Found {len(track_ids)} tracks available in market '{market}'")
        except Exception:
            raise ValueError(
                f"Input file '{input_path}' must contain a column named 'track_id'"
            )
    else:
        try:
            track_ids = tracks_df["track_id"].unique().tolist()
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

    error_log_path = output_path.replace(".jsonl", "_errors.jsonl")

    print(f"Fetching data for {len(track_ids)} track IDs")

    credentials_required = resource["requires_login"]
    headers_file_path = args.json_headers_path
    headers = None
    if headers_file_path is not None:
        headers = read_json(headers_file_path)
        print(f"Using headers from file '{headers_file_path}'")
        if credentials_required:
            print(
                "WARNING: provided headers must belong to a session where the user is logged in, otherwise the requests will fail."
            )

    if headers is None:
        headers = get_internal_api_request_headers(
            track_ids=track_ids,
            credentials=get_spotify_credentials() if credentials_required else None,
        )

    url_getter = resource["url_getter"]
    sync_main(
        url_getter=url_getter,
        track_ids=track_ids,
        headers=headers,
        output_path=output_path,
        error_log_path=error_log_path,
    )
