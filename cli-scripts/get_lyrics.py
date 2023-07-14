import json
import asyncio
import aiohttp
import argparse
from helpers.scraping import get_lyrics_api_url, get_internal_api_request_headers
from helpers.util import split_into_chunks_of_size
import pandas as pd
from tqdm import tqdm
import os
import random


async def send_request(session, url, headers):
    async with session.get(url, headers=headers) as response:
        # for some reason, internal API returns response with text content type, even though the response is JSON
        response_text = await response.text()
        return response_text


async def async_main(urls, headers):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            task = asyncio.create_task(send_request(session, url, headers))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)

        for i, response in enumerate(responses):
            try:
                responses[i] = json.loads(response)
            except Exception:
                # print(f"Failed to parse response {i}: {response}")
                responses[i] = {}

        valid_responses = [r for r in responses if r != {}]
        return valid_responses


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
        help="Path where the JSONL file containing the lyric data will be saved (this file path will also be used to resume the data fetching process (skipping track_ids that are already contained in it).",
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
        print(f"Found {len(track_ids)} unique track IDs in '{input_path}'")
    except Exception:
        raise ValueError(
            f"Input file '{input_path}' must contain a column named 'track_id'"
        )

    output_path = args.output_path
    if os.path.exists(output_path):
        # load ids from the existing JSON objects in the file
        with open(output_path, "r") as f:
            existing_data = [json.loads(line) for line in f.readlines()]
            existing_track_ids = set([obj["trackId"] for obj in existing_data])
            print(
                f"Found {len(existing_track_ids)} existing track IDs in '{output_path}'"
            )
            track_ids = [
                track_id for track_id in track_ids if track_id not in existing_track_ids
            ]
            print(f"Fetching data for {len(track_ids)} track IDs")
    else:
        # create output file and required subdirectories if they don't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        open(output_path, "w").close()

    max_requests = (
        32  # cannot send too many requests at once (internal API will fail too often)
    )
    track_id_chunks = split_into_chunks_of_size(track_ids, max_requests)

    def pick_random_track_id_for_request_header_fetching():
        # not sure if this is necessary, but maybe Spotify API will block large volumes of requests earlier if they originate from the same track ID
        return random.choice(track_ids)

    print(
        f"Processing {len(track_id_chunks)} track ID chunks of (maximum) size {max_requests}"
    )
    with tqdm(total=len(track_id_chunks)) as pbar:
        for track_ids in track_id_chunks:
            responses_saved = False
            while not responses_saved:
                ids_to_process = len(track_ids)
                urls = [get_lyrics_api_url(track_id) for track_id in track_ids]
                headers = get_internal_api_request_headers(
                    pick_random_track_id_for_request_header_fetching()
                )
                valid_responses = asyncio.run(async_main(urls=urls, headers=headers))

                invalid_response_count = ids_to_process - len(valid_responses)
                while invalid_response_count > 0:
                    previous_invalid_response_count = invalid_response_count
                    # print(
                    #     f"Encountered {invalid_response_count} invalid responses. Retrying..."
                    # )
                    track_ids_with_valid_responses = [
                        response["trackUri"].split(":")[2]
                        for response in valid_responses
                    ]
                    track_ids = [
                        track_id
                        for track_id in track_ids
                        if track_id not in track_ids_with_valid_responses
                    ]
                    # print(f"Retrying with remaining {len(track_ids)} track IDs")
                    urls = [get_lyrics_api_url(track_id) for track_id in track_ids]
                    headers = get_internal_api_request_headers(
                        pick_random_track_id_for_request_header_fetching()
                    )
                    responses_next_attempt = asyncio.run(
                        async_main(urls=urls, headers=headers)
                    )
                    valid_responses.extend(responses_next_attempt)
                    invalid_response_count = ids_to_process - len(valid_responses)
                    print(
                        invalid_response_count,
                        previous_invalid_response_count,
                    )
                    if invalid_response_count == previous_invalid_response_count:
                        print(
                            f"Failed to process {invalid_response_count} track IDs. Skipping them and writing to file..."
                        )
                        # write track IDs with missing responses to file (same directory as output_path)
                        with open(
                            os.path.join(
                                os.path.dirname(output_path),
                                f"missing_lyrics_responses.txt",
                            ),
                            "a",
                        ) as f:
                            for track_id in track_ids:
                                f.write(f"{track_id}\n")
                        invalid_response_count = 0

                    responses_saved = True

                # Save all valid responses by appending to JSONL file (i.e. file with one JSON object per line) in provided output_path
                with open(output_path, "a") as f:
                    for response in valid_responses:
                        f.write(json.dumps(response) + "\n")
                # print(f"Saved {len(valid_responses)} responses to '{output_path}'")
                pbar.update(1)
