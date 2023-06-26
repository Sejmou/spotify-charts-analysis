# Fetches track metadata for tracks on Spotify using spotipy (Python wrapper for Spotify API).
# Currently this script runs on a single thread. It could be sped up by using multiple threads.
# However, this is still fast enough for our purposes (and MUCH faster than the web scraping approach used for downloading the Spotify Chart data).

import argparse
from tqdm import tqdm
import os
import pandas as pd
from helpers.spotipy_util import create_spotipy_client
from helpers.util import split_into_chunks_of_size


def process_track_data_from_api(
    data: pd.Series, track_artists: list, track_markets: list
):
    """
    Processes the track data returned by the Spotify API.

    Args:
        data: A pandas Series containing the track data returned by the Spotify API.
        track_artists: A list of tuples of shape ('track_id', 'artist_id', 'pos').
        track_markets: A list of tuples of shape ('track_id', 'market').
    """

    for source, url in data["external_urls"].items():
        if source != "spotify":
            data[source] = url

    for platform, p_id in data["external_ids"].items():
        data[platform] = p_id
    data["album"] = data["album"]["id"]

    artist_ids = [artist["id"] for artist in data["artists"]]
    for i, artist_id in enumerate(artist_ids):
        track_artists.append((data["id"], artist_id, i))

    for market in data["available_markets"]:
        track_markets.append((data["id"], market))

    attrs_to_drop = [
        "type",  # always 'track'
        "uri",  # can be derived from 'id'
        "href",  # can be derived from 'id'
        "artists",  # already processed
        "external_urls",  # already processed
        "external_ids",  # already processed
        "available_markets",  # already processed
        "is_local",  # always False
        "popularity",  # constantly changing, not useful for static analysis
    ]

    return data.drop(labels=attrs_to_drop)


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
        "--output_dir",
        type=str,
        help="Path to folder where output files with track metadata will be stored.",
        required=True,
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

    output_dir = args.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    spotify = create_spotipy_client()

    metadata_dfs = (
        []
    )  # list of dataframes with track metadata of shape (track_id, metadata_1, ..., metadata_n)
    track_artists = []  # tuples of shape ('track_id', 'artist_id', 'pos')
    track_markets = []  # tuples of shape ('track_id', 'market')

    chunk_size = 50
    track_ids_chunks = split_into_chunks_of_size(track_ids, chunk_size)
    print(f"Fetching data in {len(track_ids_chunks)} chunks of size {chunk_size}...")

    with tqdm(total=len(track_ids_chunks)) as pbar:
        for track_ids in track_ids_chunks:
            api_resp = pd.DataFrame(spotify.tracks(track_ids)["tracks"])
            metadata = api_resp.apply(
                process_track_data_from_api,
                axis=1,
                track_artists=track_artists,
                track_markets=track_markets,
            )
            metadata_dfs.append(metadata)
            pbar.update(1)

    metadata_df = pd.concat(metadata_dfs, ignore_index=True)

    metadata_df.to_parquet(os.path.join(output_dir, "track_metadata.parquet"))
    print(f"Saved track metadata to '{output_dir}/track_metadata.parquet'")

    track_artists_df = pd.DataFrame(
        track_artists, columns=["track_id", "artist_id", "pos"]
    )
    track_artists_df.to_parquet(os.path.join(output_dir, "track_artists.parquet"))
    print(f"Saved track artists to '{output_dir}/track_artists.parquet'")

    track_markets_df = pd.DataFrame(track_markets, columns=["track_id", "market"])
    track_markets_df.to_parquet(os.path.join(output_dir, "track_markets.parquet"))
    print(f"Saved track markets to '{output_dir}/track_markets.parquet'")
