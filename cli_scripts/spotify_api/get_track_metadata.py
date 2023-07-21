import argparse
from tqdm import tqdm
import os
import pandas as pd
from helpers.spotify_util import create_spotipy_client
from helpers.util import (
    split_into_chunks_of_size,
)
from helpers.data import write_dfs_in_dict_to_parquet_files
import spotipy


def main(input_path: str, output_dir: str):
    """
    Fetches track metadata for tracks on Spotify from the Spotify API (/tracks endpoint) using spotipy.

    Three output files are generated in the specified output directory:
    - metadata.parquet: Contains the metadata for each track.
    - artists.parquet: Contains the artist IDs for each track (together with the 'position' of the artist, i.e. primary artist, secondary artist etc.).
    - markets.parquet: Contains the available markets for each track.

    Currently this script runs on a single thread. It could be sped up by using multiple threads.
    However, this is still fast enough for our purposes (and MUCH faster than the web scraping approach used for downloading the Spotify Chart data).
    """
    try:
        input_df = pd.read_parquet(input_path)
    except Exception:
        raise ValueError(f"Input file '{input_path}' must be a .parquet file")

    track_ids = (input_df)["track_id"].unique().tolist()
    print(f"Found {len(track_ids)} unique track IDs in '{input_path}'")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    spotify = create_spotipy_client()

    df_dict = get_track_metadata_from_api(track_ids=track_ids, spotify=spotify)
    write_dfs_in_dict_to_parquet_files(df_dict=df_dict, output_dir=output_dir)


def get_track_metadata_from_api(track_ids: list, spotify: spotipy.Spotify):
    artists = []  # tuples of shape ('track_id', 'artist_id', 'pos')
    markets = []  # tuples of shape ('track_id', 'market')
    metadata = (
        []
    )  # list of dictionaries for all remaining track metadata (excluding artists and markets)

    chunk_size = 50
    track_ids_chunks = split_into_chunks_of_size(track_ids, chunk_size)
    print(f"Fetching data in {len(track_ids_chunks)} chunks of size {chunk_size}...")

    with tqdm(total=len(track_ids_chunks)) as pbar:
        for track_ids in track_ids_chunks:
            api_resp = spotify.tracks(track_ids)["tracks"]
            for track_data in api_resp:
                track_id = track_data["id"]
                artists.extend(_process_artists(track_id, track_data["artists"]))
                markets.extend(
                    _process_markets(track_id, track_data["available_markets"])
                )
                metadata.append(_process_remaining_data(track_data))
                pbar.update(1)

    df_dict = {}

    df_dict["metadata"] = pd.DataFrame(metadata)
    df_dict["metadata"].set_index("track_id", inplace=True)

    df_dict["artists"] = pd.DataFrame(artists, columns=["track_id", "artist_id", "pos"])
    df_dict["artists"].set_index("track_id", inplace=True)

    df_dict["markets"] = pd.DataFrame(markets, columns=["track_id", "market"])
    df_dict["markets"].set_index("track_id", inplace=True)

    return df_dict


def _process_artists(track_id: str, artists: list):
    """
    Processes the artist data for a single track returned by the Spotify API.

    Args:
        track_id: The ID of the track.
        artists: A list of dictionaries containing the artist data for the track.

    Returns:
        A list of tuples of shape ('track_id', 'artist_id', 'pos').
    """
    track_artists = []
    for i, artist in enumerate(artists):
        track_artists.append((track_id, artist["id"], i + 1))
    return track_artists


def _process_markets(track_id: str, markets: list):
    """
    Processes the market data for a single track returned by the Spotify API.

    Args:
        track_id: The ID of the track.
        markets: A list of dictionaries containing the market data for the track.

    Returns:
        A list of tuples of shape ('track_id', 'market').
    """
    track_markets = []
    for market in markets:
        track_markets.append((track_id, market))
    return track_markets


def _process_remaining_data(data: dict):
    """
    Processes the remaining track data returned by the Spotify API, returning it as a dictionary.
    """
    for source, url in data["external_urls"].items():
        if source != "spotify":
            data[f"{source}_url"] = url

    for platform, p_id in data["external_ids"].items():
        data[f"{platform}_url"] = p_id

    data["album_id"] = data["album"]["id"]

    # rename id to track_id
    data["track_id"] = data["id"]
    del data["id"]

    attrs_to_drop = [
        "type",  # always 'track'
        "uri",  # can be derived from 'id'
        "href",  # can be derived from 'id'
        "artists",  # already processed
        "external_urls",  # already processed
        "external_ids",  # already processed
        "available_markets",  # already processed
        "album",  # already processed
        "is_local",  # always False
        "popularity",  # constantly changing, not useful for static analysis
    ]

    for attr in attrs_to_drop:
        del data[attr]

    return data


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
        "--output_dir",
        type=str,
        help="Path to folder where output files with track metadata will be stored.",
        required=True,
    )

    args = parser.parse_args()

    input_path = args.input_path
    output_dir = args.output_dir

    main(input_path=input_path, output_dir=output_dir)
