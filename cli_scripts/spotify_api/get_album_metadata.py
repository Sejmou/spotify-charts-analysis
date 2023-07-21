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
from typing import List


def main(input_path: str, output_dir: str):
    """
    Fetches metadata for albums on Spotify using spotipy (Python wrapper for Spotify API).
    Receives a path to a parquet file with album IDs as as input and outputs parquet files with metadata for all unique album IDs.

    The following output files are generated in the specified output directory:
    - metadata.parquet: Contains the metadata for each album.
    - images.parquet: Contains the album images for each album.
    - artists.parquet: Contains the artist IDs for each album (together with the 'position' of the artist, i.e. primary artist, secondary artist etc.).
    - markets.parquet: Contains the available markets for each album.
    - copyrights.parquet: Contains the copyright information for each album.

    Currently this runs on a single thread. It could be sped up by using multiple threads.
    However, this is still fast enough for our purposes (and MUCH faster than the web scraping approach used for downloading the Spotify Chart data).
    """
    input_df = pd.read_parquet(input_path)
    album_ids = input_df["album_id"].unique().tolist()

    print(f"Found {len(album_ids)} unique album IDs in {input_path}.")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    spotify = create_spotipy_client()

    df_dict = get_album_metadata_from_api(album_ids=album_ids, spotify=spotify)
    write_dfs_in_dict_to_parquet_files(df_dict=df_dict, output_dir=output_dir)


def get_album_metadata_from_api(album_ids: list, spotify: spotipy.Spotify):
    chunk_size = 20
    album_ids_chunks = split_into_chunks_of_size(album_ids, chunk_size)
    print(f"Fetching data in {len(album_ids_chunks)} chunks of size {chunk_size}...")

    imgs = []  # tuples of shape ('album_id', 'url', 'width', 'height')
    artists = []  # tuples of shape ('album_id', 'artist_id', 'pos')
    markets = []  # tuples of shape ('album_id', 'market')
    copyrights = []  # tuples of shape ('album_id', 'text', 'type')
    metadata = []  # list of dictionaries for all remaining album metadata

    with tqdm(total=len(album_ids_chunks)) as pbar:
        for album_ids in album_ids_chunks:
            api_resp = spotify.albums(album_ids)["albums"]
            for album_data in api_resp:
                if album_data is None:
                    raise ValueError(
                        'Received "None" as response from spotipy. You probably provided one or more invalid album IDs.'
                    )
                album_id = album_data["id"]
                imgs.extend(
                    _process_img_data(album_id=album_id, images=album_data["images"])
                )
                artists.extend(
                    _process_artists(album_id=album_id, artists=album_data["artists"])
                )
                markets.extend(
                    _process_markets(
                        album_id=album_id, markets=album_data["available_markets"]
                    )
                )
                copyrights.extend(
                    _process_copyrights(
                        album_id=album_id, copyrights=album_data["copyrights"]
                    )
                )
                metadata.append(_process_remaining_data(data=album_data))
            pbar.update(1)

    df_dict = {}

    df_dict["metadata"] = pd.DataFrame(metadata)
    df_dict["metadata"].set_index("album_id", inplace=True)

    df_dict["images"] = pd.DataFrame(
        imgs, columns=["album_id", "url", "width", "height"]
    )
    df_dict["images"].set_index("album_id", inplace=True)

    df_dict["artists"] = pd.DataFrame(artists, columns=["album_id", "artist_id", "pos"])
    df_dict["artists"].set_index("album_id", inplace=True)

    df_dict["markets"] = pd.DataFrame(markets, columns=["album_id", "market"])
    df_dict["markets"].set_index("album_id", inplace=True)
    df_dict["copyrights"] = pd.DataFrame(
        copyrights,
        columns=["album_id", "text", "type"],
    )
    df_dict["copyrights"].set_index("album_id", inplace=True)

    return df_dict


def _process_img_data(album_id: str, images: List[dict]):
    """
    Processes image data from the Spotify API into a list of tuples.
    """
    img_tuples = []
    for img_data in images:
        img_tuples.append(
            (
                album_id,
                img_data["url"],
                img_data["width"],
                img_data["height"],
            )
        )
    return img_tuples


def _process_artists(album_id: str, artists: List[dict]):
    """
    Processes artist data from the Spotify API into a list of tuples.
    """
    artist_tuples = []
    artist_ids = [artist["id"] for artist in artists]
    for i, artist_id in enumerate(artist_ids):
        artist_tuples.append((album_id, artist_id, i + 1))
    return artist_tuples


def _process_markets(album_id: str, markets: List[str]):
    """
    Processes market data from the Spotify API into a list of tuples.
    """
    market_tuples = []
    for market in markets:
        market_tuples.append((album_id, market))
    return market_tuples


def _process_copyrights(album_id: str, copyrights: List[str]):
    """
    Processes market data from the Spotify API into a list of tuples.
    """
    copyright_tuples = []
    for copyright_val in copyrights:
        copyright_tuples.append(
            (album_id, copyright_val["text"], copyright_val["type"])
        )
    return copyright_tuples


def _process_remaining_data(data: dict):
    """
    Processes remaining album data from the Spotify API, returning it as a dictionary.
    """
    for source, url in data["external_urls"].items():
        if source != "spotify":
            data[f"{source}_url"] = url

    for platform, p_id in data["external_ids"].items():
        data[f"{platform}_url"] = p_id

    data["release_date"] = pd.to_datetime(data["release_date"])

    attrs_to_drop = [
        "type",  # always 'album'
        "uri",  # can be derived from 'id'
        "href",  # can be derived from 'id'
        "artists",  # already processed
        "external_urls",  # already processed
        "external_ids",  # already processed
        "available_markets",  # already processed
        "images",  # already processed
        "genres",  # while this prop exists, it actually always contains an empty list :(
        "copyrights",  # already processed
        "tracks",  # not interested in that atm - also, too much data
        "popularity",  # pretty arbitrary metric (how is it even computed), also constantly changing
    ]

    # rename id to album_id
    data["album_id"] = data["id"]
    del data["id"]

    for attr in attrs_to_drop:
        del data[attr]

    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input_path",
        type=str,
        help="Path to a .parquet file containing album_ids (in a column named 'album_id'))",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        help="Path to folder where output files with album metadata will be stored.",
        required=True,
    )

    args = parser.parse_args()

    input_path = args.input_path
    output_dir = args.output_dir

    main(input_path=input_path, output_dir=output_dir)
