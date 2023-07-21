import argparse
from tqdm import tqdm
import os
import pandas as pd
from helpers.spotify_util import create_spotipy_client
from helpers.util import (
    split_into_chunks_of_size,
)
from helpers.data import write_dfs_in_dict_to_parquet_files
from typing import List
import spotipy


def main(input_paths: list, output_dir: str):
    """
    Fetches metadata for artists on Spotify using spotipy (Python wrapper for Spotify API).
    Receives paths to parquet files containing artist IDs (in a 'artist_id' column) as as input and outputs parquet files with metadata for all unique artist IDs.

    The following output files are generated in the specified output directory:
    - metadata.parquet: Contains the metadata for each artist.
    - images.parquet: Contains the available artist image URLs and sizes for each artist.
    - genres.parquet: Contains the genres for each artist.

    Currently this runs on a single thread. It could be sped up by using multiple threads.
    However, this is still fast enough for our purposes (and MUCH faster than the web scraping approach used for downloading the Spotify Chart data).
    """
    artist_ids = set()
    for input_path in input_paths:
        print(f"Reading artist IDs from '{input_path}'...")
        if not os.path.exists(input_path):
            raise Exception(f"Provided input path '{input_path}' does not exist.")
        input_df = pd.read_parquet(input_path)
        artist_ids.update(input_df["artist_id"].unique().tolist())

    artist_ids = list(artist_ids)
    print(f"Found {len(artist_ids)} unique artist IDs in the provided input paths.")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    spotify = create_spotipy_client()

    df_dict = get_artist_metadata_from_api(artist_ids=artist_ids, spotify=spotify)
    write_dfs_in_dict_to_parquet_files(df_dict=df_dict, output_dir=output_dir)


def get_artist_metadata_from_api(artist_ids: list, spotify: spotipy.Spotify):
    artist_genres = []  # tuples of shape ('artist_id', 'genre')
    artist_images = []  # tuples of shape ('artist_id', 'url', 'width', 'height')
    metadata = []  # list of dictionaries for all remaining artist metadata

    chunk_size = (
        50  # maximum number of artist IDs that can be fetched in a single API call
    )
    artist_ids_chunks = split_into_chunks_of_size(artist_ids, chunk_size)
    print(f"Fetching data in {len(artist_ids_chunks)} chunks of size {chunk_size}...")

    with tqdm(total=len(artist_ids_chunks)) as pbar:
        for artist_ids in artist_ids_chunks:
            api_resp = spotify.artists(artist_ids)["artists"]
            for artist_data in api_resp:
                if artist_data is None:
                    raise ValueError(
                        'Received "None" as response from spotipy. You probably provided one or more invalid artist IDs.'
                    )
                artist_id = artist_data["id"]
                artist_genres.extend(_process_genres(artist_id, artist_data["genres"]))
                artist_images.extend(
                    _process_img_data(artist_id, artist_data["images"])
                )
                metadata.append(_process_remaining_artist_data(artist_data))
                pbar.update(1)

    df_dict = {}

    df_dict["metadata"] = pd.DataFrame(metadata)
    df_dict["metadata"].set_index("artist_id", inplace=True)

    df_dict["genres"] = pd.DataFrame(artist_genres, columns=["artist_id", "genre"])
    df_dict["genres"].set_index("artist_id", inplace=True)

    df_dict["images"] = pd.DataFrame(
        artist_images, columns=["artist_id", "url", "width", "height"]
    )
    df_dict["images"].set_index("artist_id", inplace=True)

    return df_dict


def _process_img_data(artist_id: str, images: List[dict]):
    """
    Processes artist image data from the Spotify API into a format that can be
    written to a dataframe.

    Args:
        artist_id (str): ID of the artist
        images (List[dict]): List of image data from the Spotify API

    Returns:
        List[tuple]: List of tuples of shape (artist_id, url, width, height)

    """
    return [
        (
            artist_id,
            img_data["url"],
            img_data["width"],
            img_data["height"],
        )
        for img_data in images
    ]


def _process_genres(artist_id: str, genres: List[str]):
    artist_genres = []
    for genre in genres:
        artist_genres.append((artist_id, genre))
    return artist_genres


def _process_remaining_artist_data(
    data: dict,
):
    # for some reason, the followers prop contains a dict with the key 'total' and a value that is the number of followers and a 'href' key that is always None
    data["followers"] = data["followers"]["total"]

    for source, url in data["external_urls"].items():
        if source != "spotify":
            data[f"{source}_url"] = url

    attrs_to_drop = [
        "type",  # always 'artist'
        "uri",  # can be derived from 'id'
        "href",  # can be derived from 'id'
        "external_urls",  # already processed
        "images",  # already processed
        "genres",  # already processed
    ]

    # rename id to artist_id
    data["artist_id"] = data["id"]
    del data["id"]

    for attr in attrs_to_drop:
        del data[attr]

    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input_paths",
        type=str,
        help="Paths to .parquet files containing artist_ids (in a column named 'artist_id'))",
        required=True,
        nargs="+",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        help="Path to folder where output files with artist metadata will be stored.",
        required=True,
    )

    args = parser.parse_args()

    input_paths = args.input_paths
    output_dir = args.output_dir

    main(input_paths=input_paths, output_dir=output_dir)
