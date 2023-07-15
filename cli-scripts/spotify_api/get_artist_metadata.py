import argparse
from tqdm import tqdm
import os
import pandas as pd
from helpers.spotify_util import create_spotipy_client
from helpers.util import (
    split_into_chunks_of_size,
    create_data_source_and_timestamp_file,
)


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

    metadata_dfs = (
        []
    )  # list of dataframes with artist metadata of shape (artist_id, metadata_1, ..., metadata_n)
    artist_genres = []  # tuples of shape ('artist_id', 'genre')
    artist_images = []  # tuples of shape ('artist_id', 'url', 'width', 'height')

    chunk_size = (
        50  # maximum number of artist IDs that can be fetched in a single API call
    )
    artist_ids_chunks = split_into_chunks_of_size(artist_ids, chunk_size)
    print(f"Fetching data in {len(artist_ids_chunks)} chunks of size {chunk_size}...")

    with tqdm(total=len(artist_ids_chunks)) as pbar:
        for artist_ids in artist_ids_chunks:
            api_resp = spotify.artists(artist_ids)["artists"]

            metadata = pd.DataFrame(
                [
                    process_artist_data_from_api(
                        data=artist_data,
                        artist_genres=artist_genres,
                        artist_images=artist_images,
                    )
                    for artist_data in api_resp
                ]
            )
            metadata_dfs.append(metadata)
            pbar.update(1)

    metadata_df = pd.concat(metadata_dfs, ignore_index=True)
    metadata_path = os.path.join(output_dir, "metadata.parquet")
    metadata_df.to_parquet(metadata_path)
    print(f"Saved artist metadata to '{metadata_path}'")

    artist_genres_df = pd.DataFrame(artist_genres, columns=["artist_id", "genre"])
    artist_genres_path = os.path.join(output_dir, "artist_genres.parquet")
    artist_genres_df.to_parquet(artist_genres_path)
    print(f"Saved artist genres to '{artist_genres_path}'")

    artist_images_df = pd.DataFrame(
        artist_images, columns=["artist_id", "url", "width", "height"]
    )
    artist_images_path = os.path.join(output_dir, "artist_images.parquet")
    artist_images_df.to_parquet(artist_images_path)
    print(f"Saved artist images to '{artist_images_path}'")

    create_data_source_and_timestamp_file(
        dir_path=output_dir,
        data_source="Spotify API (using the .artists() method of the Spotify client provided by the spotipy Python library)",
    )


def process_artist_data_from_api(
    data: dict,
    artist_genres: list,
    artist_images: list,
):
    """
    Processes artist data from the Spotify API into a format that can be
    written to a dataframe.

    Args:
        data (dict): Dictionary of artist metadata from the Spotify API
        artist_genres (list): List of tuples of shape (artist_id, genre)
        artist_images (list): List of tuples of shape (artist_id, url, width, height)

    Returns:
        pd.Series: Series of artist metadata in a format that can be written to a dataframe

    This function also modifies the following arguments (lists) in-place (appending items to them):
        - artist_genres
        - artist_images

    """
    # for some reason, the followers prop contains a dict with the key 'total' and a value that is the number of followers and a 'href' key that is always None
    data["followers"] = data["followers"]["total"]

    for source, url in data["external_urls"].items():
        if source != "spotify":
            data["source"] = url

    for genre in data["genres"]:
        artist_genres.append((data["id"], genre))

    for img_data in data["images"]:
        artist_images.append(
            (
                data["id"],
                img_data["url"],
                img_data["width"],
                img_data["height"],
            )
        )

    series = pd.Series(data)

    # make sure the 'id' comes first in the series
    id_value = series.pop("id")
    id_series = pd.Series(id_value, index=["id"])
    series = pd.concat([id_series, series])

    attrs_to_drop = [
        "type",  # always 'artist'
        "uri",  # can be derived from 'id'
        "href",  # can be derived from 'id'
        "external_urls",  # already processed
        "images",  # already processed
        "genres",  # already processed
    ]

    series = series.drop(labels=attrs_to_drop)

    return series


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
