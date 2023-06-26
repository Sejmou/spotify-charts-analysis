# Fetches metadata for albums on Spotify using spotipy (Python wrapper for Spotify API).
# It receives a path to a parquet files with album IDs as as input and outputs parquet files with metadata for all unique album IDs.

# The following output files are generated in the specified output directory:
# - metadata.parquet: Contains the metadata for each album.
# - images.parquet: Contains the album images for each album.
# - artists.parquet: Contains the artist IDs for each album (together with the 'position' of the artist, i.e. primary artist, secondary artist etc.).
# - markets.parquet: Contains the available markets for each album.
# - genres.parquet: Contains the genres for each album (not sure if Spotify actually provides information here, but the API returns a genre prop for each album).
# - copyrights.parquet: Contains the copyright information for each album.

# Currently this script runs on a single thread. It could be sped up by using multiple threads.
# However, this is still fast enough for our purposes (and MUCH faster than the web scraping approach used for downloading the Spotify Chart data).

import argparse
from tqdm import tqdm
import os
import pandas as pd
from helpers.spotipy_util import create_spotipy_client
from helpers.util import (
    split_into_chunks_of_size,
    create_data_source_and_timestamp_file,
)


def process_album_data_from_api(
    data: dict,
    album_imgs: list,
    album_artists: list,
    album_markets: list,
    album_copyrights: list,
    album_genres: list,
):
    """
    Processes album data from the Spotify API into a format that can be
    written to a dataframe.

    Args:
        data (dict): Dictionary of album metadata from the Spotify API
        album_imgs (list): List of tuples of shape (album_id, url, width, height)
        album_artists (list): List of tuples of shape (album_id, artist_id, pos)
        album_markets (list): List of tuples of shape (album_id, market)
        album_copyrights (list): List of tuples of shape (album_id, text, type)
        album_genres (list): List of tuples of shape (album_id, genre)

    Returns:
        pd.Series: Series of album metadata in a format that can be written to a dataframe

    This function also modifies the following arguments (lists) in-place (appending items to them):
        - album_imgs
        - album_artists
        - album_markets
        - album_copyright
        - album_genres
    """
    for img_data in data["images"]:
        album_imgs.append(
            (
                data["id"],
                img_data["url"],
                img_data["width"],
                img_data["height"],
            )
        )

    for source, url in data["external_urls"].items():
        if source != "spotify":
            data["source"] = url

    for platform, p_id in data["external_ids"].items():
        data[platform] = p_id

    artist_ids = [artist["id"] for artist in data["artists"]]
    for i, artist_id in enumerate(artist_ids):
        album_artists.append((data["id"], artist_id, i + 1))

    for market in data["available_markets"]:
        album_markets.append((data["id"], market))

    for copyright_val in data["copyrights"]:
        album_copyrights.append(
            (data["id"], copyright_val["text"], copyright_val["type"])
        )

    for genre in data["genres"]:
        album_genres.append((data["id"], genre))

    series = pd.Series(data)

    # make sure the 'id' comes first in the series
    id_value = series.pop("id")
    id_series = pd.Series(id_value, index=["id"])
    series = pd.concat([id_series, series])

    series.release_date = pd.to_datetime(series.release_date)

    attrs_to_drop = [
        "type",  # always 'album'
        "uri",  # can be derived from 'id'
        "href",  # can be derived from 'id'
        "artists",  # already processed
        "external_urls",  # already processed
        "external_ids",  # already processed
        "available_markets",  # already processed
        "images",  # already processed
        "genres",  # already processed
        "copyrights",  # already processed
        "tracks",  # not interested in that atm - also, too much data
        "popularity",  # constantly changing, not useful for static analysis
    ]

    series = series.drop(labels=attrs_to_drop)

    return series


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
    input_df = pd.read_parquet(input_path)
    album_ids = input_df["album_id"].unique().tolist()

    print(f"Found {len(album_ids)} unique album IDs in {input_path}.")

    output_dir = args.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    spotify = create_spotipy_client()

    metadata_dfs = (
        []
    )  # list of dataframes with album metadata of shape (album_id, metadata_1, ..., metadata_n)
    album_imgs = []  # tuples of shape ('album_id', 'url', 'width', 'height')
    album_artists = []  # tuples of shape ('album_id', 'artist_id', 'pos')
    album_markets = []  # tuples of shape ('album_id', 'market')
    album_copyrights = []  # tuples of shape ('album_id', 'text', 'type')
    album_genres = []  # tuples of shape ('album_id', 'genre')

    chunk_size = 20
    album_ids_chunks = split_into_chunks_of_size(album_ids, chunk_size)
    print(f"Fetching data in {len(album_ids_chunks)} chunks of size {chunk_size}...")

    with tqdm(total=len(album_ids_chunks)) as pbar:
        for album_ids in album_ids_chunks:
            api_resp = spotify.albums(album_ids)["albums"]

            metadata = pd.DataFrame(
                [
                    process_album_data_from_api(
                        data=album_data,
                        album_imgs=album_imgs,
                        album_artists=album_artists,
                        album_markets=album_markets,
                        album_copyrights=album_copyrights,
                        album_genres=album_genres,
                    )
                    for album_data in api_resp
                ]
            )
            metadata_dfs.append(metadata)
            pbar.update(1)

    metadata_df = pd.concat(metadata_dfs, ignore_index=True)
    metadata_path = os.path.join(output_dir, "metadata.parquet")
    metadata_df.to_parquet(metadata_path)
    print(f"Saved album metadata to '{metadata_path}'")

    album_imgs_df = pd.DataFrame(
        album_imgs, columns=["album_id", "url", "width", "height"]
    )
    album_imgs_path = os.path.join(output_dir, "imgs.parquet")
    album_imgs_df.to_parquet(album_imgs_path)
    print(f"Saved album images to '{album_imgs_path}'")

    album_artists_df = pd.DataFrame(
        album_artists, columns=["album_id", "artist_id", "pos"]
    )
    album_artists_path = os.path.join(output_dir, "artists.parquet")
    album_artists_df.to_parquet(album_artists_path)
    print(f"Saved album artists to '{album_artists_path}'")

    album_markets_df = pd.DataFrame(album_markets, columns=["album_id", "market"])
    album_markets_path = os.path.join(output_dir, "markets.parquet")
    album_markets_df.to_parquet(album_markets_path)
    print(f"Saved album markets to '{album_markets_path}'")

    album_copyrights_df = pd.DataFrame(
        album_copyrights,
        columns=["album_id", "text", "type"],
    )
    album_copyrights_path = os.path.join(output_dir, "copyrights.parquet")
    album_copyrights_df.to_parquet(album_copyrights_path)
    print(f"Saved album copyrights to '{album_copyrights_path}'")

    album_genres_df = pd.DataFrame(album_genres, columns=["album_id", "genre"])
    album_genres_path = os.path.join(output_dir, "genres.parquet")
    album_genres_df.to_parquet(album_genres_path)
    print(f"Saved album genres to '{album_genres_path}'")

    create_data_source_and_timestamp_file(
        dir_path=output_dir,
        data_source="Spotify API (using the .albums() method of the Spotify client provided by the spotipy Python library)",
    )
