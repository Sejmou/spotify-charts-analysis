import pandas as pd
from tqdm import tqdm
import spotipy
from typing import List
from helpers.util import split_into_chunks_of_size
from helpers.spotify_util import create_spotipy_data_provenance_info_dict


def get_artist_metadata_from_api(artist_ids: list, spotify: spotipy.Spotify):
    artist_genres = []  # tuples of shape ('artist_id', 'genre')
    artist_images = []  # tuples of shape ('artist_id', 'url', 'width', 'height')
    metadata = []  # list of dictionaries for all remaining artist metadata
    original_responses = (
        []
    )  # list of original API responses, with added 'timestamp' and 'source' fields

    chunk_size = (
        50  # maximum number of artist IDs that can be fetched in a single API call
    )
    artist_ids_chunks = split_into_chunks_of_size(artist_ids, chunk_size)
    print(f"Fetching data in {len(artist_ids_chunks)} chunks of size {chunk_size}...")

    with tqdm(total=len(artist_ids_chunks)) as pbar:
        for artist_ids in artist_ids_chunks:
            api_resp = spotify.artists(artist_ids)["artists"]
            original_responses.append(
                create_spotipy_data_provenance_info_dict(
                    response=api_resp, client_method_name="artists"
                )
            )
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
                metadata.append(_process_remaining_data(artist_data))
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

    df_dict["original_responses"] = pd.DataFrame(original_responses)

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


def _process_remaining_data(
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
