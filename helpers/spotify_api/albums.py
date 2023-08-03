import pandas as pd
from tqdm import tqdm
import spotipy
from typing import List
from helpers.util import split_into_chunks_of_size
from helpers.spotify_util import create_spotipy_data_provenance_info_dict


def get_album_metadata_from_api(album_ids: list, spotify: spotipy.Spotify):
    chunk_size = 20
    album_ids_chunks = split_into_chunks_of_size(album_ids, chunk_size)
    print(f"Fetching data in {len(album_ids_chunks)} chunks of size {chunk_size}...")

    imgs = []  # tuples of shape ('album_id', 'url', 'width', 'height')
    artists = []  # tuples of shape ('album_id', 'artist_id', 'pos')
    markets = []  # tuples of shape ('album_id', 'market')
    copyrights = []  # tuples of shape ('album_id', 'text', 'type')
    metadata = []  # list of dictionaries for all remaining album metadata
    original_responses = (
        []
    )  # list of original API responses, with added 'timestamp' and 'source' fields

    with tqdm(total=len(album_ids_chunks)) as pbar:
        for album_ids in album_ids_chunks:
            api_resp = spotify.albums(album_ids)["albums"]
            original_responses.append(
                create_spotipy_data_provenance_info_dict(
                    response=api_resp, client_method_name="albums"
                )
            )
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

    df_dict["original_responses"] = pd.DataFrame(original_responses)

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
        data[f"{platform}_id"] = p_id

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
