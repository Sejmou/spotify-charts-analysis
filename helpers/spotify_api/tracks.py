import pandas as pd
from tqdm import tqdm
import spotipy
from typing import List
from helpers.util import split_into_chunks_of_size
from helpers.spotify_util import create_spotipy_data_provenance_info_dict


def get_track_metadata_from_api(track_ids: List[str], spotify: spotipy.Spotify):
    artists = []  # tuples of shape ('track_id', 'artist_id', 'pos')
    markets = []  # tuples of shape ('track_id', 'market')
    metadata = (
        []
    )  # list of dictionaries for all remaining track metadata (excluding artists and markets)
    original_responses = (
        []
    )  # list of original API responses, with added 'timestamp' and 'source' fields

    chunk_size = 50
    track_ids_chunks = split_into_chunks_of_size(track_ids, chunk_size)
    print(f"Fetching data in {len(track_ids_chunks)} chunks of size {chunk_size}...")

    with tqdm(total=len(track_ids_chunks)) as pbar:
        for track_ids in track_ids_chunks:
            api_resp = spotify.tracks(track_ids)["tracks"]
            original_responses.append(
                create_spotipy_data_provenance_info_dict(
                    response=api_resp, client_method_name="tracks"
                )
            )
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

    df_dict["original_responses"] = pd.DataFrame(original_responses)

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
        data[f"{platform}_id"] = p_id

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
