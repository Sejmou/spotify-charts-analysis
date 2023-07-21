from typing import List
import spotipy
from .albums import get_album_metadata_from_api
from .artists import get_artist_metadata_from_api
from .tracks import get_track_metadata_from_api


def get_metadata_from_spotify_api(track_ids: List[str], spotify: spotipy.Spotify):
    """
    Gets track, album, and artist metadata for a list of tracks from the Spotify API.

    Args:
        track_ids: A list of track IDs.
        spotify: A spotipy Spotify client.

    Returns:
        A dictionary of dictionaries of DataFrames with the following keys: "tracks", "albums", "artists".
    """
    track_metadata = get_track_metadata_from_api(track_ids=track_ids, spotify=spotify)

    album_ids = track_metadata["metadata"]["album_id"].unique().tolist()
    album_metadata = get_album_metadata_from_api(album_ids=album_ids, spotify=spotify)

    artist_ids = album_metadata["artists"]["artist_id"].unique().tolist()
    artist_ids.extend(track_metadata["artists"]["artist_id"].unique().tolist())
    artist_ids = list(set(artist_ids))
    artist_metadata = get_artist_metadata_from_api(
        artist_ids=artist_ids, spotify=spotify
    )

    return {
        "tracks": track_metadata,
        "albums": album_metadata,
        "artists": artist_metadata,
    }
