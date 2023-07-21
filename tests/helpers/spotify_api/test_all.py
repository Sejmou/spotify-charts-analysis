from helpers.spotify_api import get_metadata_from_spotify_api
from helpers.spotify_util import create_spotipy_client

example_track_ids = ["0S38Oso3I9vpDXcTb7kYt9", "4cOdK2wGLETKBW3PvgPWqT"]
spotify = create_spotipy_client()


def test_get_metadata_from_spotify_api():
    dfs = get_metadata_from_spotify_api(track_ids=example_track_ids, spotify=spotify)
    assert isinstance(dfs, dict)
    assert dfs.keys() == {"tracks", "albums", "artists"}
    # rest should be tested in test_tracks.py, test_albums.py, test_artists.py already
