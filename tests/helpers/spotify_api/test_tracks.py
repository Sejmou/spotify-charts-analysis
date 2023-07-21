from helpers.spotify_api.tracks import (
    get_track_metadata_from_api,
    _process_artists,
    _process_markets,
    _process_remaining_data,
)
from helpers.spotify_util import create_spotipy_client
import pandas as pd

example_track_id = "0S38Oso3I9vpDXcTb7kYt9"
spotify = create_spotipy_client()
example_api_resp = spotify.tracks([example_track_id])["tracks"][0]


def test_get_track_metadata():
    dfs = get_track_metadata_from_api(
        track_ids=[example_track_id, example_track_id],
        spotify=spotify,
    )
    assert isinstance(dfs, dict)
    assert dfs.keys() == {"metadata", "artists", "markets"}
    for df_name, df in dfs.items():
        assert isinstance(df, pd.DataFrame), f"{df_name} is not a DataFrame"
        assert not df.empty, f"{df_name} is empty"
        assert df.index.name == "track_id", f"{df_name} index is not 'track_id'"
        if df_name == "metadata":
            assert df.shape[0] == 2


def test_process_track_artists():
    artists = _process_artists(
        track_id=example_track_id, artists=example_api_resp["artists"]
    )
    assert len(artists) >= 1
    for t in artists:
        validate_track_artist_tuple(t)


def test_process_track_markets():
    markets = _process_markets(
        track_id=example_track_id, markets=example_api_resp["available_markets"]
    )
    assert len(markets) >= 1
    for t in markets:
        validate_track_market_tuple(t)


def test_process_remaining_data():
    data = _process_remaining_data(data=example_api_resp)
    assert isinstance(data, dict)
    assert isinstance(data["track_id"], str)
    assert isinstance(data["name"], str)
    assert not any([isinstance(v, (list)) for v in data.values()])
    assert not any([isinstance(v, (dict)) for v in data.values()])


def validate_track_artist_tuple(t):
    assert isinstance(t, tuple)
    assert len(t) == 3
    assert isinstance(t[0], str)
    assert isinstance(t[1], str)
    assert isinstance(t[2], int)


def validate_track_market_tuple(t):
    assert isinstance(t, tuple)
    assert len(t) == 2
    assert isinstance(t[0], str)
    assert isinstance(t[1], str)
