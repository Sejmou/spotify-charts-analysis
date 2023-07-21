from cli_scripts.spotify_api.get_album_metadata import (
    get_album_metadata_from_api,
    _process_img_data,
    _process_artists,
    _process_markets,
    _process_remaining_data,
    _process_copyrights,
)
from helpers.spotify_util import create_spotipy_client
import pandas as pd

# example_track_id = "0S38Oso3I9vpDXcTb7kYt9"
example_album_id = "1gjugH97doz3HktiEjx2vY"
spotify = create_spotipy_client()
example_api_resp = spotify.albums([example_album_id])["albums"][0]


def test_get_album_metadata_from_api():
    dfs = get_album_metadata_from_api(album_ids=[example_album_id], spotify=spotify)
    assert isinstance(dfs, dict)
    assert dfs.keys() == {"metadata", "images", "artists", "markets", "copyrights"}
    for df_name, df in dfs.items():
        assert isinstance(df, pd.DataFrame), f"{df_name} is not a DataFrame"
        assert not df.empty, f"{df_name} is empty"
        assert df.index.name == "album_id", f"{df_name} index is not 'album_id'"


def test_process_img_data():
    imgs = _process_img_data(
        album_id=example_album_id, images=example_api_resp["images"]
    )
    assert len(imgs) >= 1
    for t in imgs:
        validate_img_tuple(t)


def test_process_artists():
    artists = _process_artists(
        album_id=example_album_id, artists=example_api_resp["artists"]
    )
    assert len(artists) >= 1
    for t in artists:
        validate_artist_tuple(t)


def test_process_markets():
    markets = _process_markets(
        album_id=example_album_id, markets=example_api_resp["available_markets"]
    )
    assert len(markets) >= 1
    for t in markets:
        validate_market_tuple(t)


def test_process_copyrigths():
    copyrigths = _process_copyrights(
        album_id=example_album_id, copyrights=example_api_resp["copyrights"]
    )
    assert len(copyrigths) >= 1
    for t in copyrigths:
        validate_copyright_tuple(t)


def test_process_remaining_data():
    data = _process_remaining_data(data=example_api_resp)
    assert isinstance(data, dict)
    assert data["album_type"] in ["album", "single", "compilation"]
    assert isinstance(data["id"], str)
    assert isinstance(data["name"], str)
    assert isinstance(data["release_date"], pd.Timestamp)
    assert isinstance(data["release_date_precision"], str)
    assert isinstance(data["total_tracks"], int)
    assert not any([isinstance(v, (list)) for v in data.values()])
    assert not any([isinstance(v, (dict)) for v in data.values()])


def validate_img_tuple(t):
    assert len(t) == 4
    assert t[0] == example_album_id
    assert isinstance(t[1], str)  # url
    assert isinstance(t[2], int)  # width
    assert isinstance(t[3], int)  # height


def validate_artist_tuple(t):
    assert len(t) == 3
    assert t[0] == example_album_id
    assert isinstance(t[1], str)  # artist_id
    assert isinstance(t[2], int)  # pos


def validate_market_tuple(t):
    assert len(t) == 2
    assert t[0] == example_album_id
    assert isinstance(t[1], str)  # market


def validate_copyright_tuple(t):
    assert len(t) == 3
    assert t[0] == example_album_id
    assert isinstance(t[1], str)  # text
    assert isinstance(t[2], str)  # type
