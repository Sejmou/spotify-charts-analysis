from cli_scripts.spotify_api.get_artist_metadata import (
    get_artist_metadata_from_api,
    _process_img_data,
    _process_genres,
    _process_remaining_artist_data,
)
from helpers.spotify_util import create_spotipy_client
import pandas as pd

example_artist_id = "1l2ekx5skC4gJH8djERwh1"
spotify = create_spotipy_client()
example_api_resp = spotify.artists([example_artist_id])["artists"][0]


def test_get_artist_metadata():
    dfs = get_artist_metadata_from_api(
        artist_ids=[example_artist_id, example_artist_id], spotify=spotify
    )
    assert isinstance(dfs, dict)
    assert dfs.keys() == {"metadata", "images", "genres"}
    for df_name, df in dfs.items():
        assert isinstance(df, pd.DataFrame), f"{df_name} is not a DataFrame"
        assert not df.empty, f"{df_name} is empty"
        assert df.index.name == "artist_id", f"{df_name} index is not 'artist_id'"
        if df_name == "metadata":
            assert df.shape[0] == 2


def test_imgs():
    imgs = _process_img_data(
        artist_id=example_artist_id, images=example_api_resp["images"]
    )
    assert len(imgs) >= 1
    for t in imgs:
        validate_img_tuple(t)


def test_genres():
    genres = _process_genres(
        artist_id=example_artist_id, genres=example_api_resp["genres"]
    )
    assert len(genres) >= 1
    for t in genres:
        validate_genre_tuple(t)


def test_remaining_data():
    data = _process_remaining_artist_data(data=example_api_resp)
    assert isinstance(data, dict)
    assert isinstance(data["artist_id"], str)
    assert isinstance(data["name"], str)
    assert isinstance(data["followers"], int)
    assert not any([isinstance(v, (list)) for v in data.values()])
    assert not any([isinstance(v, (dict)) for v in data.values()])


def validate_img_tuple(t):
    assert isinstance(t, tuple)
    assert len(t) == 4
    assert isinstance(t[0], str)
    assert isinstance(t[1], str)
    assert isinstance(t[2], int)
    assert isinstance(t[3], int)


def validate_genre_tuple(t):
    assert isinstance(t, tuple)
    assert len(t) == 2
    assert isinstance(t[0], str)
    assert isinstance(t[1], str)
