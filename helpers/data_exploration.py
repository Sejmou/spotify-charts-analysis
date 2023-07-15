import json
from contextlib import redirect_stdout
from .data import create_data_path
import pandas as pd
import os


def pretty_print_val(val):
    if isinstance(val, dict) or isinstance(val, list):
        return json.dumps(val, indent=4)
    return val


def write_dict_to_file_as_prettified_json(
    dictionary: dict, file_path=create_data_path("pretty_out.json")
):
    with open(file_path, "w") as f:
        with redirect_stdout(f):
            print(json.dumps(dictionary, indent=4))


def write_series_to_file_as_prettified_json(
    series: pd.Series, file_path=create_data_path("pretty_out.json")
):
    with open(file_path, "w") as f:
        with redirect_stdout(f):
            print(series.to_json(indent=4))


def load_parquet_files_in_dir(dir_path: str):
    """
    Load all .parquet files in a directory into a dictionary of DataFrames.

    Args:
        dir_path (str): Path to directory containing .parquet files.

    Returns:
        dict: Dictionary of DataFrames, where the keys are the file names (without the .parquet extension).

    """

    df_dict = {}
    files = [
        f
        for f in os.listdir(dir_path)
        if os.path.isfile(os.path.join(dir_path, f)) and f.endswith(".parquet")
    ]

    for file in files:
        file_path = os.path.join(dir_path, file)
        df = pd.read_parquet(file_path)
        df_dict[file.split(".")[0]] = df

    return df_dict


def get_spotify_track_link(id: str):
    return f"https://open.spotify.com/track/{id}"


def get_spotify_album_link(id: str):
    return f"https://open.spotify.com/album/{id}"


def get_spotify_artist_link(id: str):
    return f"https://open.spotify.com/artist/{id}"


def get_id_from_spotify_uri(uri: str):
    return uri.split(":")[-1]
