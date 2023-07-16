import os
import json
from contextlib import redirect_stdout
import re
import pandas as pd


def create_data_path(filename):
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", filename)
    )


def load_parquet_files_in_dir(dir_path: str, index_col=None, new_index_name=None):
    """
    Load all .parquet files in a directory into a dictionary of DataFrames.

    Args:
        dir_path (str): Path to directory containing .parquet files.
        index_col (str, optional): Column to use as index for all DataFrames. Defaults to None.
        new_index_name (str, optional): Rename the index column in all DataFrames to this value. Defaults to None.

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
        if index_col:
            df = df.set_index(index_col)
        if new_index_name:
            df = df.rename_axis(new_index_name)
        df_dict[file.split(".")[0]] = df

    return df_dict


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


def convert_columns_to_snake_case(df: pd.DataFrame):
    df = df.rename(columns=lambda x: re.sub(r"(?<!^)(?=[A-Z])", "_", x).lower())
    return df
