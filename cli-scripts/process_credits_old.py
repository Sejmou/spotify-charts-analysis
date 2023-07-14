"""
Parses the parquet file with scraped credits data (containing arrays of objects for each track's performers, songwriters and producers
 and arrays of strings for the credits sources) and creates a separate parquet file for each array column
 (with the objects expanded into several columns and an additional 'pos' column added)
"""

import pandas as pd
import os
import argparse


def create_df_for_dict_arrays_in_column(
    df: pd.DataFrame, col_name: str, add_array_item_pos_col: bool = True
):
    col_dicts = df[col_name].explode()
    df_expanded = pd.json_normalize(col_dicts).set_index(col_dicts.index)
    if add_array_item_pos_col:
        # Add 'pos' column with incremental values for each
        df_expanded["pos"] = df_expanded.groupby(df_expanded.index).cumcount() + 1

    return df_expanded


def create_df_for_simple_arrays_in_column(
    df: pd.DataFrame,
    col_name: str,
    vals_col_name: str,
    add_array_item_pos_col: bool = True,
):
    col_values = df[col_name].explode()
    df_expanded = pd.DataFrame(col_values.tolist(), index=col_values.index)
    df_expanded.columns = [vals_col_name]
    if add_array_item_pos_col:
        # Add 'pos' column with incremental values for each
        df_expanded["pos"] = df_expanded.groupby(df_expanded.index).cumcount() + 1

    return df_expanded


def main(credits_data_path: str, output_data_path: str):
    credits_df = pd.read_parquet(credits_data_path)
    credits_df = credits_df.set_index("track_id")

    credits_dfs = {}
    for col in ["performers", "songwriters", "producers"]:
        credits_dfs[col] = create_df_for_dict_arrays_in_column(credits_df, col)
    credits_dfs["sources"] = create_df_for_simple_arrays_in_column(
        credits_df, "sources", "name"
    )

    if not os.path.exists(output_data_path):
        os.makedirs(output_data_path, exist_ok=True)

    for col, df in credits_dfs.items():
        df.to_parquet(os.path.join(output_data_path, f"{col}.parquet"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input_data_path",
        type=str,
        required=True,
        help="Path to the parquet file with scraped credits data",
    )
    parser.add_argument(
        "-o",
        "--output_data_path",
        type=str,
        required=True,
        help="Path to the directory where the output parquet files should be saved",
    )
    args = parser.parse_args()
    main(args.input_data_path, args.output_data_path)
