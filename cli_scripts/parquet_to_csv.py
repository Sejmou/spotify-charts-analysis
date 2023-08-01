"""
Recursively converts all parquet files in a directory to csv files.

This is needed as neo4j only works with CSV input files (parquet is not supported).
"""

import argparse
import os
import pandas as pd


def main(input_folder_path: str, output_folder_path: str):
    if not os.path.exists(output_folder_path):
        os.makedirs(output_folder_path)

    for name in os.listdir(input_folder_path):
        if name == "charts.parquet":
            # skip full charts.parquet file
            continue
        if name.endswith(".parquet"):
            input_path = os.path.join(input_folder_path, name)
            output_path = os.path.join(
                output_folder_path, name.replace(".parquet", ".csv")
            )
            print(f"Converting '{input_path}' to '{output_path}'")
            df = pd.read_parquet(input_path)
            write_index = df.index.name is not None and "id" in df.index.name
            df.to_csv(output_path, index=write_index)
            print(f"Converted '{input_path}' to '{output_path}'")
        elif os.path.isdir(os.path.join(input_folder_path, name)):
            main(
                input_folder_path=os.path.join(input_folder_path, name),
                output_folder_path=os.path.join(output_folder_path, name),
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert parquet files to csv files")
    parser.add_argument(
        "-i",
        "--input_folder_path",
        type=str,
        help="Path to a directory containing parquet files",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_folder_path",
        type=str,
        help="Path to a directory where output files will be written to",
        required=True,
    )
    args = parser.parse_args()
    main(
        input_folder_path=args.input_folder_path,
        output_folder_path=args.output_folder_path,
    )
