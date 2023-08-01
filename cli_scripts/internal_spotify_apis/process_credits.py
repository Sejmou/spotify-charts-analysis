"""
Processes credits data contained in (successful) API responses from internal Spotify APIs.
"""

import argparse
import os
import pandas as pd
from helpers.internal_spotify_apis import process_credits


def main(input_jsonl_path: str, output_folder_path: str):
    api_resp_df = pd.read_json(input_jsonl_path, lines=True).set_index("track_id")

    dfs = process_credits(
        pd.DataFrame.from_records(api_resp_df.content, index=api_resp_df.index)
    )

    if not os.path.exists(output_folder_path):
        os.makedirs(output_folder_path)
    for name, df in dfs.items():
        output_path = os.path.join(output_folder_path, f"{name}.parquet")
        df.to_parquet(output_path)
        print(f"Stored {name} in '{output_path}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process credits data fetched from internal Spotify APIs."
    )
    parser.add_argument(
        "-i",
        "--input_jsonl_path",
        type=str,
        help="Path to a .jsonl file containing the (successful) API responses",
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
        input_jsonl_path=args.input_jsonl_path,
        output_folder_path=args.output_folder_path,
    )
