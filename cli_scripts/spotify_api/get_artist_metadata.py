import argparse
import os
import pandas as pd
from helpers.spotify_util import create_spotipy_client
from helpers.data import write_dfs_in_dict_to_parquet_files
from helpers.spotify_api import get_artist_metadata_from_api


def main(input_paths: list, output_dir: str):
    """
    Fetches metadata for artists on Spotify using spotipy (Python wrapper for Spotify API).
    Receives paths to parquet files containing artist IDs (in a 'artist_id' column) as as input and outputs parquet files with metadata for all unique artist IDs.

    The following output files are generated in the specified output directory:
    - metadata.parquet: Contains the metadata for each artist.
    - images.parquet: Contains the available artist image URLs and sizes for each artist.
    - genres.parquet: Contains the genres for each artist.

    Currently this runs on a single thread. It could be sped up by using multiple threads.
    However, this is still fast enough for our purposes (and MUCH faster than the web scraping approach used for downloading the Spotify Chart data).
    """
    artist_ids = set()
    for input_path in input_paths:
        print(f"Reading artist IDs from '{input_path}'...")
        if not os.path.exists(input_path):
            raise Exception(f"Provided input path '{input_path}' does not exist.")
        input_df = pd.read_parquet(input_path)
        artist_ids.update(input_df["artist_id"].unique().tolist())

    artist_ids = list(artist_ids)
    print(f"Found {len(artist_ids)} unique artist IDs in the provided input paths.")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    spotify = create_spotipy_client()

    df_dict = get_artist_metadata_from_api(artist_ids=artist_ids, spotify=spotify)
    write_dfs_in_dict_to_parquet_files(df_dict=df_dict, output_dir=output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input_paths",
        type=str,
        help="Paths to .parquet files containing artist_ids (in a column named 'artist_id'))",
        required=True,
        nargs="+",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        help="Path to folder where output files with artist metadata will be stored.",
        required=True,
    )

    args = parser.parse_args()

    input_paths = args.input_paths
    output_dir = args.output_dir

    main(input_paths=input_paths, output_dir=output_dir)
