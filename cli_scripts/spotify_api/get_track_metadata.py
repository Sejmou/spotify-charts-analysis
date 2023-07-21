import argparse
import os
import pandas as pd
from helpers.spotify_util import create_spotipy_client
from helpers.data import write_dfs_in_dict_to_parquet_files
from helpers.spotify_api import get_track_metadata_from_api


def main(input_path: str, output_dir: str):
    """
    Fetches track metadata for tracks on Spotify from the Spotify API (/tracks endpoint) using spotipy.

    Three output files are generated in the specified output directory:
    - metadata.parquet: Contains the metadata for each track.
    - artists.parquet: Contains the artist IDs for each track (together with the 'position' of the artist, i.e. primary artist, secondary artist etc.).
    - markets.parquet: Contains the available markets for each track.

    Currently this script runs on a single thread. It could be sped up by using multiple threads.
    However, this is still fast enough for our purposes (and MUCH faster than the web scraping approach used for downloading the Spotify Chart data).
    """
    try:
        input_df = pd.read_parquet(input_path)
    except Exception:
        raise ValueError(f"Input file '{input_path}' must be a .parquet file")

    track_ids = (input_df)["track_id"].unique().tolist()
    print(f"Found {len(track_ids)} unique track IDs in '{input_path}'")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    spotify = create_spotipy_client()

    df_dict = get_track_metadata_from_api(track_ids=track_ids, spotify=spotify)
    write_dfs_in_dict_to_parquet_files(df_dict=df_dict, output_dir=output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input_path",
        type=str,
        help="Path to a .parquet file containing Spotify track IDs (in a column named 'track_id'))",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        help="Path to folder where output files with track metadata will be stored.",
        required=True,
    )

    args = parser.parse_args()

    input_path = args.input_path
    output_dir = args.output_dir

    main(input_path=input_path, output_dir=output_dir)
