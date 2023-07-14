"""
Combines the functionality of all the other scripts in this directory, applying each of them one after the other.

I.e., it first fetches track metadata, then album metadata for every album in the track metadata,
then artist metadata for every artist in the album metadata AND track metadata (artists can be album artists, but not track artists).

"""
import argparse
import os
from get_album_metadata import main as get_album_metadata
from get_track_metadata import main as get_track_metadata
from get_artist_metadata import main as get_artist_metadata


def main(chart_file_path: str, output_dir: str):
    tracks_subdir = os.path.join(output_dir, "tracks")
    print(f"Getting track metadata for {chart_file_path}")
    get_track_metadata(input_path=chart_file_path, output_dir=tracks_subdir)

    track_metadata_path = os.path.join(tracks_subdir, "metadata.parquet")
    albums_subdir = os.path.join(output_dir, "albums")
    print()
    print(f"Getting album metadata for {track_metadata_path}")
    get_album_metadata(input_path=track_metadata_path, output_dir=albums_subdir)

    album_artists_path = os.path.join(albums_subdir, "artists.parquet")
    track_artists_path = os.path.join(tracks_subdir, "artists.parquet")
    artists_subdir = os.path.join(output_dir, "artists")
    print()
    print(f"Getting artist metadata for {album_artists_path} and {track_artists_path}")
    get_artist_metadata(
        input_paths=[album_artists_path, track_artists_path], output_dir=artists_subdir
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get Spotify API data for a list of tracks. Example usage: python cli-scripts/spotify_api/get_all.py -i data/top200_01-2017_06-2023/charts.parquet -o data/top200_01-2017_06-2023"
    )
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
        help="Path to a directory where output files with the Spotify API will be written to (in a subdirectory).",
        required=True,
    )
    args = parser.parse_args()
    main(chart_file_path=args.input_path, output_dir=args.output_dir)
