# combines all Spotify Charts CSV files in a directory into a single CSV or Parquet file
# expected to be used after running download_charts.py to download Spotify Charts CSV files into a given directory
# usage: python combine_charts.py -i <input_dir> -o <output_file> -s <start_date> -e <end_date>

# %%
import os
import pandas as pd
import multiprocessing
import argparse
from tqdm import tqdm
import re


def process_spotify_daily_charts_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        # name of each daily charts file downloaded from https://charts.spotify.com is in format 'regional-<region_code>-daily-<YYYY-MM-DD>.csv'
        # region_code is two letter country code (e.g. 'us' for United States) except for global charts
        # global charts use region code 'global' -> replace with two letter code ('ww' for worldwide; not 'gl' as that is the official ISO code for Greenland)
        filename_components = os.path.basename(file_path).split(".csv")[0].split("-")

        region_code = filename_components[1]
        if region_code == "global":
            region_code = "ww"
        df.insert(0, "region_code", region_code)

        date_str = "-".join(filename_components[3:6])
        df.insert(0, "date", pd.to_datetime(date_str))
        df.date = df.date.dt.floor(
            "d"
        )  # set time component to 0 (midnight); should result in performant date queries https://stackoverflow.com/a/41718815/13727176

        def extract_track_id(uri):
            return uri.split(":")[-1]

        # replace uri column with track_id column
        df.insert(2, "track_id", df.uri.apply(extract_track_id))
        df = df.drop(columns=["uri"])

        df = df.rename(
            columns={"rank": "pos"}
        )  # rank has special meaning in pandas DF API, rename for convenience

        return df
    except Exception as e:
        print(f"Error reading file: {file_path}")
        print(e)
        return None


def combine_csv_files(
    directory,
    start_date_filter: pd.Timestamp = None,
    end_date_filter: pd.Timestamp = None,
):
    filenames = [file for file in os.listdir(directory) if file.endswith(".csv")]

    # remove duplicates
    duplicate_pattern = r"\(\d+\)\.csv$"
    non_duplicates = [f for f in filenames if not re.search(duplicate_pattern, f)]
    if len(non_duplicates) != len(filenames):
        print(
            f"Warning: {len(filenames) - len(non_duplicates)} duplicate files found among the {len(filenames)} files in {directory}. They will be ignored."
        )

    files = [os.path.join(directory, file) for file in non_duplicates]

    num_files = len(files)
    print(f"Processing {num_files} files")

    # Determine the number of processes to use (you can adjust this as needed)
    num_processes = min(multiprocessing.cpu_count(), num_files)

    # Create a pool of worker processes
    pool = multiprocessing.Pool(processes=num_processes)

    # Process the files concurrently
    results = []
    with tqdm(total=num_files) as pbar:
        for result in pool.imap_unordered(process_spotify_daily_charts_csv, files):
            results.append(result)
            pbar.update()

    pool.close()
    pool.join()

    print(f"Done processing files, combining results...")

    # Combine the dataframes
    combined_df = pd.concat(results, ignore_index=True)

    combined_df = combined_df.sort_values(
        by=["date", "region_code", "pos"]
    )  # sort by date, region_code, and pos
    # convert region codes to uppercase to match ISO 3166-1 alpha-2 country codes more closely and make joins with Spotify API data easier
    combined_df.region_code = combined_df.region_code.str.upper()

    if start_date_filter is not None:
        combined_df = combined_df[combined_df.date >= start_date_filter]

    if end_date_filter is not None:
        combined_df = combined_df[combined_df.date <= end_date_filter]

    return combined_df


# %%

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Combine data from multiple Spotify Charts CSV files. The filenames are expected to be in the format 'regional-<region_code>-daily-<YYYY-MM-DD>.csv'"
    )

    parser.add_argument(
        "-i",
        "--input_dir",
        type=str,
        help="the directory containing downloaded Spotify Charts CSV files",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_file",
        type=str,
        help="the filename of the output file (either csv or parquet)",
        required=True,
    )

    parser.add_argument(
        "-s",
        "--start_date",
        type=str,
        help="the start date (inclusive) of the date range to include in the output file (format: YYYY-MM-DD)",
        required=True,
    )
    parser.add_argument(
        "-e",
        "--end_date",
        type=str,
        help="the end date (inclusive) of the date range to include in the output file (format: YYYY-MM-DD)",
        required=True,
    )

    args = parser.parse_args()

    out_path = args.output_file
    file_ext = out_path.split(".")[-1]

    if file_ext not in ["csv", "parquet"]:
        raise ValueError(f"Unsupported file extension: '.{file_ext}'")

    input_dir = args.input_dir
    try:
        start_date_filter = (
            pd.to_datetime(args.start_date) if args.start_date is not None else None
        )
    except ValueError as e:
        print(f"Invalid start date filter provided: {args.start_date}")
        exit(1)

    try:
        end_date_filter = (
            pd.to_datetime(args.end_date) if args.end_date is not None else None
        )
    except ValueError as e:
        print(f"Invalid end date filter provided: {args.end_date}")
        exit(1)

    combined_data = combine_csv_files(input_dir, start_date_filter, end_date_filter)

    print(f"Combined data has {len(combined_data)} rows")
    print(
        f"Combined data contains {len(combined_data.track_id.unique())} unique tracks"
    )
    print(
        f"Combined data contains {len(combined_data.region_code.unique())} unique regions"
    )
    print(f"Combined data contains {len(combined_data.date.unique())} unique dates")
    print(f"First date is {combined_data.date.min()}")
    print(f"Last date is {combined_data.date.max()}")

    print(f'Saving combined data to "{out_path}"')

    output_dir = os.path.dirname(out_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if file_ext == "parquet":
        combined_data.to_parquet(out_path, index=False)
    else:
        combined_data.to_csv(out_path)
