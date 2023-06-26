from datetime import datetime
import os


def split_into_chunks_of_size(list_to_split: list, chunk_size: int):
    return [
        list_to_split[i : i + chunk_size]
        for i in range(0, len(list_to_split), chunk_size)
    ]


def create_data_source_and_timestamp_file(dir_path: str, data_source: str):
    # Get the current UTC date and time
    current_utc_time = datetime.utcnow()

    # Format the UTC time as a string
    utc_time_string = current_utc_time.strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(os.path.join(dir_path, "info.txt"), "w") as f:
        f.write(
            f"Data was obtained from {data_source}\nDate and time: {utc_time_string}"
        )
