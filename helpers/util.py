from datetime import datetime
import os


def split_into_chunks_of_size(list_to_split: list, chunk_size: int):
    return [
        list_to_split[i : i + chunk_size]
        for i in range(0, len(list_to_split), chunk_size)
    ]


def create_data_source_and_timestamp_file(dir_path: str, data_source: str):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    with open(os.path.join(dir_path, "info.txt"), "w") as f:
        f.write(f"Data was obtained from {data_source} on {timestamp}")
