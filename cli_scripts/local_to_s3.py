"""
Copies all files from a local directory to an S3 bucket, using credentials stored in the .env file.

Make sure to define the following environment variables in the .env file:
    - WASABI_ACCESS_KEY
    - WASABI_SECRET_KEY
"""

import os
import boto3
from dotenv import load_dotenv
import argparse
from tqdm import tqdm
import s3fs
from fsspec.callbacks import TqdmCallback


def main(
    local_path: str,
    endpoint_url: str,
    bucket: str,
    bucket_folder: str,
):
    """
    Copies local data to an S3 bucket, using credentials stored in the .env file.

    Args:
        local_path (str): a local path to a directory or a file. If the path refers to a directory, the directory will be zipped and the zip file will be uploaded to the S3 bucket. If the path refers to a file, the file will be uploaded as it is.
        endpoint_url (str): URL of the S3 bucket endpoint to which data should be copied.
        bucket (str): name of the S3 bucket to which data should be copied.
        bucket_folder (str): folder path in the S3 bucket to which data should be copied. Set to "" to copy to the root directory of the bucket.
    """
    # load credentials from .env file
    load_dotenv()
    wasabi_access_key = os.environ["WASABI_KEY_ID"]
    wasabi_secret_key = os.environ["WASABI_SECRET"]

    # check if local path is a directory or a file
    dest_str = (
        f"'{bucket_folder}' in" if bucket_folder != "" else "root dir of"
    ) + f" S3 bucket '{bucket}'."

    # a bit unfortunate, but we have to use different packages and logic for files and directories
    if os.path.isdir(local_path):
        print(f"Copying files from local directory '{local_path}' to " + dest_str)

        s3_files = s3fs.S3FileSystem(
            endpoint_url=endpoint_url,
            key=wasabi_access_key,
            secret=wasabi_secret_key,
        )

        s3_files.put(
            lpath=local_path,
            rpath=f"{bucket}/{bucket_folder}",
            recursive=True,
            callback=TqdmCallback(),
        )
    elif os.path.isfile(local_path):
        print(f"Copying file '{local_path}' to " + dest_str)
        s3 = boto3.client(
            "s3",
            aws_access_key_id=wasabi_access_key,
            aws_secret_access_key=wasabi_secret_key,
            endpoint_url=endpoint_url,
        )

        file_size = os.path.getsize(local_path)  # in bytes
        filename = local_path.split(os.path.sep)[
            -1
        ]  # actual filename, Filename param is actually a local path lol
        key = (
            bucket_folder + "/" + filename if bucket_folder != "" else filename
        )  # key is the path in the bucket
        with tqdm(total=file_size, unit="B", unit_scale=True, desc=filename) as pbar:
            s3.upload_file(
                Filename=local_path,
                Bucket=bucket,
                Key=key,
                Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
            )
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_path",
        "-i",
        type=str,
        help="Path to local data (file or directory containing files) that should be copied to the S3 bucket.",
        required=True,
    )
    parser.add_argument(
        "--bucket",
        "-b",
        type=str,
        help="Name of the S3 bucket to which the data should be copied.",
        required=True,
    )
    parser.add_argument(
        "--endpoint_url",
        "-e",
        type=str,
        help="URL of the S3 bucket endpoint to which the csv files should be copied.",
        default="https://s3.eu-central-2.wasabisys.com",
    )
    parser.add_argument(
        "--bucket_folder",
        "-f",
        type=str,
        help="Path to the folder in the S3 bucket to which data should be copied.",
        default="",
    )
    args = parser.parse_args()

    if not os.path.exists(args.input_path):
        raise ValueError(f"Path '{args.input_path}' does not exist.")

    main(
        local_path=args.input_path,
        endpoint_url=args.endpoint_url,
        bucket=args.bucket,
        bucket_folder=args.bucket_folder,
    )
