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
import zipfile
from zipfile import ZipFile
from io import BytesIO


def main(
    local_path: str,
    endpoint_url: str,
    bucket: str,
    bucket_folder: str,
):
    """
    Copies files from a local directory to an S3 bucket, using credentials stored in the .env file.

    If the provided path refers to a directory, the directory will be zipped and the zip file (named like the input directory) will be uploaded to the S3 bucket.
    If the provided path refers to a file, the file will be uploaded as it is.
    """
    # load credentials from .env file
    load_dotenv()
    wasabi_access_key = os.environ["WASABI_KEY_ID"]
    wasabi_secret_key = os.environ["WASABI_SECRET"]

    # connect to S3 bucket
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=wasabi_access_key,
        aws_secret_access_key=wasabi_secret_key,
    )
    print(f"Connected to S3 endpoint '{endpoint_url}'.")
    print(
        f"Copying files from local directory '{local_path}' to '{bucket_folder}' in S3 bucket '{bucket}'."
    )

    # copy all files from local directory to S3 bucket
    # VERY slow if there are many small files
    # for filename in tqdm(os.listdir(local_folder)):
    #     res = s3.put_object(
    #         Body=f"{local_folder}/{filename}",
    #         Bucket=bucket,
    #         Key=f"{bucket_folder}/{filename}",
    #     )
    #     if res["ResponseMetadata"]["HTTPStatusCode"] != 200:
    #         print(
    #             f"Failed to copy file '{filename}' to S3 bucket '{bucket}' with error code {res['ResponseMetadata']['HTTPStatusCode']}."
    #         )

    files_to_zip = []

    # Collect the list of files to zip
    for root, _, files in os.walk(local_path):
        for file in files:
            file_path = os.path.join(root, file)
            arcname = os.path.relpath(file_path, local_path)
            files_to_zip.append((file_path, arcname))

    # create zip file of local directory (in memory)
    memory_zip = BytesIO()

    # Create a ZipFile object using the in-memory byte stream
    with zipfile.ZipFile(memory_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
        with tqdm(total=len(files_to_zip), desc="Zipping files", unit=" files") as pbar:
            for file_path, arcname in files_to_zip:
                zipf.write(file_path, arcname=arcname)
                pbar.update(1)

    # Now you have the zip data in memory. You can access it using memory_zip.getvalue()
    zip_data = memory_zip.getvalue()
    file_size = len(zip_data)
    print("Files zipped (total size: {:.2f} MB).".format(file_size / 1024**2))

    print("Uploading zip file to S3 bucket...")

    # upload zip file to S3 bucket
    res = s3.put_object(
        Body=zip_data,
        Bucket=bucket,
        Key=f"{bucket_folder}/{local_path.split(os.sep)[-1]}.zip",
    )

    if res["ResponseMetadata"]["HTTPStatusCode"] != 200:
        print(
            f"Failed to copy file '{local_path}' to S3 bucket '{bucket}' with error code {res['ResponseMetadata']['HTTPStatusCode']}."
        )

    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_path",
        "-i",
        type=str,
        help="Path to the local directory containing the csv files that should be copied to the S3 bucket.",
        required=True,
    )
    parser.add_argument(
        "--bucket",
        "-b",
        type=str,
        help="Name of the S3 bucket to which the files should be copied.",
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
        help="Name of the folder in the S3 bucket to which the files should be copied.",
        required=True,
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
