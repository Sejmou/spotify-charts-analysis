"""
Peek into the csv files in a directory (recursively) and print the first 5 lines of each file.
"""

import argparse
import os


def main(folder_path: str):
    for name in os.listdir(folder_path):
        if name.endswith(".csv"):
            path = os.path.join(folder_path, name)
            print(f"First five rows of '{path}':")
            with open(path, "r") as f:
                for _ in range(5):
                    print(f.readline().strip())
            print()

        elif os.path.isdir(os.path.join(folder_path, name)):
            main(os.path.join(folder_path, name))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Peek into csv files in a directory")
    parser.add_argument(
        "input_folder_path",
        type=str,
        help="Path to a directory containing csv files",
    )
    args = parser.parse_args()
    main(
        folder_path=args.input_folder_path,
    )
