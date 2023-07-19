"""
This script will save the cookies from a selenium session to a file.

The main use case for this is to save cookies from a logged in session to
avoid having to log in again when using selenium in the future (loading the cookies from the cookies file instead).

In the context of this project, this script is used to save cookies from a browsing session where some user is logged in to Spotify.

The generated cookies file can be passed to the `internal_spotify_apis/get.py` script to load the cookies and use them to make requests to internal Spotify APIs.
"""

import argparse
from selenium import webdriver
import pickle
import os


def main(output_dir: str):
    if not os.path.isdir(output_dir):
        if os.path.exists(output_dir):
            raise ValueError(f"Output directory '{output_dir}' is not a directory.")
        else:
            os.makedirs(output_dir, exist_ok=True)

    driver = webdriver.Chrome()

    try:
        print("Please navigate to the site whose cookies you want to save and log in.")
        input("Once you are logged in, press enter to continue...")

        cookies = driver.get_cookies()
        url = driver.current_url
        # get url without protocol, subpaths, and query string
        trimmed_url = url.split("//")[1].split("/")[0].split("?")[0]

        output_path = os.path.join(output_dir, f"cookies_{trimmed_url}.pkl")
        with open(output_path, "wb") as file:
            pickle.dump(cookies, file)

        driver.quit()

        print(f"Cookies saved to '{output_path}'")
    except KeyboardInterrupt:
        print("Keyboard interrupt detected. Quitting...")
        driver.quit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Selenium Cookie Saver")
    parser.add_argument(
        "output_dir", type=str, help="Directory to save the cookie file in."
    )
    args = parser.parse_args()

    main(args.output_dir)
