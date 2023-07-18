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
