from spotipy.oauth2 import SpotifyClientCredentials
from spotipy import Spotify
import os
import inquirer
from dotenv import load_dotenv
import spotipy
from datetime import datetime
import subprocess

client_id, client_secret = None, None


def create_spotipy_client():
    """
    Creates a spotipy client with the credentials provided in the .env file.
    If the credentials are not provided in the .env file, the user will be prompted to enter them manually.
    """

    global client_id, client_secret
    questions = []
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(env_path)
    if not client_id:
        client_id = os.environ.get("SPOTIPY_CLIENT_ID")
        if not client_id:
            questions.append(
                inquirer.Text("client_id", message="Enter your Spotify Client ID:")
            )
    if not client_secret:
        client_secret = os.environ.get("SPOTIPY_CLIENT_SECRET")
        if not client_secret:
            questions.append(
                inquirer.Text(
                    "client_secret", message="Enter your Spotify Client Secret:"
                )
            )

    if questions:
        answers = inquirer.prompt(questions)
        client_id = answers.get("client_id", client_id)
        client_secret = answers.get("client_secret", client_secret)

    return Spotify(
        client_credentials_manager=SpotifyClientCredentials(client_id, client_secret)
    )


def create_spotipy_data_provenance_info_dict(response: dict, client_method_name: str):
    return {
        "source": f"Spotify API (spotipy v{SPOTIFY_VERSION}, client method: '{client_method_name}')",
        "content": response,
        "timestamp": datetime.utcnow(),
    }


def get_spotipy_version():
    # https://stackoverflow.com/a/7353141/13727176
    p1 = subprocess.Popen(["pip", "show", "spotipy"], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(["grep", "Version"], stdin=p1.stdout, stdout=subprocess.PIPE)
    p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
    output, err = p2.communicate()
    version_str = output.decode("utf-8").split("Version: ")[-1].strip()
    return version_str


SPOTIFY_VERSION = get_spotipy_version()


def get_spotify_track_link(id: str):
    return f"https://open.spotify.com/track/{id}"


def get_spotify_album_link(id: str):
    return f"https://open.spotify.com/album/{id}"


def get_spotify_artist_link(id: str):
    return f"https://open.spotify.com/artist/{id}"


def get_id_from_spotify_uri(uri: str):
    return uri.split(":")[-1]


def get_track_uri_from_id(track_id: str):
    return f"spotify:track:{track_id}"


if __name__ == "__main__":
    sp = create_spotipy_client()
    print('Searching for "Kanye" on Spotify')
    print(sp.search("Kanye"))
