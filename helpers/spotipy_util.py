from spotipy.oauth2 import SpotifyClientCredentials
from spotipy import Spotify
import os
import inquirer
from dotenv import load_dotenv

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


if __name__ == "__main__":
    sp = create_spotipy_client()
    print('Searching for "Kanye" on Spotify')
    print(sp.search("Kanye"))
