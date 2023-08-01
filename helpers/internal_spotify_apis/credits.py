import pandas as pd
from typing import Dict
import numpy as np


def process_credits(credits_resp_content: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Process credits data fetched from internal Spotify API.
    """
    # only roleCredits column is actually usable/relevant
    roles = _create_credits_df(credits_resp_content.roleCredits)

    # split into separate DFs for writers, producers, and performers

    writers = roles[roles.roleTitle == "Writer"].drop(columns=["roleTitle"])

    producers = roles[roles.roleTitle == "Producer"].drop(columns=["roleTitle"])
    # data exploration showed that all producers have exactly one subrole, which is always "producer"
    # verify this assumption
    assert (
        producers.subroles.apply(len).sum() == producers.shape[0]
    ), "At least one producer has multiple subroles"
    assert (
        producers.subroles.apply(lambda arr: arr[0]).values == "producer"
    ).all(), "At least one producer has subroles != ['producer']"
    producers = producers.drop(columns=["subroles"])
    # verify that all externalUrl and creatorUri values are indeed NaN (as in data exploration)
    assert pd.isna(producers.externalUrl).all()
    assert pd.isna(producers.creatorUri).all()
    producers = producers.drop(columns=["externalUrl", "creatorUri"]).reset_index(
        drop=True
    )

    performers = (
        roles[roles.roleTitle == "Performer"]
        .drop(columns=["roleTitle"])
        .reset_index(drop=True)
    )
    # verify that all externalUrl and creatorUri values are indeed NaN (as in data exploration)
    assert pd.isna(performers.externalUrl).all()
    assert pd.isna(performers.creatorUri).all()
    performers = performers.drop(columns=["externalUrl", "creatorUri"]).reset_index(
        drop=True
    )

    return {"writers": writers, "producers": producers, "performers": performers}


def _create_credits_df(role_credits: pd.Series):
    rows = []

    for track_id, track_role_credits in role_credits.items():
        rows.extend(_create_credits_rows(track_id, track_role_credits))

    roles_df = pd.DataFrame(rows)

    # remove duplicates in every subroles array - nonsenical values like ['producer', 'producer'] can occur
    roles_df.subroles = roles_df.subroles.apply(lambda x: list(set(x)))

    # verify that all non-empty uri values are indeed Spotify artist URIs
    assert roles_df.uri.apply(
        lambda uri: uri.startswith("spotify:artist:") if type(uri) == str else True
    ).all(), "At least one non-empty uri value is not a Spotify artist URI"
    roles_df["artist_id"] = roles_df.uri.apply(
        lambda uri: uri.split("spotify:artist:")[1] if type(uri) == str else np.nan
    )

    # make sure artist_id, name, and pos are first three columns
    roles_df = roles_df[
        ["artist_id", "name", "pos"]
        + [col for col in roles_df.columns if col not in ["artist_id", "name", "pos"]]
    ]

    return roles_df


def _create_credits_rows(track_id: str, role_credits: list):
    rows = []
    for role in role_credits:
        title = role["roleTitle"]
        for i, artist in enumerate(role["artists"]):
            rows.append(
                {
                    **artist,
                    "roleTitle": title[:-1],
                    "pos": i + 1,
                    "track_id": track_id,
                }
            )
    return rows
