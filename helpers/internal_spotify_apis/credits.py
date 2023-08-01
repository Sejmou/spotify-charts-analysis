import pandas as pd
from typing import Dict
import numpy as np


def process_credits(credits_resp_content: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Process credits data fetched from internal Spotify API.
    """
    # only roleCredits column is actually usable/relevant
    roles = _create_role_credits_df(credits_resp_content.roleCredits)

    # split into separate DFs for writers, producers, and performers

    writers = roles[roles.roleTitle == "Writer"].drop(columns=["roleTitle"])

    # producer subroles are always "producer" - not sure if that should be accounted for (e.g. by deleting column)
    producers = roles[roles.roleTitle == "Producer"].drop(columns=["roleTitle"])

    performers = roles[roles.roleTitle == "Performer"].drop(columns=["roleTitle"])

    return {"writers": writers, "producers": producers, "performers": performers}


def _create_role_credits_df(role_credits: pd.Series):
    role_rows = []

    def _create_rol_credits_rows(track_id: str, role_credits: list):
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

    for track_id, track_role_credits in role_credits.items():
        role_rows.extend(_create_rol_credits_rows(track_id, track_role_credits))

    roles_df = pd.DataFrame(role_rows)
    return roles_df
