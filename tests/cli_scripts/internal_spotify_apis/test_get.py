import cli_scripts.internal_spotify_apis.get as get
import pandas as pd


def test_get_error_ids_to_skip():
    df = pd.DataFrame(
        [("a", 404), ("b", 404), ("b", 403), ("b", 403), ("a", 500), ("c", 401)],
        columns=["track_id", "status_code"],
    )
    error_ids = get.get_error_ids_to_skip(
        error_log_df=df, status_codes_to_skip=set([404, 403])
    )
    assert error_ids == set("c")
