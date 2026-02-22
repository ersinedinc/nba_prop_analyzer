import time
import logging
from typing import Callable, Any

import pandas as pd
from nba_api.stats.endpoints.scheduleleaguev2 import ScheduleLeagueV2
from nba_api.stats.endpoints.playergamelogs import PlayerGameLogs

from config import CURRENT_SEASON, SEASON_TYPE, API_RETRY_ATTEMPTS, API_RETRY_DELAY, API_TIMEOUT

logger = logging.getLogger(__name__)


def _retry(func: Callable, *args: Any, **kwargs: Any) -> Any:
    """Call func with args/kwargs, retrying up to API_RETRY_ATTEMPTS times."""
    last_exc: Exception | None = None
    for attempt in range(1, API_RETRY_ATTEMPTS + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, API_RETRY_ATTEMPTS, func.__name__, exc)
            if attempt < API_RETRY_ATTEMPTS:
                time.sleep(API_RETRY_DELAY)
    raise RuntimeError(f"All {API_RETRY_ATTEMPTS} attempts failed for {func.__name__}") from last_exc


def fetch_schedule_for_date(game_date_str: str) -> list[dict]:
    """Return regular-season games for the given date (YYYY-MM-DD).

    Uses ScheduleLeagueV2 which covers the full season schedule,
    including past, today, and future dates.
    Preseason and playoff games are excluded by filtering on gameId prefix '002'.
    """
    logger.info("Fetching schedule for %s ...", game_date_str)
    endpoint = _retry(ScheduleLeagueV2, season=CURRENT_SEASON, timeout=API_TIMEOUT)
    df = endpoint.get_data_frames()[0]

    # Regular season gameIds start with '002'; preseason with '001'
    df = df[df["gameId"].str.startswith("002")].copy()

    # gameDate column is "MM/DD/YYYY" or "MM/DD/YYYY HH:MM:SS" — normalize to YYYY-MM-DD
    df["_date"] = pd.to_datetime(df["gameDate"], format="mixed").dt.strftime("%Y-%m-%d")
    df = df[df["_date"] == game_date_str]

    games = []
    for _, row in df.iterrows():
        games.append({
            "game_id": str(row["gameId"]),
            "game_date": game_date_str,
            "home_team_id": int(row["homeTeam_teamId"]),
            "home_team_name": str(row["homeTeam_teamName"]),
            "home_team_city": str(row["homeTeam_teamCity"]),
            "home_abbreviation": str(row["homeTeam_teamTricode"]),
            "away_team_id": int(row["awayTeam_teamId"]),
            "away_team_name": str(row["awayTeam_teamName"]),
            "away_team_city": str(row["awayTeam_teamCity"]),
            "away_abbreviation": str(row["awayTeam_teamTricode"]),
        })

    logger.info("Found %d regular-season games for %s", len(games), game_date_str)
    return games


def fetch_team_game_logs(
    team_id: int,
    season: str = CURRENT_SEASON,
    season_type: str = SEASON_TYPE,
) -> pd.DataFrame:
    """Fetch all player game logs for a team in the given season.

    Returns a DataFrame with raw NBA API columns (uppercase).
    """
    logger.info("Fetching game logs for team_id=%d season=%s ...", team_id, season)
    logs = _retry(
        PlayerGameLogs,
        season_nullable=season,
        season_type_nullable=season_type,
        team_id_nullable=team_id,
        timeout=API_TIMEOUT,
    )
    df = logs.get_data_frames()[0]
    logger.info("  -> %d rows returned for team_id=%d", len(df), team_id)
    return df
