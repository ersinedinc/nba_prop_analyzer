import time
import logging
from datetime import datetime as _dt
from typing import Callable, Any

import pandas as pd
from nba_api.live.nba.endpoints.scoreboard import ScoreBoard
from nba_api.stats.endpoints.scoreboardv2 import ScoreboardV2
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


def fetch_today_scoreboard() -> list[dict]:
    """Return a list of game dicts for today from the NBA Live ScoreBoard endpoint."""
    logger.info("Fetching today's scoreboard...")
    board = _retry(ScoreBoard)
    games_raw = board.games.get_dict()

    games = []
    for g in games_raw:
        # gameEt format: "2025-10-22T19:30:00-04:00"
        game_date = g.get("gameEt", "")[:10]
        games.append(
            {
                "game_id": g["gameId"],
                "game_date": game_date,
                "home_team_id": g["homeTeam"]["teamId"],
                "home_team_name": g["homeTeam"]["teamName"],
                "home_team_city": g["homeTeam"]["teamCity"],
                "home_abbreviation": g["homeTeam"]["teamTricode"],
                "away_team_id": g["awayTeam"]["teamId"],
                "away_team_name": g["awayTeam"]["teamName"],
                "away_team_city": g["awayTeam"]["teamCity"],
                "away_abbreviation": g["awayTeam"]["teamTricode"],
            }
        )

    logger.info("Found %d games today.", len(games))
    return games


def fetch_scoreboard_for_date(game_date_str: str) -> list[dict]:
    """Fetch games for a specific historical date (YYYY-MM-DD).

    Uses the Stats API ScoreboardV2 endpoint which supports arbitrary dates.
    """
    date_fmt = _dt.strptime(game_date_str, "%Y-%m-%d").strftime("%m/%d/%Y")
    logger.info("Fetching scoreboard for %s ...", game_date_str)
    board = _retry(ScoreboardV2, game_date=date_fmt, timeout=API_TIMEOUT)

    game_df = board.game_header.get_data_frame()
    line_df = board.line_score.get_data_frame()

    if game_df.empty:
        logger.info("No games found for %s", game_date_str)
        return []

    games = []
    for _, g in game_df.iterrows():
        game_id = str(g["GAME_ID"])
        home_id = int(g["HOME_TEAM_ID"])
        away_id = int(g["VISITOR_TEAM_ID"])

        def _team_info(team_id: int) -> dict:
            row = line_df[line_df["TEAM_ID"] == team_id]
            if row.empty:
                return {"name": "", "city": "", "abbr": ""}
            r = row.iloc[0]
            return {
                "name": str(r.get("TEAM_NICKNAME", "")),
                "city": str(r.get("TEAM_CITY_NAME", "")),
                "abbr": str(r.get("TEAM_ABBREVIATION", "")),
            }

        home = _team_info(home_id)
        away = _team_info(away_id)

        games.append({
            "game_id": game_id,
            "game_date": game_date_str,
            "home_team_id": home_id,
            "home_team_name": home["name"],
            "home_team_city": home["city"],
            "home_abbreviation": home["abbr"],
            "away_team_id": away_id,
            "away_team_name": away["name"],
            "away_team_city": away["city"],
            "away_abbreviation": away["abbr"],
        })

    logger.info("Found %d games for %s", len(games), game_date_str)
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
