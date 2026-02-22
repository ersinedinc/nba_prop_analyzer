import logging
from datetime import datetime
from typing import Callable

import pandas as pd

from api_client import fetch_today_scoreboard, fetch_team_game_logs
from db_manager import (
    get_connection,
    upsert_team,
    upsert_player,
    upsert_daily_game,
    upsert_boxscore,
    start_etl_run,
    finish_etl_run,
)
from utils import parse_minutes
from config import CURRENT_SEASON, SEASON_TYPE

logger = logging.getLogger(__name__)

# Columns expected from NBA API PlayerGameLogs (uppercase)
REQUIRED_API_COLS = {
    "PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "GAME_ID", "GAME_DATE",
    "MIN", "PTS", "REB", "AST",
}


def run_etl(progress_callback: Callable[[str, float], None] | None = None) -> dict:
    """Run the full ETL pipeline.

    Args:
        progress_callback: Optional callable(message, fraction 0.0–1.0)

    Returns:
        Summary dict with keys: status, games_found, teams_processed,
        players_processed, rows_upserted, error_message.
    """
    started_at = datetime.utcnow().isoformat()
    run_id: int | None = None

    summary = {
        "games_found": 0,
        "teams_processed": 0,
        "players_processed": 0,
        "rows_upserted": 0,
        "status": "SUCCESS",
        "error_message": None,
    }

    def _progress(msg: str, frac: float = 0.0) -> None:
        logger.info(msg)
        if progress_callback:
            progress_callback(msg, frac)

    try:
        # Record ETL run start
        with get_connection() as conn:
            run_id = start_etl_run(conn, started_at)

        # Step 1 – Fetch today's scoreboard
        _progress("Fetching today's scoreboard...", 0.05)
        games = fetch_today_scoreboard()
        summary["games_found"] = len(games)

        if not games:
            _progress("No games scheduled today. ETL complete.", 1.0)
            _close_run(run_id, started_at, summary)
            return summary

        # Step 2 – Persist teams and games
        _progress(f"Saving {len(games)} games and team metadata...", 0.10)
        team_ids: list[int] = []

        with get_connection() as conn:
            for g in games:
                for side in ("home", "away"):
                    tid = g[f"{side}_team_id"]
                    city = g[f"{side}_team_city"]
                    name = g[f"{side}_team_name"]
                    abbr = g[f"{side}_abbreviation"]
                    upsert_team(conn, tid, f"{city} {name}", abbr)
                    if tid not in team_ids:
                        team_ids.append(tid)

                upsert_daily_game(
                    conn,
                    game_id=g["game_id"],
                    game_date=g["game_date"],
                    home_team_id=g["home_team_id"],
                    away_team_id=g["away_team_id"],
                )

        # Step 3 – Fetch and store game logs per team
        total_teams = len(team_ids)
        players_seen: set[int] = set()

        for i, team_id in enumerate(team_ids):
            frac = 0.15 + (i / total_teams) * 0.75
            _progress(f"Processing team {i + 1}/{total_teams} (id={team_id})...", frac)
            try:
                df = fetch_team_game_logs(team_id)
                rows_upserted, new_players = _store_game_logs(df, team_id)
                summary["rows_upserted"] += rows_upserted
                summary["teams_processed"] += 1
                players_seen.update(new_players)
            except Exception as exc:
                logger.error("Failed to process team %d: %s", team_id, exc)
                summary["status"] = "PARTIAL"

        summary["players_processed"] = len(players_seen)
        _progress("ETL complete.", 1.0)

    except Exception as exc:
        logger.exception("ETL pipeline failed: %s", exc)
        summary["status"] = "FAILED"
        summary["error_message"] = str(exc)

    _close_run(run_id, started_at, summary)
    return summary


def _store_game_logs(df: pd.DataFrame, team_id: int) -> tuple[int, set[int]]:
    """Transform and upsert one team's game log DataFrame.

    Returns:
        (rows_upserted, set of player_ids processed)
    """
    if df.empty:
        return 0, set()

    df = df.copy()
    df.columns = [c.upper() for c in df.columns]

    missing = REQUIRED_API_COLS - set(df.columns)
    if missing:
        logger.warning("Skipping team %d – missing columns: %s", team_id, missing)
        return 0, set()

    rows_upserted = 0
    players_seen: set[int] = set()

    with get_connection() as conn:
        for _, api_row in df.iterrows():
            min_raw = str(api_row["MIN"]) if pd.notna(api_row.get("MIN")) else None
            minutes_played = parse_minutes(min_raw)

            if minutes_played <= 0:
                continue

            player_id = int(api_row["PLAYER_ID"])
            player_name = str(api_row["PLAYER_NAME"])
            game_id = str(api_row["GAME_ID"])
            # GAME_DATE from NBA API is typically "2025-10-22T00:00:00" or "OCT 22, 2025"
            raw_date = str(api_row["GAME_DATE"])
            game_date = raw_date[:10]  # take YYYY-MM-DD prefix

            upsert_player(conn, player_id, player_name, team_id)

            record = {
                "player_id": player_id,
                "team_id": team_id,
                "game_id": game_id,
                "game_date": game_date,
                "season": CURRENT_SEASON,
                "season_type": SEASON_TYPE,
                "min_raw": min_raw,
                "minutes_played": minutes_played,
                "pts": int(api_row.get("PTS") or 0),
                "reb": int(api_row.get("REB") or 0),
                "ast": int(api_row.get("AST") or 0),
            }
            upsert_boxscore(conn, record)
            rows_upserted += 1
            players_seen.add(player_id)

    return rows_upserted, players_seen


def _close_run(run_id: int | None, started_at: str, summary: dict) -> None:
    """Write the final ETL run record to the database."""
    if run_id is None:
        return
    finished_at = datetime.utcnow().isoformat()
    players = summary.get("players_processed", 0)
    if isinstance(players, set):
        players = len(players)
    try:
        with get_connection() as conn:
            finish_etl_run(
                conn,
                run_id=run_id,
                finished_at=finished_at,
                status=summary["status"],
                games_found=summary["games_found"],
                teams_processed=summary["teams_processed"],
                players_processed=players,
                rows_upserted=summary["rows_upserted"],
                error_message=summary.get("error_message"),
            )
    except Exception as exc:
        logger.error("Failed to close ETL run record: %s", exc)
