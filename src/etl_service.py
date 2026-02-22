import logging
from datetime import datetime, date, timedelta
from typing import Callable
from zoneinfo import ZoneInfo

import pandas as pd

from api_client import fetch_today_scoreboard, fetch_scoreboard_for_date, fetch_team_game_logs
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
from config import CURRENT_SEASON, SEASON_TYPE, SEASON_START

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


def run_etl_for_date(
    date_str: str,
    progress_callback: Callable[[str, float], None] | None = None,
) -> dict:
    """Fetch games and player stats for a specific historical date (YYYY-MM-DD).

    Used when the user selects a past date that has no data in the DB.
    """
    def _progress(msg: str, frac: float = 0.0) -> None:
        logger.info(msg)
        if progress_callback:
            progress_callback(msg, frac)

    summary = {
        "status": "SUCCESS",
        "games_found": 0,
        "teams_processed": 0,
        "rows_upserted": 0,
        "error_message": None,
    }

    try:
        _progress(f"Fetching games for {date_str}...", 0.05)
        games = fetch_scoreboard_for_date(date_str)
        summary["games_found"] = len(games)

        if not games:
            _progress("No games found for this date.", 1.0)
            return summary

        # Teams already in boxscores — skip re-fetching their season logs
        with get_connection() as conn:
            fetched_teams: set[int] = {
                r[0] for r in conn.execute(
                    "SELECT DISTINCT team_id FROM boxscores"
                ).fetchall()
            }

        # Save teams and games
        _progress(f"Saving {len(games)} games...", 0.15)
        new_team_ids: list[int] = []

        with get_connection() as conn:
            for g in games:
                for side in ("home", "away"):
                    tid = g[f"{side}_team_id"]
                    city = g[f"{side}_team_city"]
                    name = g[f"{side}_team_name"]
                    abbr = g[f"{side}_abbreviation"]
                    upsert_team(conn, tid, f"{city} {name}", abbr)
                    if tid not in fetched_teams:
                        new_team_ids.append(tid)

                upsert_daily_game(
                    conn,
                    game_id=g["game_id"],
                    game_date=date_str,
                    home_team_id=g["home_team_id"],
                    away_team_id=g["away_team_id"],
                )

        # Fetch full season logs for new teams only
        unique_new = list(set(new_team_ids))
        total_new = len(unique_new)

        for i, tid in enumerate(unique_new):
            frac = 0.20 + (i / max(total_new, 1)) * 0.75
            _progress(f"Fetching season logs for team {i + 1}/{total_new}...", frac)
            try:
                df = fetch_team_game_logs(tid)
                rows, _ = _store_game_logs(df, tid)
                summary["rows_upserted"] += rows
                summary["teams_processed"] += 1
                fetched_teams.add(tid)
            except Exception as exc:
                logger.error("Failed to fetch logs for team %d: %s", tid, exc)
                summary["status"] = "PARTIAL"

        _progress("Done.", 1.0)

    except Exception as exc:
        logger.exception("run_etl_for_date failed for %s: %s", date_str, exc)
        summary["status"] = "FAILED"
        summary["error_message"] = str(exc)

    return summary


def run_backfill(progress_callback: Callable[[str, float], None] | None = None) -> dict:
    """Find all missing dates since season start and fetch their game data.

    For each missing date:
      1. Fetch historical scoreboard (ScoreboardV2)
      2. Save teams + daily_games to DB
      3. Fetch PlayerGameLogs only for teams not yet in boxscores (full season)

    Returns a summary dict.
    """
    def _progress(msg: str, frac: float = 0.0) -> None:
        logger.info(msg)
        if progress_callback:
            progress_callback(msg, frac)

    et_today = datetime.now(ZoneInfo("America/New_York")).date()
    season_start = date.fromisoformat(SEASON_START)

    # Build full list of dates from season start to yesterday
    all_dates = []
    d = season_start
    while d < et_today:
        all_dates.append(d.isoformat())
        d += timedelta(days=1)

    # Find which dates are missing from daily_games
    with get_connection() as conn:
        existing_dates = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT game_date FROM daily_games"
            ).fetchall()
        }

    missing_dates = [d for d in all_dates if d not in existing_dates]

    if not missing_dates:
        _progress("No missing dates found. All data is up to date.", 1.0)
        return {
            "status": "SUCCESS",
            "missing_found": 0,
            "dates_processed": 0,
            "rows_upserted": 0,
            "error_message": None,
        }

    _progress(f"Found {len(missing_dates)} missing dates. Fetching...", 0.0)

    # Teams already in boxscores — no need to re-fetch their full season logs
    with get_connection() as conn:
        fetched_teams: set[int] = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT team_id FROM boxscores"
            ).fetchall()
        }

    summary = {
        "status": "SUCCESS",
        "missing_found": len(missing_dates),
        "dates_processed": 0,
        "rows_upserted": 0,
        "error_message": None,
    }

    total = len(missing_dates)

    for i, date_str in enumerate(missing_dates):
        frac = i / total
        _progress(f"Processing {date_str}... ({i + 1}/{total})", frac)

        try:
            games = fetch_scoreboard_for_date(date_str)
            if not games:
                summary["dates_processed"] += 1
                continue

            new_team_ids: list[int] = []

            with get_connection() as conn:
                for g in games:
                    for side in ("home", "away"):
                        tid = g[f"{side}_team_id"]
                        city = g[f"{side}_team_city"]
                        name = g[f"{side}_team_name"]
                        abbr = g[f"{side}_abbreviation"]
                        upsert_team(conn, tid, f"{city} {name}", abbr)
                        if tid not in fetched_teams:
                            new_team_ids.append(tid)

                    upsert_daily_game(
                        conn,
                        game_id=g["game_id"],
                        game_date=date_str,
                        home_team_id=g["home_team_id"],
                        away_team_id=g["away_team_id"],
                    )

            # Fetch full season logs only for teams we haven't seen before
            for tid in set(new_team_ids):
                try:
                    _progress(f"  → Fetching season logs for team {tid}...", frac)
                    df = fetch_team_game_logs(tid)
                    rows, _ = _store_game_logs(df, tid)
                    summary["rows_upserted"] += rows
                    fetched_teams.add(tid)
                except Exception as exc:
                    logger.error("Backfill: failed to fetch logs for team %d: %s", tid, exc)
                    summary["status"] = "PARTIAL"

            summary["dates_processed"] += 1

        except Exception as exc:
            logger.error("Backfill: failed to process date %s: %s", date_str, exc)
            summary["status"] = "PARTIAL"

    _progress(
        f"Backfill complete. {summary['dates_processed']}/{total} dates, "
        f"{summary['rows_upserted']} rows upserted.",
        1.0,
    )
    return summary
