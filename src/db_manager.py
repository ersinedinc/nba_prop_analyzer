import sqlite3
import logging
from contextlib import contextmanager
from typing import Generator

from config import DB_PATH, DATA_DIR

logger = logging.getLogger(__name__)

# DDL: tables and indexes only (PRAGMAs are set per-connection in get_connection)
DDL = """
CREATE TABLE IF NOT EXISTS teams (
    team_id      INTEGER PRIMARY KEY,
    team_name    TEXT    NOT NULL,
    abbreviation TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS players (
    player_id       INTEGER PRIMARY KEY,
    player_name     TEXT    NOT NULL,
    current_team_id INTEGER REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS daily_games (
    game_id      TEXT    PRIMARY KEY,
    game_date    DATE    NOT NULL,
    home_team_id INTEGER NOT NULL REFERENCES teams(team_id),
    away_team_id INTEGER NOT NULL REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS boxscores (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id      INTEGER NOT NULL REFERENCES players(player_id),
    team_id        INTEGER NOT NULL REFERENCES teams(team_id),
    game_id        TEXT    NOT NULL,
    game_date      DATE    NOT NULL,
    season         TEXT    NOT NULL,
    season_type    TEXT    NOT NULL,
    min_raw        TEXT,
    minutes_played INTEGER NOT NULL DEFAULT 0,
    pts            INTEGER NOT NULL DEFAULT 0,
    reb            INTEGER NOT NULL DEFAULT 0,
    ast            INTEGER NOT NULL DEFAULT 0,
    UNIQUE(player_id, game_id)
);

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id            INTEGER  PRIMARY KEY AUTOINCREMENT,
    started_at        DATETIME NOT NULL,
    finished_at       DATETIME,
    status            TEXT     NOT NULL DEFAULT 'RUNNING',
    games_found       INTEGER  DEFAULT 0,
    teams_processed   INTEGER  DEFAULT 0,
    players_processed INTEGER  DEFAULT 0,
    rows_upserted     INTEGER  DEFAULT 0,
    error_message     TEXT
);

CREATE INDEX IF NOT EXISTS idx_boxscores_game_id      ON boxscores(game_id);
CREATE INDEX IF NOT EXISTS idx_boxscores_player_id    ON boxscores(player_id);
CREATE INDEX IF NOT EXISTS idx_boxscores_team_id      ON boxscores(team_id);
CREATE INDEX IF NOT EXISTS idx_boxscores_game_date    ON boxscores(game_date);
CREATE INDEX IF NOT EXISTS idx_daily_games_game_date  ON daily_games(game_date);
CREATE INDEX IF NOT EXISTS idx_players_current_team_id ON players(current_team_id);
"""


def init_db() -> None:
    """Create the schema if it does not already exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with get_connection() as conn:
        conn.executescript(DDL)
    logger.info("Database initialized: %s", DB_PATH)


@contextmanager
def get_connection() -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Upsert helpers ────────────────────────────────────────────────────────────

def upsert_team(conn: sqlite3.Connection, team_id: int, team_name: str, abbreviation: str) -> None:
    conn.execute(
        """
        INSERT INTO teams(team_id, team_name, abbreviation)
        VALUES(?, ?, ?)
        ON CONFLICT(team_id) DO UPDATE SET
            team_name    = excluded.team_name,
            abbreviation = excluded.abbreviation
        """,
        (team_id, team_name, abbreviation),
    )


def upsert_player(conn: sqlite3.Connection, player_id: int, player_name: str, current_team_id: int | None) -> None:
    conn.execute(
        """
        INSERT INTO players(player_id, player_name, current_team_id)
        VALUES(?, ?, ?)
        ON CONFLICT(player_id) DO UPDATE SET
            player_name     = excluded.player_name,
            current_team_id = excluded.current_team_id
        """,
        (player_id, player_name, current_team_id),
    )


def upsert_daily_game(
    conn: sqlite3.Connection,
    game_id: str,
    game_date: str,
    home_team_id: int,
    away_team_id: int,
) -> None:
    conn.execute(
        """
        INSERT INTO daily_games(game_id, game_date, home_team_id, away_team_id)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(game_id) DO UPDATE SET
            game_date    = excluded.game_date,
            home_team_id = excluded.home_team_id,
            away_team_id = excluded.away_team_id
        """,
        (game_id, game_date, home_team_id, away_team_id),
    )


def upsert_boxscore(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        """
        INSERT INTO boxscores
            (player_id, team_id, game_id, game_date, season, season_type,
             min_raw, minutes_played, pts, reb, ast)
        VALUES
            (:player_id, :team_id, :game_id, :game_date, :season, :season_type,
             :min_raw, :minutes_played, :pts, :reb, :ast)
        ON CONFLICT(player_id, game_id) DO UPDATE SET
            team_id        = excluded.team_id,
            game_date      = excluded.game_date,
            season         = excluded.season,
            season_type    = excluded.season_type,
            min_raw        = excluded.min_raw,
            minutes_played = excluded.minutes_played,
            pts            = excluded.pts,
            reb            = excluded.reb,
            ast            = excluded.ast
        """,
        row,
    )


# ── ETL run tracking ──────────────────────────────────────────────────────────

def start_etl_run(conn: sqlite3.Connection, started_at: str) -> int:
    cursor = conn.execute(
        "INSERT INTO etl_runs(started_at, status) VALUES(?, 'RUNNING')",
        (started_at,),
    )
    return cursor.lastrowid


def finish_etl_run(
    conn: sqlite3.Connection,
    run_id: int,
    finished_at: str,
    status: str,
    games_found: int,
    teams_processed: int,
    players_processed: int,
    rows_upserted: int,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE etl_runs SET
            finished_at       = ?,
            status            = ?,
            games_found       = ?,
            teams_processed   = ?,
            players_processed = ?,
            rows_upserted     = ?,
            error_message     = ?
        WHERE run_id = ?
        """,
        (
            finished_at, status, games_found, teams_processed,
            players_processed, rows_upserted, error_message, run_id,
        ),
    )


# ── Query helpers ─────────────────────────────────────────────────────────────

def get_today_games(conn: sqlite3.Connection, game_date: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            dg.game_id,
            dg.game_date,
            ht.team_id   AS home_team_id,
            ht.team_name AS home_team,
            ht.abbreviation AS home_abbr,
            at.team_id   AS away_team_id,
            at.team_name AS away_team,
            at.abbreviation AS away_abbr
        FROM daily_games dg
        JOIN teams ht ON ht.team_id = dg.home_team_id
        JOIN teams at ON at.team_id = dg.away_team_id
        WHERE dg.game_date = ?
        ORDER BY dg.game_id
        """,
        (game_date,),
    ).fetchall()


def get_boxscores_for_teams(
    conn: sqlite3.Connection,
    team_ids: list[int],
    min_minutes: int = 10,
) -> list[sqlite3.Row]:
    placeholders = ",".join("?" * len(team_ids))
    return conn.execute(
        f"""
        SELECT
            b.player_id,
            p.player_name,
            b.team_id,
            t.abbreviation AS team_abbr,
            b.game_id,
            b.game_date,
            b.minutes_played,
            b.pts,
            b.reb,
            b.ast
        FROM boxscores b
        JOIN players p ON p.player_id = b.player_id
        JOIN teams   t ON t.team_id   = b.team_id
        WHERE b.team_id IN ({placeholders})
          AND b.minutes_played >= ?
        ORDER BY b.player_id, b.game_date
        """,
        (*team_ids, min_minutes),
    ).fetchall()
