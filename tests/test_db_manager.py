import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import sqlite3
import pytest

# Point the module at an in-memory DB before importing anything that uses DB_PATH
import config
config.DB_PATH = ":memory:"  # type: ignore[assignment]

import db_manager
db_manager.DB_PATH = ":memory:"  # type: ignore[assignment]


# ── Fixture: fresh in-memory connection per test ──────────────────────────────

@pytest.fixture
def conn() -> sqlite3.Connection:
    """Return an in-memory SQLite connection with the full schema applied."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = ON")
    c.executescript(db_manager.DDL)
    yield c
    c.close()


# ── Helper ────────────────────────────────────────────────────────────────────

def _seed_team(conn: sqlite3.Connection, team_id: int = 1610612747) -> None:
    db_manager.upsert_team(conn, team_id, "Los Angeles Lakers", "LAL")


def _seed_player(conn: sqlite3.Connection, player_id: int = 2544, team_id: int = 1610612747) -> None:
    db_manager.upsert_player(conn, player_id, "LeBron James", team_id)


# ── Team tests ────────────────────────────────────────────────────────────────

def test_upsert_team_insert(conn):
    _seed_team(conn)
    row = conn.execute("SELECT * FROM teams WHERE team_id = 1610612747").fetchone()
    assert row is not None
    assert row["team_name"] == "Los Angeles Lakers"
    assert row["abbreviation"] == "LAL"


def test_upsert_team_update(conn):
    _seed_team(conn)
    db_manager.upsert_team(conn, 1610612747, "LA Lakers", "LAL")
    row = conn.execute("SELECT * FROM teams WHERE team_id = 1610612747").fetchone()
    assert row["team_name"] == "LA Lakers"


def test_upsert_team_no_duplicate(conn):
    _seed_team(conn)
    _seed_team(conn)
    count = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
    assert count == 1


# ── Player tests ──────────────────────────────────────────────────────────────

def test_upsert_player_insert(conn):
    _seed_team(conn)
    _seed_player(conn)
    row = conn.execute("SELECT * FROM players WHERE player_id = 2544").fetchone()
    assert row["player_name"] == "LeBron James"
    assert row["current_team_id"] == 1610612747


def test_upsert_player_update(conn):
    _seed_team(conn)
    _seed_player(conn)
    db_manager.upsert_player(conn, 2544, "LeBron James", None)
    row = conn.execute("SELECT * FROM players WHERE player_id = 2544").fetchone()
    assert row["current_team_id"] is None


# ── Daily game tests ──────────────────────────────────────────────────────────

def test_upsert_daily_game(conn):
    _seed_team(conn, 1610612747)
    _seed_team(conn, 1610612738)
    db_manager.upsert_daily_game(conn, "0022500001", "2025-10-22", 1610612747, 1610612738)
    row = conn.execute("SELECT * FROM daily_games WHERE game_id = '0022500001'").fetchone()
    assert row is not None
    assert row["game_date"] == "2025-10-22"


# ── Boxscore tests ────────────────────────────────────────────────────────────

def _boxscore_record(
    player_id: int = 2544,
    team_id: int = 1610612747,
    game_id: str = "0022500001",
) -> dict:
    return {
        "player_id": player_id,
        "team_id": team_id,
        "game_id": game_id,
        "game_date": "2025-10-22",
        "season": "2025-26",
        "season_type": "Regular Season",
        "min_raw": "32:15",
        "minutes_played": 32,
        "pts": 28,
        "reb": 7,
        "ast": 9,
    }


def test_upsert_boxscore_insert(conn):
    _seed_team(conn)
    _seed_player(conn)
    db_manager.upsert_boxscore(conn, _boxscore_record())
    count = conn.execute("SELECT COUNT(*) FROM boxscores").fetchone()[0]
    assert count == 1


def test_upsert_boxscore_idempotent(conn):
    """Upserting the same record twice should produce exactly one row."""
    _seed_team(conn)
    _seed_player(conn)
    rec = _boxscore_record()
    db_manager.upsert_boxscore(conn, rec)
    db_manager.upsert_boxscore(conn, rec)
    count = conn.execute("SELECT COUNT(*) FROM boxscores").fetchone()[0]
    assert count == 1


def test_upsert_boxscore_updates_stats(conn):
    """A second upsert with different stats should update, not duplicate."""
    _seed_team(conn)
    _seed_player(conn)
    db_manager.upsert_boxscore(conn, _boxscore_record())
    updated = {**_boxscore_record(), "pts": 35, "reb": 10}
    db_manager.upsert_boxscore(conn, updated)
    row = conn.execute("SELECT pts, reb FROM boxscores").fetchone()
    assert row["pts"] == 35
    assert row["reb"] == 10


# ── ETL run tests ─────────────────────────────────────────────────────────────

def test_start_and_finish_etl_run(conn):
    run_id = db_manager.start_etl_run(conn, "2025-10-22T10:00:00")
    assert run_id is not None and run_id > 0

    db_manager.finish_etl_run(
        conn,
        run_id=run_id,
        finished_at="2025-10-22T10:01:00",
        status="SUCCESS",
        games_found=5,
        teams_processed=10,
        players_processed=150,
        rows_upserted=3000,
    )
    row = conn.execute("SELECT * FROM etl_runs WHERE run_id = ?", (run_id,)).fetchone()
    assert row["status"] == "SUCCESS"
    assert row["games_found"] == 5
    assert row["rows_upserted"] == 3000
    assert row["error_message"] is None
