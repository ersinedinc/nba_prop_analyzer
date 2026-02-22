import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest
import pandas as pd
from calculations import compute_hit_rates

# ── Fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_ROWS = [
    # Alice – 3 games, strong scorer
    {"player_id": 1, "player_name": "Alice", "team_abbr": "LAL", "game_date": "2025-10-01", "minutes_played": 32, "pts": 22, "reb": 7, "ast": 5},
    {"player_id": 1, "player_name": "Alice", "team_abbr": "LAL", "game_date": "2025-10-03", "minutes_played": 28, "pts": 18, "reb": 9, "ast": 3},
    {"player_id": 1, "player_name": "Alice", "team_abbr": "LAL", "game_date": "2025-10-05", "minutes_played": 35, "pts": 25, "reb": 5, "ast": 8},
    # Bob – 2 games, high assists, low points
    {"player_id": 2, "player_name": "Bob",   "team_abbr": "LAL", "game_date": "2025-10-01", "minutes_played": 22, "pts": 8,  "reb": 4, "ast": 12},
    {"player_id": 2, "player_name": "Bob",   "team_abbr": "LAL", "game_date": "2025-10-03", "minutes_played": 20, "pts": 5,  "reb": 3, "ast": 9},
]


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(SAMPLE_ROWS)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_returns_one_row_per_player(sample_df):
    result = compute_hit_rates(sample_df, min_minutes=10)
    assert len(result) == 2
    assert set(result["Player"]) == {"Alice", "Bob"}


def test_alice_pts_hit_rates(sample_df):
    result = compute_hit_rates(sample_df, min_minutes=10)
    alice = result[result["Player"] == "Alice"].iloc[0]

    assert alice["G"] == 3
    # PTS 10+: all 3 games → 100%
    assert alice["PTS 10+"] == pytest.approx(100.0, abs=0.1)
    # PTS 15+: 22, 18, 25 → all 3 → 100%
    assert alice["PTS 15+"] == pytest.approx(100.0, abs=0.1)
    # PTS 20+: 22, 25 → 2/3
    assert alice["PTS 20+"] == pytest.approx(2 / 3 * 100, abs=0.1)
    # PTS 25+: 25 → 1/3
    assert alice["PTS 25+"] == pytest.approx(1 / 3 * 100, abs=0.1)


def test_alice_reb_hit_rates(sample_df):
    result = compute_hit_rates(sample_df, min_minutes=10)
    alice = result[result["Player"] == "Alice"].iloc[0]
    # REB 5+: 7, 9, 5 → all 3 → 100%
    assert alice["REB 5+"] == pytest.approx(100.0, abs=0.1)
    # REB 8+: 9 → 1/3
    assert alice["REB 8+"] == pytest.approx(1 / 3 * 100, abs=0.1)
    # REB 10+: none → 0%
    assert alice["REB 10+"] == pytest.approx(0.0, abs=0.1)


def test_alice_ast_hit_rates(sample_df):
    result = compute_hit_rates(sample_df, min_minutes=10)
    alice = result[result["Player"] == "Alice"].iloc[0]
    # AST 5+: 5, 8 → 2/3
    assert alice["AST 5+"] == pytest.approx(2 / 3 * 100, abs=0.1)
    # AST 8+: 8 → 1/3
    assert alice["AST 8+"] == pytest.approx(1 / 3 * 100, abs=0.1)
    # AST 10+: none → 0%
    assert alice["AST 10+"] == pytest.approx(0.0, abs=0.1)


def test_min_minutes_filter_excludes_games(sample_df):
    result = compute_hit_rates(sample_df, min_minutes=30)
    alice = result[result["Player"] == "Alice"].iloc[0]
    # Only 32 and 35 min games pass → G = 2
    assert alice["G"] == 2


def test_min_minutes_filter_excludes_player():
    """Player with all games below the threshold should not appear."""
    rows = [
        {"player_id": 3, "player_name": "Charlie", "team_abbr": "BOS", "game_date": "2025-10-01", "minutes_played": 5, "pts": 10, "reb": 3, "ast": 2},
    ]
    df = pd.DataFrame(rows)
    result = compute_hit_rates(df, min_minutes=10)
    assert result.empty


def test_empty_dataframe():
    df = pd.DataFrame(
        columns=["player_id", "player_name", "team_abbr", "game_date", "minutes_played", "pts", "reb", "ast"]
    )
    result = compute_hit_rates(df, min_minutes=10)
    assert result.empty


def test_missing_columns_returns_empty():
    df = pd.DataFrame([{"player_id": 1, "player_name": "X", "pts": 20}])
    result = compute_hit_rates(df, min_minutes=0)
    assert result.empty


def test_avg_min_calculation(sample_df):
    result = compute_hit_rates(sample_df, min_minutes=10)
    alice = result[result["Player"] == "Alice"].iloc[0]
    expected_avg = (32 + 28 + 35) / 3
    assert alice["Avg Min"] == pytest.approx(expected_avg, abs=0.1)


def test_last5_column_present(sample_df):
    result = compute_hit_rates(sample_df, min_minutes=10)
    assert "L5 PTS10+" in result.columns
    assert "L5 REB5+" in result.columns
    assert "L5 AST5+" in result.columns
