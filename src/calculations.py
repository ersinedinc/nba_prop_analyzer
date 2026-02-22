import logging
import pandas as pd
from config import THRESHOLDS, MIN_MINUTES_PLAYED

logger = logging.getLogger(__name__)

# Columns required in the input DataFrame
REQUIRED_COLS = {"player_id", "player_name", "team_abbr", "game_date", "minutes_played", "pts", "reb", "ast"}


def compute_hit_rates(df: pd.DataFrame, min_minutes: int = MIN_MINUTES_PLAYED) -> pd.DataFrame:
    """Compute per-player hit rate percentages for each configured threshold.

    Args:
        df: DataFrame with columns: player_id, player_name, team_abbr,
            game_date, minutes_played, pts, reb, ast
        min_minutes: Only count games where minutes_played >= this value.

    Returns:
        One row per player with hit rate columns and supporting metrics.
    """
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        logger.error("Missing columns in input DataFrame: %s", missing)
        return pd.DataFrame()

    df = df[df["minutes_played"] >= min_minutes].copy()

    if df.empty:
        return pd.DataFrame()

    results = []

    for (player_id, player_name, team_abbr), grp in df.groupby(
        ["player_id", "player_name", "team_abbr"], sort=False
    ):
        g = len(grp)
        avg_min = grp["minutes_played"].mean()

        row: dict = {
            "Player": player_name,
            "Team": team_abbr,
            "G": g,
            "Avg Min": round(avg_min, 1),
        }

        # Season-wide hit rates
        for stat, thresholds in THRESHOLDS.items():
            col = stat.lower()
            for t in thresholds:
                hits = int((grp[col] >= t).sum())
                row[f"{stat} {t}+"] = round(hits / g * 100, 1) if g > 0 else 0.0

        # Last-5 hit rate for the primary threshold of each stat
        last5 = grp.sort_values("game_date").tail(5)
        g5 = len(last5)
        for stat, thresholds in THRESHOLDS.items():
            col = stat.lower()
            t0 = thresholds[0]
            hits5 = int((last5[col] >= t0).sum())
            row[f"L5 {stat}{t0}+"] = round(hits5 / g5 * 100, 1) if g5 > 0 else 0.0

        results.append(row)

    return pd.DataFrame(results)


def _pct_to_rdylgn(value: float) -> str:
    """Map a 0–100 value to a Red-Yellow-Green hex color without matplotlib."""
    v = max(0.0, min(100.0, float(value))) / 100.0
    if v <= 0.5:
        # Red (#d73027) → Yellow (#ffffbf)
        t = v / 0.5
        r = int(215 + (255 - 215) * t)
        g = int(48  + (255 - 48)  * t)
        b = int(39  + (191 - 39)  * t)
    else:
        # Yellow (#ffffbf) → Green (#1a9850)
        t = (v - 0.5) / 0.5
        r = int(255 + (26  - 255) * t)
        g = int(255 + (152 - 255) * t)
        b = int(191 + (80  - 191) * t)
    return f"background-color: #{r:02x}{g:02x}{b:02x}; color: {'#000' if v < 0.85 else '#fff'}"


def style_dataframe(df: pd.DataFrame) -> "pd.io.formats.style.Styler":
    """Apply a Red-Yellow-Green gradient to all hit-rate columns (no matplotlib needed)."""
    rate_cols = [
        c for c in df.columns
        if c not in ("Player", "Team", "G", "Avg Min")
    ]
    fmt = {col: "{:.1f}%" for col in rate_cols}
    fmt["Avg Min"] = "{:.1f}"
    fmt["G"] = "{:.0f}"

    return (
        df.style
        .map(_pct_to_rdylgn, subset=rate_cols)
        .format(fmt)
    )
