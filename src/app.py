import sys
from pathlib import Path

# Ensure src/ is on the path when running via `streamlit run src/app.py`
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import streamlit as st
from datetime import date, datetime
from zoneinfo import ZoneInfo

def _et_today() -> date:
    """Return the current date in NBA Eastern Time (America/New_York)."""
    return datetime.now(ZoneInfo("America/New_York")).date()

from utils import setup_logging
from db_manager import init_db, get_connection, get_today_games, get_boxscores_for_teams
from etl_service import run_etl
from calculations import compute_hit_rates, style_dataframe
from config import MIN_MINUTES_PLAYED

# ── Initialisation ────────────────────────────────────────────────────────────
setup_logging()
init_db()

st.set_page_config(
    page_title="NBA Player Analysis",
    page_icon="🏀",
    layout="wide",
)

# ── Session-state defaults ────────────────────────────────────────────────────
if "min_minutes" not in st.session_state:
    st.session_state["min_minutes"] = MIN_MINUTES_PLAYED
if "min_games" not in st.session_state:
    st.session_state["min_games"] = 25

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("🏀 NBA Prop Analyzer")

st.session_state["min_minutes"] = st.sidebar.slider(
    "Min minutes played per game",
    min_value=0,
    max_value=36,
    value=st.session_state["min_minutes"],
    step=1,
    help="Games where a player logged fewer than this many minutes are excluded from analysis.",
)

st.session_state["min_games"] = st.sidebar.slider(
    "Minimum matches played",
    min_value=1,
    max_value=40,
    value=st.session_state["min_games"],
    step=1,
    help="Players with fewer qualifying games than this threshold are hidden from results.",
)

st.sidebar.divider()

if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    status_text = st.sidebar.empty()
    progress_bar = st.sidebar.progress(0.0)

    def _on_progress(msg: str, frac: float) -> None:
        status_text.text(msg)
        progress_bar.progress(min(frac, 1.0))

    with st.spinner("Running ETL pipeline…"):
        summary = run_etl(progress_callback=_on_progress)

    progress_bar.progress(1.0)

    if summary["status"] == "SUCCESS":
        st.sidebar.success(
            f"Done!  \n"
            f"Games: {summary['games_found']}  \n"
            f"Teams: {summary['teams_processed']}  \n"
            f"Players: {summary['players_processed']}  \n"
            f"Rows upserted: {summary['rows_upserted']}"
        )
    elif summary["status"] == "PARTIAL":
        st.sidebar.warning(
            "Partial success – some teams may have failed. Check `logs/app.log` for details."
        )
    else:
        st.sidebar.error(f"ETL failed: {summary.get('error_message')}")

# ── Main page ─────────────────────────────────────────────────────────────────
et_now = datetime.now(ZoneInfo("America/New_York"))
st.title("Daily NBA Player Analysis")
st.caption(f"NBA Eastern Time: {et_now.strftime('%A, %B %d, %Y  %H:%M ET')}")

# ── Date picker ───────────────────────────────────────────────────────────────
# Fetch all dates that have game data in the DB
with get_connection() as conn:
    available_dates = [
        row[0] for row in conn.execute(
            "SELECT DISTINCT game_date FROM daily_games ORDER BY game_date DESC"
        ).fetchall()
    ]

col1, col2 = st.columns([2, 3])

with col1:
    default_date = (
        date.fromisoformat(available_dates[0]) if available_dates else _et_today()
    )
    selected_date = st.date_input(
        "Select date",
        value=default_date,
        min_value=date(2025, 10, 1),
        max_value=_et_today(),
    )

with col2:
    if available_dates:
        st.caption(
            f"Dates with data in DB: {', '.join(available_dates[:5])}"
            + (" …" if len(available_dates) > 5 else "")
        )

selected_date_str = selected_date.isoformat()

with get_connection() as conn:
    games = get_today_games(conn, selected_date_str)

if not games:
    if selected_date == _et_today():
        st.info(
            "No games found for today in the database.  \n"
            "Click **Refresh Data** in the sidebar to fetch today's schedule."
        )
    else:
        st.info(
            f"No games found for **{selected_date_str}** in the database.  \n"
            "Try a different date or click **Refresh Data** to load today's games."
        )
    st.stop()

# ── Game selector ─────────────────────────────────────────────────────────────
game_options: dict[str, dict] = {
    f"{dict(g)['away_team']} @ {dict(g)['home_team']}": dict(g)
    for g in games
}

selected_label = st.selectbox(
    "Select a game",
    options=list(game_options.keys()),
)
selected_game = game_options[selected_label]

team_ids = [selected_game["home_team_id"], selected_game["away_team_id"]]
min_minutes = st.session_state["min_minutes"]

# ── Fetch player stats for both teams ────────────────────────────────────────
with get_connection() as conn:
    rows = get_boxscores_for_teams(conn, team_ids, min_minutes=min_minutes)

if not rows:
    st.warning(
        "No player statistics found for this game.  \n"
        "Click **Refresh Data** to load season stats, or lower the minutes filter."
    )
    st.stop()

df_raw = pd.DataFrame([dict(r) for r in rows])

# ── Compute hit rates ─────────────────────────────────────────────────────────
df_rates = compute_hit_rates(df_raw, min_minutes=min_minutes)

# Apply minimum games filter
min_games = st.session_state["min_games"]
df_rates = df_rates[df_rates["G"] >= min_games]

if df_rates.empty:
    st.warning(
        "No players meet the current filters.  \n"
        "Try reducing **Min minutes played** or **Minimum matches played**."
    )
    st.stop()

# ── Display heatmap table ─────────────────────────────────────────────────────
st.subheader(f"Hit Rate Analysis — {selected_label}")
st.caption(
    f"{selected_date_str}  |  "
    f"{len(df_rates)} players shown  |  "
    f"Min {min_minutes} min/game  |  "
    f"Min {min_games} matches played  |  "
    f"Green = higher hit rate, Red = lower"
)

styled = style_dataframe(df_rates)
st.dataframe(styled, width="stretch", height=620)

# ── Raw data expander ─────────────────────────────────────────────────────────
with st.expander("Raw game log data"):
    st.dataframe(df_raw, width="stretch")
