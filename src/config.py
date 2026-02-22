from pathlib import Path

# Project root (one level above src/)
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"
LOGS_DIR = ROOT_DIR / "logs"

# Database
DB_PATH = DATA_DIR / "nba_veri.db"

# Season
CURRENT_SEASON = "2025-26"
SEASON_TYPE = "Regular Season"

# API
API_TIMEOUT = 30
API_RETRY_ATTEMPTS = 3
API_RETRY_DELAY = 5  # seconds between retries

# Analysis thresholds
THRESHOLDS: dict[str, list[int]] = {
    "PTS": [10, 15, 20, 25],
    "REB": [3, 5, 7, 10],
    "AST": [3, 5, 7, 10],
}

# Minimum minutes played to count a game as valid
MIN_MINUTES_PLAYED = 25

# Logging
LOG_FILE = LOGS_DIR / "app.log"
LOG_LEVEL = "INFO"
