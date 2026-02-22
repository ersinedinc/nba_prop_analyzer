import logging
from config import LOG_FILE, LOG_LEVEL, LOGS_DIR


def setup_logging() -> logging.Logger:
    """Configure file + console logging. Safe to call multiple times."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    if root_logger.handlers:
        return logging.getLogger("nba_prop_analyzer")

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger("nba_prop_analyzer")


def parse_minutes(min_raw: str | None) -> int:
    """Parse a minute string (e.g. '32:15' or '32') into integer minutes.

    Returns 0 for DNP / missing / unparseable values.
    """
    if not min_raw:
        return 0
    min_raw = str(min_raw).strip()
    if not min_raw or min_raw.upper() in ("DNP", "NWT", "DID NOT PLAY", "N/A", ""):
        return 0
    try:
        if ":" in min_raw:
            parts = min_raw.split(":")
            return int(parts[0])
        return int(float(min_raw))
    except (ValueError, IndexError):
        return 0
