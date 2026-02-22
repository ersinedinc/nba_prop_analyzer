from dataclasses import dataclass
from typing import Optional
from datetime import date, datetime


@dataclass
class Team:
    team_id: int
    team_name: str
    abbreviation: str


@dataclass
class Player:
    player_id: int
    player_name: str
    current_team_id: Optional[int] = None


@dataclass
class DailyGame:
    game_id: str
    game_date: date
    home_team_id: int
    away_team_id: int


@dataclass
class Boxscore:
    player_id: int
    team_id: int
    game_id: str
    game_date: date
    season: str
    season_type: str
    min_raw: Optional[str]
    minutes_played: int
    pts: int
    reb: int
    ast: int


@dataclass
class ETLRun:
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: str = "RUNNING"
    games_found: int = 0
    teams_processed: int = 0
    players_processed: int = 0
    rows_upserted: int = 0
    error_message: Optional[str] = None
