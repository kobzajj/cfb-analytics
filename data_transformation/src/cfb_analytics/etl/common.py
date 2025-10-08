import pandas as pd
from pathlib import Path

def load_rosters(path: Path) -> pd.DataFrame:
    # season, player_id, player_name, team_id, team_name, conference, position/position_group
    return pd.read_csv(path)

def load_participation(path: Path) -> pd.DataFrame:
    # games, starts, snaps, routes, def_snaps, pass_rush_snaps, run_defense_snaps, etc.
    return pd.read_csv(path)

def load_pbp(path: Path) -> pd.DataFrame:
    # pbp.parquet preferred
    if str(path).endswith('.parquet'):
        return pd.read_parquet(path)
    return pd.read_csv(path)
