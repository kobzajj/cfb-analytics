from __future__ import annotations
import re
from pathlib import Path
from typing import Iterable, Optional, Dict, List
import pandas as pd

# ------------------------------
# Minimal, configurable adapter
# ------------------------------
# Edit these mappings to match your PFF export headers.
# Left = standardized column name (what we output); Right = column name in your PFF CSVs.
PFF_COLMAP_DEFAULT: Dict[str, str] = {
    "season": "season",
    "pff_team_name": "team",            # e.g., "Alabama"
    "pff_team_code": "team_code",       # optional, e.g., "ALA"
    "player_name": "player",
    "jersey": "jersey",                 # optional in some feeds
    "position": "position",             # e.g., "QB", "RB", "WR", "TE", "CB", etc.
    "position_group": "position_group", # optional, e.g., "DL", "LB", "DB", "OL", "WR", etc.
    "height": "height",                 # optional
    "weight": "weight",                 # optional
    "dob": "dob",                       # optional date of birth
    "class_year": "class",              # optional ("FR","SO","JR","SR","GR")
    "pff_player_id": "player_id",       # if present in your export
}

# Any additional stats you want to carry forward can be listed here and will be prefixed with "pff_"
PFF_PASSTHRU_STATS: List[str] = [
    # examples—adjust to your files
    "dropbacks", "pass_attempts", "completions", "passing_yards", "passing_tds",
    "interceptions", "sacks", "pressures", "targets", "receptions", "receiving_yards",
    "receiving_tds", "rush_attempts", "rushing_yards", "rushing_tds",
]

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # strip/widen typical header noise
    df.columns = [c.strip() for c in df.columns]
    return df

def _rename_apply_map(df: pd.DataFrame, colmap: Dict[str, str]) -> pd.DataFrame:
    # Keep only columns we know, but don’t error if missing; we’ll fill later.
    keep = {std: src for std, src in colmap.items() if src in df.columns}
    out = df.rename(columns=keep)
    # Ensure all standardized columns exist
    for std in colmap.keys():
        if std not in out.columns:
            out[std] = pd.NA
    return out

def load_pff_player_seasons(
    path_or_glob: str,
    colmap: Optional[Dict[str, str]] = None,
    passthru_stats: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Load one or many PFF player-season CSV files and return a standardized DataFrame.
    Standard columns: season, pff_team_name, pff_team_code, player_name, jersey,
                      position, position_group, height, weight, dob, class_year, pff_player_id
    Any columns listed in `passthru_stats` that exist will be renamed with prefix 'pff_' and included.

    Args:
        path_or_glob: directory or glob to CSV files (e.g., 'data/vendor/pff/2021/*.csv')
    """
    colmap = colmap or PFF_COLMAP_DEFAULT
    passthru_stats = passthru_stats or PFF_PASSTHRU_STATS

    files: List[Path] = []
    p = Path(path_or_glob)
    if p.is_dir():
        files = list(p.rglob("*.csv"))
    else:
        files = [Path(x) for x in sorted(Path().glob(path_or_glob))]

    frames: List[pd.DataFrame] = []
    for f in files:
        df = pd.read_csv(f)
        df = _norm_cols(df)
        df = _rename_apply_map(df, colmap)

        # Pass-through stats (optional)
        for c in passthru_stats:
            if c in df.columns:
                df[f"pff_{c}"] = df[c]

        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=list(colmap.keys()))

    out = pd.concat(frames, ignore_index=True)

    # Light normalization
    out["season"] = pd.to_numeric(out["season"], errors="coerce").astype("Int64")
    if "jersey" in out.columns:
        # jersey as string (leading zeros for duplicates possible)
        out["jersey"] = out["jersey"].astype(str).str.extract(r"(\d+)")[0]

    # normalize a few string fields
    for c in ["pff_team_name", "pff_team_code", "player_name", "position", "position_group", "class_year"]:
        if c in out.columns:
            out[c] = out[c].astype(str).str.strip()

    return out