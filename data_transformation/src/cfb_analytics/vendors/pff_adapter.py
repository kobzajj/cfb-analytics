from __future__ import annotations
import re
from pathlib import Path
from typing import Iterable, Optional, Dict, List
import pandas as pd

# ------------------------------
# Minimal, configurable adapter
# ------------------------------
# Edit these mappings to match your PFF export headers.
# Left = column name in your PFF CSVs; Right = standardized column name (what we output).
PFF_COLMAP_DEFAULT: Dict[str, str] = {
    "season": "season",
    "team_name": "pff_team_name",            # e.g., "Alabama"
    # "pff_team_code": "team_code",       # optional, e.g., "ALA"
    "player": "player_name",
    # "jersey": "jersey",                 # optional in some feeds
    "position": "position",             # e.g., "QB", "RB", "WR", "TE", "CB", etc.
    # "position_group": "position_group", # optional, e.g., "DL", "LB", "DB", "OL", "WR", etc.
    # "height": "height",                 # optional
    # "weight": "weight",                 # optional
    # "dob": "dob",                       # optional date of birth
    # "class_year": "class",              # optional ("FR","SO","JR","SR","GR")
    "player_id": "pff_player_id",       # if present in your export
}

# Any additional stats you want to carry forward can be listed here and will be prefixed with "pff_"
PFF_PASSTHRU_STATS: List[str] = ['batted_passes',
 'pressures_allowed',
 'total_pressures',
 'rec_yards',
 'contested_receptions',
 'true_pass_set_snap_counts_pass_rush',
 'routes',
 'hits_allowed',
 'grades_run_block',
 'breakaway_yards',
 'yprr',
 'true_pass_set_pass_rush_win_rate',
 'grades_pass_rush_defense',
 'bats',
 'declined_penalties',
 'forced_fumbles',
 'tackles_for_loss',
 'slot_snaps',
 'route_rate',
 'dropped_ints',
 'snap_counts_dl_a_gap',
 'true_pass_set_pass_rush_percent',
 'drops',
 'explosive',
 'fumbles',
 'snap_counts_coverage',
 'grades_tackle',
 'avg_time_to_throw',
 'pass_block_percent',
 'pass_rush_wins',
 'tackles',
 'team_name',
 'hurries',
 'first_downs',
 'snap_counts_pass_rush',
 'player_id',
 'touchdowns',
 'qb_rating',
 'stops',
 'stop_percent',
 'true_pass_set_hurries',
 'grades_offense_penalty',
 'snap_counts_lt',
 'grades_hands_fumble',
 'yards_after_contact',
 'yco_attempt',
 'scrambles',
 'sacks_allowed',
 'aimed_passes',
 'position',
 'designed_yards',
 'grades_run',
 'true_pass_set_hits',
 'interceptions',
 'snap_counts_pass_play',
 'zone_attempts',
 'safeties',
 'hurries_allowed',
 'snap_counts_corner',
 'true_pass_set_pass_rush_wins',
 'targets',
 'sacks',
 'player_game_count',
 'elu_rush_mtf',
 'missed_tackle_rate',
 'hits',
 'franchise_id',
 'snap_counts_run_block',
 'grades_run_defense',
 'snap_counts_lg',
 'snap_counts_dl_b_gap',
 'true_pass_set_grades_pass_rush_defense',
 'non_spike_pass_block_percentage',
 'true_pass_set_total_pressures',
 'pass_rush_opp',
 'snap_counts_defense',
 'coverage_percent',
 'grades_coverage_defense',
 'turnover_worthy_plays',
 'block_percent',
 'elu_yco',
 'gap_attempts',
 'avoided_tackles',
 'snap_counts_block',
 'snap_counts_run',
 'grades_offense',
 'avg_depth_of_target',
 'breakaway_percent',
 'snap_counts_run_defense',
 'prp',
 'catch_rate',
 'fumble_recoveries',
 'run_plays',
 'forced_incompletion_rate',
 'assists',
 'grades_defense_penalty',
 'snap_counts_te',
 'scramble_yards',
 'wide_rate',
 'passing_snaps',
 'yards_after_catch_per_reception',
 'missed_tackles',
 'grades_defense',
 'grades_pass_block',
 'interception_touchdowns',
 'pass_block_rate',
 'big_time_throws',
 'true_pass_set_snap_counts_pass_play',
 'total_touches',
 'true_pass_set_sacks',
 'breakaway_attempts',
 'grades_hands_drop',
 'slot_rate',
 'snap_counts_offense',
 'true_pass_set_pass_rush_opp',
 'true_pass_set_batted_passes',
 'btt_rate',
 'attempts',
 'thrown_aways',
 'def_gen_pressures',
 'accuracy_percent',
 'snap_counts_rg',
 'pass_break_ups',
 'forced_incompletes',
 'run_stop_opp',
 'pass_plays',
 'pass_blocks',
 'coverage_snaps_per_target',
 'penalties',
 'snap_counts_pass_block',
 'snap_counts_ce',
 'contested_catch_rate',
 'elu_recv_mtf',
 'yards_per_reception',
 'drop_rate',
 'dropbacks',
 'fumble_recovery_touchdowns',
 'ypa',
 'non_spike_pass_block',
 'player',
 'pass_rush_percent',
 'pbe',
 'hit_as_threw',
 'avg_depth_of_tackle',
 'snap_counts_dl_over_t',
 'completions',
 'snap_counts_rt',
 'yards_per_coverage_snap',
 'snap_counts_box',
 'snap_counts_dl_outside_t',
 'yards',
 'qb_rating_against',
 'inline_rate',
 'snap_counts_offball',
 'snap_counts_fs',
 'receptions',
 'caught_percent',
 'inline_snaps',
 'completion_percent',
 'snap_counts_slot',
 'contested_targets',
 'twp_rate',
 'snap_counts_dl',
 'true_pass_set_prp',
 'targeted_qb_rating',
 'pass_rush_win_rate',
 'coverage_snaps_per_reception',
 'yards_after_catch',
 'longest',
 'sack_percent',
 'wide_snaps',
 'grades_pass_route',
 'elusive_rating',
 'pressure_to_sack_rate',
 'grades_pass',
 'spikes']

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # strip/widen typical header noise
    df.columns = [c.strip() for c in df.columns]
    return df

def _rename_apply_map(df: pd.DataFrame, colmap: Dict[str, str]) -> pd.DataFrame:
    # Keep only columns we know, but don’t error if missing; we’ll fill later.
    # keep = {std: src for std, src in colmap.items() if src in df.columns}
    keep = df[list(colmap.keys())]
    out = keep.rename(columns=colmap)
    return out

def load_pff_player_seasons(
    path_or_glob: str,
    season: str,
    colmap: Optional[Dict[str, str]] = None,
    passthru_stats: Optional[List[str]] = None,
    include_stats: bool = False,
    stat_types: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Load one or many PFF player-season CSV files and return a standardized DataFrame.
    Standard columns: season, pff_team_name, pff_team_code, player_name, jersey,
                      position, position_group, height, weight, dob, class_year, pff_player_id
    Any columns listed in `passthru_stats` that exist will be renamed with prefix 'pff_' and included.

    Args:
        path_or_glob: directory or glob to CSV files (e.g., 'data/vendor/pff/2021/*.csv')
        season: the season of data to load
        colmap: dictionary translating input column names from PFF files to output column names
        passthru_stats: optional list of columns to pass through from PFF files to crosswalk output
        include_stats: boolean that indicates whether to include pass through stats in output
        stat_types: specifies which (if any) PFF stat type to filter the dataset down to (for type-specific analysis)
    """
    colmap = colmap or PFF_COLMAP_DEFAULT

    print(colmap)

    passthru_stats = passthru_stats or PFF_PASSTHRU_STATS

    files: List[Path] = []
    p = Path(path_or_glob)
    if p.is_dir():
        files = list(p.rglob("*.csv"))
    else:
        files = [Path(x) for x in sorted(Path().glob(path_or_glob))]

    frames: List[pd.DataFrame] = []
    for f in files:

        print(f)
        df = pd.read_csv(f)

        df["season"] = season

        df = _norm_cols(df)
        print(df.shape)
        df_processed = _rename_apply_map(df, colmap)
        print(df.shape)

        # Pass-through stats (optional)
        if include_stats and passthru_stats:
            for c in passthru_stats:
                if c in df.columns:
                    df_processed[f"pff_{c}"] = df[c]

        if "passing" in str(f):
            df_processed["pff_file_source"] = "passing"
        elif "rushing" in str(f):
            df_processed["pff_file_source"] = "rushing"
        elif "receiving" in str(f):
            df_processed["pff_file_source"] = "receiving"
        elif "blocking" in str(f):
            df_processed["pff_file_source"] = "blocking"
        else:
            df_processed["pff_file_source"] = "defense"

        print(df_processed.head(10))
        print(df_processed[df_processed["player_name"] == "Joe Burrow"])
        frames.append(df_processed)

    if not frames:
        return pd.DataFrame(columns=list(colmap.keys()))

    out = pd.concat(frames, ignore_index=True)

    if stat_types:
        out = out[out["pff_file_source"].isin(stat_types)]

    print(out[out["player_name"] == "Joe Burrow"])

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