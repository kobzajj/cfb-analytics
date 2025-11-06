
#!/usr/bin/env python
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np

try:
    from cfb_analytics.vendors.pff_adapter import load_pff_player_seasons
except Exception as e:
    raise SystemExit("Missing adapter: src/cfb_analytics/vendors/pff_adapter.py. "
                     "Ensure it's importable (pip install -e .).")

def safe_div(n, d):
    n = n.astype(float)
    d = d.astype(float)
    return np.where((d==0) | (~np.isfinite(d)), np.nan, n / d)

def ensure_master_dir(season: int) -> Path:
    outdir = Path(f"data/master/{season}")
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir

def load_cfbd_table(season: int, filename: str) -> pd.DataFrame:
    p = Path(f"data/processed/{season}/{filename}")
    if not p.exists():
        raise SystemExit(f"CFBD table not found: {p}")
    return pd.read_csv(p)

def load_xref_players() -> pd.DataFrame:
    p = Path("data/xref/players_pff_cfbd.csv")
    if not p.exists():
        raise SystemExit("Player crosswalk not found: data/xref/players_pff_cfbd.csv")
    return pd.read_csv(p)[["season","cfbd_player_id","pff_player_id","pff_team_name"]]

PFF_RUSH_COLS = {
    'player_game_count': 'player_game_count',
    'attempts': 'attempts',
    'avoided_tackles': 'avoided_tackles',
    'breakaway_attempts': 'breakaway_attempts',
    'breakaway_percent': 'breakaway_percent',
    'breakaway_yards': 'breakaway_yards',
    'declined_penalties': 'declined_penalties',
    'designed_yards': 'designed_yards',
    'drops': 'drops',
    'elu_recv_mtf': 'elu_recv_mtf',
    'elu_rush_mtf': 'elu_rush_mtf',
    'elu_yco': 'elu_yco',
    'elusive_rating': 'elusive_rating',
    'explosive': 'explosive',
    'first_downs': 'first_downs',
    'franchise_id': 'franchise_id',
    'fumbles': 'fumbles',
    'gap_attempts': 'gap_attempts',
    'grades_hands_fumble': 'grades_hands_fumble',
    'grades_offense': 'grades_offense',
    'grades_offense_penalty': 'grades_offense_penalty',
    'grades_pass': 'grades_pass',
    'grades_pass_block': 'grades_pass_block',
    'grades_pass_route': 'grades_pass_route',
    'grades_run': 'grades_run',
    'grades_run_block': 'grades_run_block',
    'longest': 'longest',
    'penalties': 'penalties',
    'rec_yards': 'rec_yards',
    'receptions': 'receptions',
    'routes': 'routes',
    'run_plays': 'run_plays',
    'scramble_yards': 'scramble_yards',
    'scrambles': 'scrambles',
    'targets': 'targets',
    'total_touches': 'total_touches',
    'touchdowns': 'touchdowns',
    'yards': 'yards',
    'yards_after_contact': 'yards_after_contact',
    'yco_attempt': 'yco_attempt',
    'ypa': 'ypa',
    'yprr': 'yprr',
    'zone_attempts': 'zone_attempts'
}

def build_one(season: int):
    cfbd = load_cfbd_table(season, f"players_rushing_{season}.csv")
    pff_raw = load_pff_player_seasons(f"../data_extraction/data/pff/{season}", season=str(season), include_stats=True, stat_types=["rushing"])
    xref = load_xref_players()
    pff = pff_raw.merge(xref, on=["season","pff_player_id","pff_team_name"], how="left")

    keep = ["season","cfbd_player_id"]
    for _, raw in PFF_RUSH_COLS.items():
        col = f"pff_{raw}"
        if col in pff.columns:
            keep.append(col)
    pff_ps = pff[keep].drop_duplicates(["season","cfbd_player_id"])

    cfbd["cfbd_player_id"] = cfbd["player_id"]

    m = cfbd.merge(pff_ps, on=["season","cfbd_player_id"], how="left")

    if "pff_yards_before_contact" in m.columns:
        m["yards_before_contact"] = m["pff_yards_before_contact"]
    if "pff_yards_after_contact" in m.columns:
        m["yards_after_contact"] = m["pff_yards_after_contact"]
    if "pff_broken_tackles" in m.columns:
        m["broken_tackles"] = m["pff_broken_tackles"]
    if "pff_forced_missed_tackles" in m.columns:
        m["forced_missed_tackles"] = m["pff_forced_missed_tackles"]
    if "pff_rpo_carries" in m.columns and "rush_att" in m.columns:
        m["rpo_carry_rate"] = safe_div(m["pff_rpo_carries"].fillna(0), m["rush_att"].fillna(0))
    if "pff_read_option_carries" in m.columns and "rush_att" in m.columns:
        m["read_option_rate"] = safe_div(m["pff_read_option_carries"].fillna(0), m["rush_att"].fillna(0))
    if "pff_expected_yards" in m.columns and "rush_att" in m.columns:
        m["expected_yards_per_att"] = safe_div(m["pff_expected_yards"].fillna(0), m["rush_att"].fillna(0))
        if "yards_per_carry" in m.columns:
            m["yards_over_expected_per_att"] = m["yards_per_carry"] - m["expected_yards_per_att"]

    outdir = ensure_master_dir(season)
    outp = outdir / "rushing_master.csv"
    m.to_csv(outp, index=False)
    print(f"[ok] {season} → {outp}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", type=int, nargs="+", required=True)
    args = ap.parse_args()
    for yr in args.seasons:
        build_one(int(yr))

if __name__ == "__main__":
    main()
