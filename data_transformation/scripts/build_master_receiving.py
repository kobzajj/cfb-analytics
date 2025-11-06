
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

PFF_RECV_COLS = {
    'player_game_count': 'player_game_count',
    'avg_depth_of_target': 'avg_depth_of_target',
    'avoided_tackles': 'avoided_tackles',
    'caught_percent': 'caught_percent',
    'contested_catch_rate': 'contested_catch_rate',
    'contested_receptions': 'contested_receptions',
    'contested_targets': 'contested_targets',
    'declined_penalties': 'declined_penalties',
    'drop_rate': 'drop_rate',
    'drops': 'drops',
    'first_downs': 'first_downs',
    'franchise_id': 'franchise_id',
    'fumbles': 'fumbles',
    'grades_hands_drop': 'grades_hands_drop',
    'grades_hands_fumble': 'grades_hands_fumble',
    'grades_offense': 'grades_offense',
    'grades_pass_block': 'grades_pass_block',
    'grades_pass_route': 'grades_pass_route',
    'inline_rate': 'inline_rate',
    'inline_snaps': 'inline_snaps',
    'interceptions': 'interceptions',
    'longest': 'longest',
    'pass_block_rate': 'pass_block_rate',
    'pass_blocks': 'pass_blocks',
    'pass_plays': 'pass_plays',
    'penalties': 'penalties',
    'receptions': 'receptions',
    'route_rate': 'route_rate',
    'routes': 'routes',
    'slot_rate': 'slot_rate',
    'slot_snaps': 'slot_snaps',
    'targeted_qb_rating': 'targeted_qb_rating',
    'targets': 'targets',
    'touchdowns': 'touchdowns',
    'wide_rate': 'wide_rate',
    'wide_snaps': 'wide_snaps',
    'yards': 'yards',
    'yards_after_catch': 'yards_after_catch',
    'yards_after_catch_per_reception': 'yards_after_catch_per_reception',
    'yards_per_reception': 'yards_per_reception',
    'yprr': 'yprr'
}

def build_one(season: int):
    cfbd = load_cfbd_table(season, f"players_receiving_{season}.csv")
    pff_raw = load_pff_player_seasons(f"../data_extraction/data/pff/{season}", season=str(season), include_stats=True, stat_types=["receiving"])
    xref = load_xref_players()
    pff = pff_raw.merge(xref, on=["season","pff_player_id","pff_team_name"], how="left")

    keep = ["season","cfbd_player_id"]
    for _, raw in PFF_RECV_COLS.items():
        col = f"pff_{raw}"
        if col in pff.columns:
            keep.append(col)
    pff_ps = pff[keep].drop_duplicates(["season","cfbd_player_id"])

    cfbd["cfbd_player_id"] = cfbd["player_id"]

    m = cfbd.merge(pff_ps, on=["season","cfbd_player_id"], how="left")

    # Derived
    base_yards = m["rec_yards"] if "rec_yards" in m.columns else m.get("receiving_yards")
    if base_yards is not None and "routes" in m.columns:
        m["yards_per_route_run"] = safe_div(base_yards.fillna(0), m["routes"].fillna(0))
    if "targets" in m.columns and "routes" in m.columns:
        m["tgt_per_route"] = safe_div(m["targets"], m["routes"])

    if {"targets","pff_slot_targets","pff_wide_targets","pff_inline_targets"}.issubset(m.columns):
        m["slot_rate"] = safe_div(m["pff_slot_targets"], m["targets"])
        m["wide_rate"] = safe_div(m["pff_wide_targets"], m["targets"])
        m["inline_te_rate"] = safe_div(m["pff_inline_targets"], m["targets"])

    if {"targets","pff_man_targets","pff_zone_targets"}.issubset(m.columns):
        m["man_tgt_rate"] = safe_div(m["pff_man_targets"], m["targets"])
        m["zone_tgt_rate"] = safe_div(m["pff_zone_targets"], m["targets"])

    if "pff_separation_avg" in m.columns:
        m["separation_avg_yards"] = m["pff_separation_avg"]

    outdir = ensure_master_dir(season)
    outp = outdir / "receiving_master.csv"
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
