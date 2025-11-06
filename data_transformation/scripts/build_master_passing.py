
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

PFF_PASS_COLS = {
    "player_game_count": "player_game_count",
    "accuracy_percent": "accuracy_percent",
    "aimed_passes": "aimed_passes",
    "attempts": "attempts",
    "avg_depth_of_target": "avg_depth_of_target",
    "avg_time_to_throw": "avg_time_to_throw",
    "bats": "bats",
    "big_time_throws": "big_time_throws",
    "btt_rate": "btt_rate",
    "completion_percent": "completion_percent",
    "completions": "completions",
    "def_gen_pressures": "def_gen_pressures",
    "drop_rate": "drop_rate",
    "dropbacks": "dropbacks",
    "drops": "drops",
    "first_downs": "first_downs",
    "grades_hands_fumble": "grades_hands_fumble",
    "grades_offense": "grades_offense",
    "grades_pass": "grades_pass",
    "grades_run": "grades_run",
    "hit_as_threw": "hit_as_threw",
    "interceptions": "interceptions",
    "passing_snaps": "passing_snaps",
    "penalties": "penalties",
    "declined_penalties": "declined_penalties",
    "pressure_to_sack_rate": "pressure_to_sack_rate",
    "qb_rating": "qb_rating",
    "sack_percent": "sack_percent",
    "sacks": "sacks",
    "scrambles": "scrambles",
    "spikes": "spikes",
    "thrown_aways": "thrown_aways",
    "touchdowns": "touchdowns",
    "turnover_worthy_plays": "turnover_worthy_plays",
    "twp_rate": "twp_rate",
    "yards": "yards",
    "ypa": "ypa",
}

def build_one(season: int):
    cfbd = load_cfbd_table(season, f"players_passing_{season}.csv")
    pff_raw = load_pff_player_seasons(f"../data_extraction/data/pff/{season}", season=str(season), include_stats=True, stat_types=["passing"])
    xref = load_xref_players()
    pff = pff_raw.merge(xref, on=["season","pff_player_id","pff_team_name"], how="left")

    keep = ["season", "cfbd_player_id"]
    for _, raw in PFF_PASS_COLS.items():
        col = f"pff_{raw}"
        if col in pff.columns:
            keep.append(col)
    pff_ps = pff[keep].drop_duplicates(["season", "cfbd_player_id"])

    # print(pff_ps.columns.tolist())
    # print(cfbd.columns.tolist())

    cfbd["cfbd_player_id"] = cfbd["player_id"]

    m = cfbd.merge(pff_ps, on=["season", "cfbd_player_id"], how="left")

    if "pff_dropbacks" in m.columns:
        m["dropbacks"] = m["pff_dropbacks"].fillna(m.get("dropbacks"))
    if "pff_pressures" in m.columns:
        m["pressures_faced"] = m["pff_pressures"]
        m["pressure_rate"] = safe_div(m["pff_pressures"].fillna(0), m.get("dropbacks", 0).fillna(0))
    if "pff_air_yards" in m.columns:
        m["air_yards"] = m["pff_air_yards"]
    if "pff_yac" in m.columns:
        m["yac"] = m["pff_yac"]
    if "pff_exp_completion_prob" in m.columns:
        m["exp_completion_pct"] = m["pff_exp_completion_prob"] * 100.0
        if "completion_pct" in m.columns:
            m["cpoe"] = m["completion_pct"] - m["exp_completion_pct"]

    outdir = ensure_master_dir(season)
    outp = outdir / "passing_master.csv"
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
