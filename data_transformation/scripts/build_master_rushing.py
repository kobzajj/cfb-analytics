
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
    "rush_attempts": "rush_attempts",
    "rushing_yards": "rushing_yards",
    "rushing_tds": "rushing_tds",
    "yards_before_contact": "yards_before_contact",
    "yards_after_contact": "yards_after_contact",
    "broken_tackles": "broken_tackles",
    "forced_missed_tackles": "forced_missed_tackles",
    "rpo_carries": "rpo_carries",
    "read_option_carries": "read_option_carries",
    "expected_yards": "expected_yards",
}

def build_one(season: int):
    cfbd = load_cfbd_table(season, "rushing_cfbd.csv")
    pff_raw = load_pff_player_seasons(f"data/vendor/pff/{season}", include_stats=True)
    xref = load_xref_players()
    pff = pff_raw.merge(xref, on=["season","pff_player_id","pff_team_name"], how="left")

    keep = ["season","cfbd_player_id"]
    for _, raw in PFF_RUSH_COLS.items():
        col = f"pff_{raw}"
        if col in pff.columns:
            keep.append(col)
    pff = pff[keep].copy()

    agg = {c: "sum" for c in pff.columns if c.startswith("pff_")}
    pff_ps = pff.groupby(["season","cfbd_player_id"], dropna=False).agg(agg).reset_index()

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
