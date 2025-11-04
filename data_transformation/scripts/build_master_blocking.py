
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

PFF_BLOCK_COLS = {
    "pass_block_snaps": "pass_block_snaps",
    "pressures_allowed": "pressures_allowed",
    "sacks_allowed": "sacks_allowed",
    "hits_allowed": "hits_allowed",
    "hurries_allowed": "hurries_allowed",
    "pass_block_win_rate": "pass_block_win_rate",
}

def build_one(season: int):
    cfbd = load_cfbd_table(season, "blocking_cfbd.csv")
    pff_raw = load_pff_player_seasons(f"data/vendor/pff/{season}", include_stats=True)
    xref = load_xref_players()
    pff = pff_raw.merge(xref, on=["season","pff_player_id","pff_team_name"], how="left")

    keep = ["season","cfbd_player_id"]
    for _, raw in PFF_BLOCK_COLS.items():
        col = f"pff_{raw}"
        if col in pff.columns:
            keep.append(col)
    pff = pff[keep].copy()

    agg = {}
    for c in pff.columns:
        if not c.startswith("pff_"):
            continue
        agg[c] = "mean" if c.endswith("win_rate") else "sum"
    pff_ps = pff.groupby(["season","cfbd_player_id"], dropna=False).agg(agg).reset_index()

    m = cfbd.merge(pff_ps, on=["season","cfbd_player_id"], how="left")

    for c in ["pressures_allowed","sacks_allowed","hits_allowed","hurries_allowed","pass_block_snaps"]:
        pc = f"pff_{c}"
        if pc in m.columns:
            m[c] = m[pc]

    if {"pressures_allowed","pass_block_snaps"}.issubset(m.columns):
        m["pass_block_pressure_rate"] = safe_div(m["pressures_allowed"], m["pass_block_snaps"])

    if "pff_pass_block_win_rate" in m.columns:
        m["pass_block_win_rate"] = m["pff_pass_block_win_rate"]

    outdir = ensure_master_dir(season)
    outp = outdir / "blocking_master.csv"
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
