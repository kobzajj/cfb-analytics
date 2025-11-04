
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

PFF_DEF_COLS = {
    "def_snaps": "def_snaps",
    "total_tackles": "total_tackles",
    "missed_tackles": "missed_tackles",
    "pressures": "pressures",
    "sacks": "sacks",
    "qb_hits": "qb_hits",
    "hurries": "hurries",
    "targets": "targets",
    "receptions_allowed": "receptions_allowed",
    "yards_allowed": "yards_allowed",
    "td_allowed": "td_allowed",
    "interceptions": "interceptions",
    "pass_breakups": "pass_breakups",
    "passes_defensed": "passes_defensed",
    "stop_rate": "stop_rate",
    "passer_rating_allowed": "passer_rating_allowed",
    "coverage_success_rate_allowed": "coverage_success_rate_allowed",
    "explosive_allowed_rate": "explosive_allowed_rate",
}

def build_one(season: int):
    cfbd = load_cfbd_table(season, "defense_cfbd.csv")
    pff_raw = load_pff_player_seasons(f"data/vendor/pff/{season}", include_stats=True)
    xref = load_xref_players()
    pff = pff_raw.merge(xref, on=["season","pff_player_id","pff_team_name"], how="left")

    keep = ["season","cfbd_player_id"]
    for _, raw in PFF_DEF_COLS.items():
        col = f"pff_{raw}"
        if col in pff.columns:
            keep.append(col)
    pff = pff[keep].copy()

    rate_suffixes = ("_rate", "rating")
    agg = {}
    for c in pff.columns:
        if not c.startswith("pff_"):
            continue
        agg[c] = "mean" if c.endswith(rate_suffixes) else "sum"
    pff_ps = pff.groupby(["season","cfbd_player_id"], dropna=False).agg(agg).reset_index()

    m = cfbd.merge(pff_ps, on=["season","cfbd_player_id"], how="left")

    for c in ["pressures","sacks","qb_hits","hurries","targets","receptions_allowed","yards_allowed","td_allowed",
              "interceptions","pass_breakups","passes_defensed","total_tackles","missed_tackles"]:
        pc = f"pff_{c}"
        if pc in m.columns:
            m[c] = m[pc]

    if {"pressures","def_snaps"}.issubset(m.columns):
        m["pressure_rate"] = safe_div(m["pressures"], m["def_snaps"])

    for c in ["stop_rate","coverage_success_rate_allowed","explosive_allowed_rate","passer_rating_allowed"]:
        pc = f"pff_{c}"
        if pc in m.columns:
            m[c] = m[pc]

    outdir = ensure_master_dir(season)
    outp = outdir / "defense_master.csv"
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
