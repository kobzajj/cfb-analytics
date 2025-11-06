
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
    p = Path(f"../data/processed/{season}/{filename}")
    if not p.exists():
        raise SystemExit(f"CFBD table not found: {p}")
    return pd.read_csv(p)

def load_xref_players() -> pd.DataFrame:
    p = Path("data/xref/players_pff_cfbd.csv")
    if not p.exists():
        raise SystemExit("Player crosswalk not found: data/xref/players_pff_cfbd.csv")
    return pd.read_csv(p)[["season","cfbd_player_id","pff_player_id","pff_team_name"]]

PFF_RECV_COLS = {
    "targets": "targets",
    "receptions": "receptions",
    "receiving_yards": "receiving_yards",
    "receiving_tds": "receiving_tds",
    "drops": "drops",
    "air_yards": "air_yards",
    "yac": "yac",
    "routes": "routes",
    "slot_targets": "slot_targets",
    "wide_targets": "wide_targets",
    "inline_targets": "inline_targets",
    "man_targets": "man_targets",
    "zone_targets": "zone_targets",
    "separation_avg": "separation_avg",
}

def build_one(season: int):
    cfbd = load_cfbd_table(season, "receiving_cfbd.csv")
    pff_raw = load_pff_player_seasons(f"data/vendor/pff/{season}", include_stats=True)
    xref = load_xref_players()
    pff = pff_raw.merge(xref, on=["season","pff_player_id","pff_team_name"], how="left")

    keep = ["season","cfbd_player_id"]
    for _, raw in PFF_RECV_COLS.items():
        col = f"pff_{raw}"
        if col in pff.columns:
            keep.append(col)
    pff_ps = pff[keep].drop_duplicates(["season","cfbd_player_id"])

    m = cfbd.merge(pff_ps, on=["season","cfbd_player_id"], how="left")

    for c in ["air_yards","yac","drops","routes"]:
        pc = f"pff_{c}"
        if pc in m.columns:
            m[c] = m[pc]

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
