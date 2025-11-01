#!/usr/bin/env python
# scripts/xref_build_players_pff_cfbd.py
from __future__ import annotations
import argparse
import pandas as pd
import unidecode, re

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    from rapidfuzz import fuzz, process
    HAVE_RAPIDFUZZ = True
except Exception:
    import difflib
    HAVE_RAPIDFUZZ = False

from cfb_analytics.vendors.pff_adapter import load_pff_player_seasons

def norm_name(s: str) -> str:
    # import unidecode, re
    s = unidecode.unidecode((s or "").lower())
    # Remove suffixes and punctuation
    s = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def match_block(pf_t: pd.DataFrame, cf_t: pd.DataFrame) -> pd.DataFrame:
    """
    Deterministic matches first (name+jersey), then fuzzy on name.
    Returns rows with columns: season, pff_player_id, cfbd_player_id, pff_team_name, cfbd_team_id,
    name_pff, name_cfbd, jersey_pff, jersey_cfbd, position_pff, position_cfbd, match_method, match_score
    """
    pf = pf_t.copy()
    cf = cf_t.copy()

    pf["name_key"] = pf["player_name"].map(norm_name)
    cf["name_key"] = cf["player_name"].map(norm_name)

    # deterministic pass: exact (name_key + jersey)
    rows = []
    if "jersey" in pf.columns and "jersey" in cf.columns:
        exact = pf.merge(cf, on=["name_key","jersey"], suffixes=("_pff","_cfbd"))
        for _, r in exact.iterrows():
            rows.append(dict(
                season=r["season_pff"],
                pff_player_id=r.get("pff_player_id", pd.NA),
                cfbd_player_id=r["player_id_cfbd"],
                pff_team_name=r["pff_team_name"],
                cfbd_team_id=r["team_id_cfbd"],
                name_pff=r["player_name_pff"], name_cfbd=r["player_name_cfbd"],
                jersey_pff=r.get("jersey", pd.NA), jersey_cfbd=r.get("jersey", pd.NA),
                position_pff=r.get("position_pff", pd.NA), position_cfbd=r.get("position_cfbd", pd.NA),
                match_method="exact_name+jersey", match_score=100
            ))

    # Exclude already matched from fuzzy
    matched_cfbd = {r["cfbd_player_id"] for r in rows}
    matched_pff = {r["pff_player_id"] for r in rows if pd.notna(r["pff_player_id"])}

    pf_rem = pf[~pf.index.isin(exact.index.get_level_values(0))] if 'exact' in locals() else pf
    cf_rem = cf[~cf["player_id"].isin(matched_cfbd)] if matched_cfbd else cf

    # fuzzy pass: name only, constrained by team block
    cf_name_list = cf_rem[["name_key","player_id"]].drop_duplicates().values.tolist()
    cf_names = [x[0] for x in cf_name_list]

    for _, p in pf_rem.iterrows():
        key = p["name_key"]
        if HAVE_RAPIDFUZZ:
            best = process.extractOne(key, cf_names, scorer=fuzz.token_set_ratio)
            score = int(best[1]) if best else 0
            cand_name = best[0] if best else None
        else:
            matches = difflib.get_close_matches(key, cf_names, n=1, cutoff=0.0)
            cand_name = matches[0] if matches else None
            score = 100 if cand_name == key else (80 if matches else 0)

        if cand_name is None:
            continue
        if score < 92:   # threshold you can tune
            continue

        cf_pid = next(pid for nk, pid in cf_name_list if nk == cand_name)
        if cf_pid in matched_cfbd:
            continue

        rows.append(dict(
            season=p["season"],
            pff_player_id=p.get("pff_player_id", pd.NA),
            cfbd_player_id=cf_pid,
            pff_team_name=p["pff_team_name"],
            cfbd_team_id=cf_t["team_id"].iloc[0] if not cf_t.empty else pd.NA,
            name_pff=p["player_name"], name_cfbd=cf_t.loc[cf_t["player_id"]==cf_pid,"player_name"].iloc[0],
            jersey_pff=p.get("jersey", pd.NA), jersey_cfbd=pd.NA,
            position_pff=p.get("position", pd.NA), position_cfbd=pd.NA,
            match_method="fuzzy_name", match_score=score
        ))
        matched_cfbd.add(cf_pid)

    return pd.DataFrame(rows)

def build_player_xref(
    pff_df: pd.DataFrame,
    cfbd_rosters: pd.DataFrame,
    team_xref: pd.DataFrame,
    seasons: list[int]
) -> pd.DataFrame:
    # Normalize CFBD roster cols we need
    cf = (cfbd_rosters
          .rename(columns={"position":"position_cfbd"})
          [["season","player_id","player_name","team_id","team_name","position_cfbd","jersey"]]
          .copy())
    cf["jersey"] = cf["jersey"].astype(str).str.extract(r"(\d+)")[0]
    cf = cf.rename(columns={"player_id":"player_id_cfbd","team_id":"team_id_cfbd"})

    # Attach cfbd team_id to the PFF rows using team crosswalk
    tx = team_xref[["season","pff_team_name","cfbd_team_id"]].dropna()
    pf = pff_df.merge(tx, on=["season","pff_team_name"], how="left").rename(columns={"cfbd_team_id":"team_id_cfbd"})

    # Build matches per (season, team_id_cfbd)
    rows = []
    for yr in sorted(seasons):
        pf_y = pf[pf["season"] == yr].copy()
        cf_y = cf[cf["season"] == yr].copy()

        for tid in sorted(cf_y["team_id_cfbd"].dropna().unique()):
            cf_t = cf_y[cf_y["team_id_cfbd"] == tid].copy()
            pf_t = pf_y[pf_y["team_id_cfbd"] == tid].copy()
            if pf_t.empty or cf_t.empty:
                continue
            m = match_block(pf_t, cf_t)
            rows.append(m)

    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=[
        "season","pff_player_id","cfbd_player_id","pff_team_name","cfbd_team_id",
        "name_pff","name_cfbd","jersey_pff","jersey_cfbd","position_pff","position_cfbd",
        "match_method","match_score"
    ])
    # De-dup to top-score per (season, pff_player_id or (name,team)) key
    if "pff_player_id" in out.columns and out["pff_player_id"].notna().any():
        out = (out.sort_values(["season","pff_player_id","match_score"], ascending=[True,True,False])
                 .drop_duplicates(["season","pff_player_id"]))
    else:
        out = (out.sort_values(["season","name_pff","cfbd_team_id","match_score"], ascending=[True,True,True,False])
                 .drop_duplicates(["season","name_pff","cfbd_team_id"]))
    out["override"] = False
    out["override_note"] = pd.NA
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", type=int, nargs="+", required=True, help="e.g., 2019 2020 2021 2022 2023 2024")
    ap.add_argument("--pff", type=str, default="../../data_extraction/data/pff", help="dir with season subfolders")
    ap.add_argument("--cfbd_rosters_root", type=str, default="../../data_extraction/data/raw", help="dir with data/raw/{season}/rosters.csv")
    ap.add_argument("--team_xref", type=str, default="../data/xref/teams_pff_cfbd.csv")
    ap.add_argument("--out", type=str, default="../data/xref/players_pff_cfbd.csv")
    args = ap.parse_args()

    # Load PFF player-seasons
    pff_frames = []
    for yr in args.seasons:
        pff_frames.append(load_pff_player_seasons(f"{args.pff}/{yr}", yr, include_stats = False))
    pff_df = pd.concat(pff_frames, ignore_index=True) if pff_frames else pd.DataFrame()

    # Load CFBD rosters
    rost_frames = []
    for yr in args.seasons:
        p = Path(args.cfbd_rosters_root) / str(yr) / "rosters.csv"
        if p.exists():
            rost_frames.append(pd.read_csv(p))
    if not rost_frames:
        raise SystemExit("No CFBD rosters found.")

    cfbd_rosters = pd.concat(rost_frames, ignore_index=True)

    # Load team crosswalk
    team_xref = pd.read_csv(args.team_xref)

    out = build_player_xref(pff_df, cfbd_rosters, team_xref, args.seasons)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"[ok] wrote {args.out}")

if __name__ == "__main__":
    main()