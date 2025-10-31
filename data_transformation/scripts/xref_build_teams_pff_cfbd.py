#!/usr/bin/env python
# scripts/xref_build_teams_pff_cfbd.py
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

try:
    from rapidfuzz import fuzz, process   # better fuzzy matching
    HAVE_RAPIDFUZZ = True
except Exception:
    import difflib
    HAVE_RAPIDFUZZ = False

from cfb_analytics.vendors.pff_adapter import load_pff_player_seasons

def norm_name(s: str) -> str:
    import unidecode, re
    s = unidecode.unidecode((s or "").lower())
    s = s.replace("&", "and")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def build_team_xref(pff_df: pd.DataFrame, cfbd_rosters: pd.DataFrame, seasons: list[int]) -> pd.DataFrame:
    # CFBD teams present in your rosters
    cfbd_teams = (
        cfbd_rosters.query("season in @seasons")[["season", "team_id", "team_name"]]
        .drop_duplicates()
        .assign(team_name_norm=lambda d: d["team_name"].map(norm_name))
    )

    # PFF team list present in your PFF player seasons
    pff_teams = (
        pff_df.query("season in @seasons")[["season", "pff_team_name"]]
        .drop_duplicates()
        .assign(pff_team_name_norm=lambda d: d["pff_team_name"].map(norm_name))
    )

    rows = []
    for yr in sorted(pff_teams["season"].dropna().unique()):
        pf = pff_teams[pff_teams["season"] == yr]
        cf = cfbd_teams[cfbd_teams["season"] == yr]

        tgt = cf["team_name_norm"].unique().tolist()

        for _, r in pf.iterrows():
            cand = r["pff_team_name_norm"]
            if HAVE_RAPIDFUZZ:
                best = process.extractOne(cand, tgt, scorer=fuzz.token_set_ratio)
                score = int(best[1]) if best else 0
                match = best[0] if best else None
            else:
                matches = difflib.get_close_matches(cand, tgt, n=1, cutoff=0.0)
                match = matches[0] if matches else None
                score = 100 if match == cand else (80 if matches else 0)

            if match is None:
                rows.append({
                    "season": yr, "pff_team_name": r["pff_team_name"],
                    "cfbd_team_id": pd.NA, "cfbd_team_name": pd.NA,
                    "match_method": "no_match", "match_score": 0, "override": False
                })
                continue

            cf_row = cf.loc[cf["team_name_norm"] == match].head(1).iloc[0]
            rows.append({
                "season": yr,
                "pff_team_name": r["pff_team_name"],
                "cfbd_team_id": int(cf_row["team_id"]),
                "cfbd_team_name": cf_row["team_name"],
                "match_method": "fuzzy_token_set" if HAVE_RAPIDFUZZ else "difflib",
                "match_score": score,
                "override": False
            })

    out = pd.DataFrame(rows).drop_duplicates(["season","pff_team_name"])
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", type=int, nargs="+", required=True, help="e.g., 2019 2020 2021 2022 2023 2024")
    ap.add_argument("--pff", type=str, default="../data_extraction/data/pff", help="dir containing season subfolders of PFF CSVs")
    ap.add_argument("--cfbd_rosters_root", type=str, default="../data_extraction/data/raw", help="dir with data/raw/{season}/rosters.csv")
    ap.add_argument("--out", type=str, default="data/xref/teams_pff_cfbd.csv")
    args = ap.parse_args()

    # Load PFF player-season rows (all seasons requested)
    frames = []
    for yr in args.seasons:
        frames.append(load_pff_player_seasons(f"{args.pff}/{yr}"), include_stats = False)
    pff_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["season","pff_team_name"])

    # Load CFBD rosters for team list
    rost_frames = []
    for yr in args.seasons:
        p = Path(args.cfbd_rosters_root) / str(yr) / "rosters.csv"
        if p.exists():
            rost_frames.append(pd.read_csv(p))
    if not rost_frames:
        raise SystemExit("No CFBD rosters found. Expected data/raw/{season}/rosters.csv")
    cfbd_rosters = pd.concat(rost_frames, ignore_index=True)

    out = build_team_xref(pff_df, cfbd_rosters, args.seasons)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"[ok] wrote {args.out}")

if __name__ == "__main__":
    main()