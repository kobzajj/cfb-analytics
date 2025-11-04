#!/usr/bin/env python
# scripts/xref_build_players_pff_cfbd.py
from __future__ import annotations
import argparse
import pandas as pd
import unidecode, re
import numpy as np

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
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", s)
    s = re.sub(r"[^\w\s-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

_POS_MAP = {
    # PFF/CFBD fine positions -> coarse groups
    "qb":"QB","rb":"RB","fb":"RB","wr":"WR","te":"TE", "hb":"RB",
    "lt":"OL","lg":"OL","c":"OL","rg":"OL","rt":"OL","ol":"OL",
    "de":"DL","dt":"DL","dl":"DL","edge":"DL", "ed":"DL",
    "lb":"LB","ilb":"LB","olb":"LB",
    "cb":"DB","s":"DB","ss":"DB","fs":"DB","db":"DB",
    "k":"K","p":"P","ls":"K"
}

def _pos_group(x: str) -> str:
    if not x: return ""
    elif pd.isna(x): return ""
    return _POS_MAP.get(x.strip().lower(), x.strip().upper())

def _body_match_score(h1, w1, h2, w2) -> float:
    # returns 1.0 when very close, down toward 0.0 as distance grows
    import math
    try:
        dh = abs(float(h1) - float(h2))
        dw = abs(float(w1) - float(w2))
    except Exception:
        return 0.5  # unknown -> neutral
    # tolerances: 2 inches, 15 lbs
    s = max(0.0, 1.0 - (dh/2.0 + dw/15.0)/2.0)
    return float(s)

def match_block_without_jersey(pf_t: pd.DataFrame, cf_t: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns (matches_df, ambiguous_df, no_match_df)
    """

    pf = pf_t.copy()
    cf = cf_t.copy()

    # normalize fields we will use
    pf["name_key"] = pf["player_name"].map(norm_name)
    cf["name_key"] = cf["player_name"].map(norm_name)

    # NEED TO UPDATE - START FROM HERE

    print(pf["position"].unique())
    print(cf["position_cfbd"].unique())

    pf["pos_group"] = pf.get("position", "").map(_pos_group)
    cf["pos_group"] = cf.get("position_cfbd", cf.get("position", "")).map(_pos_group)

    matches = []
    ambiguous = []
    no_match = []

    # Build a name->list[candidate rows] index for CFBD within team
    cf_idx = {}
    for _, r in cf.iterrows():
        cf_idx.setdefault(r["name_key"], []).append(r)

    for _, p in pf.iterrows():
        pk = p["name_key"]
        # 1) deterministic: exact name match and unique within team
        exact = cf_idx.get(pk, [])
        if len(exact) == 1:
            r = exact[0]
            matches.append(dict(
                season=p["season"],
                pff_player_id=p.get("pff_player_id", pd.NA),
                cfbd_player_id=r["player_id_cfbd"],
                pff_team_name=p["pff_team_name"],
                cfbd_team_id=r["team_id_cfbd"],
                name_pff=p["player_name"], name_cfbd=r["player_name"],
                position_pff=p.get("position", pd.NA), position_cfbd=r.get("position_cfbd", pd.NA),
                match_method="exact_name", match_score=100
            ))
            continue

        # 2) deterministic tie-break by position group (if multiple with same name)
        if len(exact) > 1:
            same_pos = [r for r in exact if _pos_group(r.get("position_cfbd", "")) == p.get("pos_group", "")]
            pool = same_pos or exact
            if len(pool) == 1:
                r = pool[0]
                matches.append(dict(
                    season=p["season"],
                    pff_player_id=p.get("pff_player_id", pd.NA),
                    cfbd_player_id=r["player_id_cfbd"],
                    pff_team_name=p["pff_team_name"],
                    cfbd_team_id=r["team_id_cfbd"],
                    name_pff=p["player_name"], name_cfbd=r["player_name"],
                    position_pff=p.get("position", pd.NA), position_cfbd=r.get("position_cfbd", pd.NA),
                    match_method="exact_name+posgroup", match_score=98
                ))
                continue
            # still multiple: fall through to fuzzy with scoring among this pool only

        # 3) fuzzy within team roster
        cf_names = cf["name_key"].unique().tolist()
        # removed parameter: scorer=fuzz.token_set_ratio,
        best = process.extract(pk, cf_names, limit=3)
        if not best:
            no_match.append(dict(season=p["season"], pff_player_id=p.get("pff_player_id", pd.NA),
                                 pff_team_name=p["pff_team_name"], name_pff=p["player_name"],
                                 reason="no_cfbd_name_candidate"))
            continue

        # Build scored candidates
        candidates = []
        for cand_name, name_sim, _ in best:
            crows = cf[cf["name_key"] == cand_name]
            for _, r in crows.iterrows():
                pos_bonus = 1.0 if _pos_group(r.get("position_cfbd","")) == p.get("pos_group","") else 0.0
                body = _body_match_score(p.get("height", np.nan), p.get("weight", np.nan),
                                         r.get("height", np.nan), r.get("weight", np.nan))
                composite = 0.85*(name_sim/100.0) + 0.10*pos_bonus + 0.05*body
                candidates.append((composite, name_sim, pos_bonus, body, r))

        candidates.sort(reverse=True, key=lambda x: (x[0], x[1]))
        if not candidates or candidates[0][0] < 0.92:
            no_match.append(dict(season=p["season"], pff_player_id=p.get("pff_player_id", pd.NA),
                                 pff_team_name=p["pff_team_name"], name_pff=p["player_name"],
                                 best_score=candidates[0][0] if candidates else np.nan,
                                 reason="below_threshold"))
            continue

        top = [c for c in candidates if abs(c[0] - candidates[0][0]) < 0.01]  # near-ties
        if len(top) > 1:
            # ambiguous → send to review
            for c in top:
                r = c[4]
                ambiguous.append(dict(
                    season=p["season"], pff_player_id=p.get("pff_player_id", pd.NA),
                    pff_team_name=p["pff_team_name"], name_pff=p["player_name"],
                    cfbd_player_id_candidate=r["player_id_cfbd"], name_cfbd=r["player_name"],
                    score=c[0], name_sim=c[1], pos_bonus=c[2], body_score=c[3]
                ))
            continue

        # unique best
        _, name_sim, pos_bonus, body, r = candidates[0]
        matches.append(dict(
            season=p["season"],
            pff_player_id=p.get("pff_player_id", pd.NA),
            cfbd_player_id=r["player_id_cfbd"],
            pff_team_name=p["pff_team_name"],
            cfbd_team_id=r["team_id_cfbd"],
            name_pff=p["player_name"], name_cfbd=r["player_name"],
            position_pff=p.get("position", pd.NA), position_cfbd=r.get("position_cfbd", pd.NA),
            match_method="fuzzy_composite", match_score=int(round(100*candidates[0][0])))
        )

    return pd.DataFrame(matches), pd.DataFrame(ambiguous), pd.DataFrame(no_match)

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
          [["season","player_id","player_name","team_id","team_name","position_cfbd"]]
          .copy())
    # cf["jersey"] = cf["jersey"].astype(str).str.extract(r"(\d+)")[0]
    cf = cf.rename(columns={"player_id":"player_id_cfbd","team_id":"team_id_cfbd"})

    # Attach cfbd team_id to the PFF rows using team crosswalk
    tx = team_xref[["season","pff_team_name","cfbd_team_id"]].dropna()
    pf = pff_df.merge(tx, on=["season","pff_team_name"], how="left").rename(columns={"cfbd_team_id":"team_id_cfbd"})

    # OLD VERSION WITH JERSEY MATCH
    # Build matches per (season, team_id_cfbd)
    # rows = []
    # for yr in sorted(seasons):
    #     pf_y = pf[pf["season"] == yr].copy()
    #     cf_y = cf[cf["season"] == yr].copy()
    #
    #     for tid in sorted(cf_y["team_id_cfbd"].dropna().unique()):
    #         cf_t = cf_y[cf_y["team_id_cfbd"] == tid].copy()
    #         pf_t = pf_y[pf_y["team_id_cfbd"] == tid].copy()
    #         if pf_t.empty or cf_t.empty:
    #             continue
    #         m = match_block(pf_t, cf_t)
    #         rows.append(m)
    #
    # out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=[
    #     "season","pff_player_id","cfbd_player_id","pff_team_name","cfbd_team_id",
    #     "name_pff","name_cfbd","jersey_pff","jersey_cfbd","position_pff","position_cfbd",
    #     "match_method","match_score"
    # ])
    # # De-dup to top-score per (season, pff_player_id or (name,team)) key
    # if "pff_player_id" in out.columns and out["pff_player_id"].notna().any():
    #     out = (out.sort_values(["season","pff_player_id","match_score"], ascending=[True,True,False])
    #              .drop_duplicates(["season","pff_player_id"]))
    # else:
    #     out = (out.sort_values(["season","name_pff","cfbd_team_id","match_score"], ascending=[True,True,True,False])
    #              .drop_duplicates(["season","name_pff","cfbd_team_id"]))
    # out["override"] = False
    # out["override_note"] = pd.NA
    # return out

    m_all, amb_all, miss_all = [], [], []
    for yr in sorted(seasons):
        pf_y = pf[pf["season"] == yr].copy()
        cf_y = cf[cf["season"] == yr].copy()

        for tid in sorted(cf_y["team_id_cfbd"].dropna().unique()):
            cf_t = cf_y[cf_y["team_id_cfbd"] == tid].copy()
            pf_t = pf_y[pf_y["team_id_cfbd"] == tid].copy()
            if pf_t.empty or cf_t.empty:
                continue
            m, amb, miss = match_block_without_jersey(pf_t, cf_t)
            m_all.append(m)
            amb_all.append(amb)
            miss_all.append(miss)

    matches = pd.concat(m_all, ignore_index=True) if m_all else pd.DataFrame()
    ambiguous = pd.concat(amb_all, ignore_index=True) if amb_all else pd.DataFrame()
    no_match = pd.concat(miss_all, ignore_index=True) if miss_all else pd.DataFrame()

    # De-dup to top-score per season+pff_player_id (when present) or name+team
    if "pff_player_id" in matches.columns and matches["pff_player_id"].notna().any():
        matches = (matches.sort_values(["season", "pff_player_id", "match_score"], ascending=[True, True, False])
                   .drop_duplicates(["season", "pff_player_id"]))
    else:
        matches = (matches.sort_values(["season", "name_pff", "cfbd_team_id", "match_score"],
                                       ascending=[True, True, True, False])
                   .drop_duplicates(["season", "name_pff", "cfbd_team_id"]))

    # Write review queues too
    Path("data/xref/review").mkdir(parents=True, exist_ok=True)
    ambiguous.to_csv("../data/xref/review/players_pff_cfbd_ambiguous.csv", index=False)
    no_match.to_csv("../data/xref/review/players_pff_cfbd_unmatched.csv", index=False)

    return matches

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