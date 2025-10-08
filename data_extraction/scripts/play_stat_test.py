#!/usr/bin/env python
from __future__ import annotations
import os, sys
from pathlib import Path
import pandas as pd, pyarrow as pa, pyarrow.parquet as pq

try:
    import cfbd
except ImportError:
    print("Install cfbd: pip install -r requirements.txt", file=sys.stderr)
    raise

def get_client():
    token = os.environ.get("CFBD_API_KEY")
    if not token:
        raise RuntimeError("Please export CFBD_API_KEY")
    cfg = cfbd.Configuration(access_token=token)
    return cfbd.ApiClient(cfg)

def get_play_types():
    with get_client() as api:
        plays_api = cfbd.PlaysApi(api)
        
        return plays_api.get_play_types()

def get_play_stat_types():
    with get_client() as api:
        plays_api = cfbd.PlaysApi(api)
        
        return plays_api.get_play_stat_types()

def get_play_stats(game_id: int) -> pd.DataFrame:
    with get_client() as api:
        plays_api = cfbd.PlaysApi(api)
        try:
            play_stats = plays_api.get_play_stats(game_id=game_id)
        except Exception as e:
            print(f"[warn] play stats game={game_id}: {e}", file=sys.stderr)
        return play_stats

        # for i in range(1, 15):
        #     print('play by play week: ', i)
        #     try:
        #         plays_i = plays_api.get_plays(year, i)
        #     except Exception as e:
        #         print(f"[warn] plays week={i} {year}: {e}", file=sys.stderr); continue
        #     for pl in plays_i:
        #         d = pl.to_dict(); d["season"] = year; d["game_id"] = pl.game_id
        #         recs.append(d)

        
        # for g in games:
        #     print(g.id, g.season, g.week, g.home_team, g.home_points, g.away_team, g.away_points)
        # week_1_plays = plays_api.get_plays(2019, 1)
        # game_1_id = week_1_plays[0].game_id
        # for p in week_1_plays:
        #     if p.game_id == game_1_id:
        #         print(p.offense, p.offense_score, p.defense, p.defense_score, p.period, p.clock, p.yardline, p.down, p.distance, p.play_type, p.yards_gained, p.scoring)
                
    #     for g in games:
    #         try:
    #             plays = plays_api.get_plays(game_id=g.id)
    #         except Exception as e:
    #             print(f"[warn] plays game_id={g.id} {year}: {e}", file=sys.stderr); continue
    #         for pl in plays:
    #             d = pl.to_dict(); d["season"] = year; d["game_id"] = g.id
    #             recs.append(d)
        return pd.json_normalize(recs, max_level=1)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, nargs="+", default=[2019,2020,2021,2022,2023,2024])
    ap.add_argument("--rawroot", type=str, default="data/raw")
    args = ap.parse_args()
    rawroot = Path(args.rawroot)
    play_types = get_play_types()
    for pt in play_types:
        print(pt.id, pt.text, pt.abbreviation)
    play_stat_types = get_play_stat_types()
    for pst in play_stat_types:
        print(pst.id, pst.name)
    play_stats = get_play_stats(401112238)
    sorted_play_stats = sorted(play_stats, key=lambda PlayStat: PlayStat.play_id)
    for ps in sorted_play_stats:
        print("play id: ", ps.play_id, ps.athlete_id, ps.athlete_name, ps.stat_type, ps.stat)

if __name__ == "__main__":
    main()
