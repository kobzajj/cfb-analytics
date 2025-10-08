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

def fetch_rosters(year: int) -> pd.DataFrame:
    with get_client() as api:
        teams_api = cfbd.TeamsApi(api)
        # players_api = cfbd.PlayersApi(api)
        rows = []
        teams = teams_api.get_teams(year=year)
        
        # for t in teams:
        #     print(t.school)
        # look at Nebraska as an example
        # try:
        #     roster = teams_api.get_roster(year=year, team='Nebraska')
        # except Exception as e:
        #     print(f"[warn] roster Nebraska {year}: {e}", file=sys.stderr)
        # for p in roster:
        #     print(p.id, p.first_name, p.last_name, p.position)
        #     rows.append({
        #         "season": year, "player_id": p.id,
        #         "player_name": f"{(p.first_name or '').strip()} {(p.last_name or '').strip()}".strip(),
        #         "team_id": 12345, "team_name": 'Nebraska', "conference": 'Big Ten',
        #         "position": p.position
        #     })

        for t in teams:
            print(t.school, t.conference)
            try:
                roster = teams_api.get_roster(year=year, team=t.school)
            except Exception as e:
                print(f"[warn] roster {t.school} {year}: {e}", file=sys.stderr); continue
            for p in roster:
                rows.append({
                    "season": year, "player_id": p.id,
                    "player_name": f"{(p.first_name or '').strip()} {(p.last_name or '').strip()}".strip(),
                    "team_id": t.id, "team_name": t.school, "conference": t.conference,
                    "position": p.position
                })
        return pd.DataFrame(rows)

def get_play_stats(game_id: int) -> pd.DataFrame:
    with get_client() as api:
        plays_api = cfbd.PlaysApi(api)
        try:
            play_stats = plays_api.get_play_stats(game_id=game_id)
        except Exception as e:
            print(f"[warn] play stats game={game_id}: {e}", file=sys.stderr)
        return play_stats

def fetch_pbp(year: int) -> pd.DataFrame:
    with get_client() as api:
        games_api = cfbd.GamesApi(api); plays_api = cfbd.PlaysApi(api)
        recs = []

        for i in range(1, 15):
            print('play by play week: ', i)
            
            # initialize stats lists for the current week
            play_stats = []
            play_stats_game_ids = []
            
            try:
                plays_i = plays_api.get_plays(year, i)
            except Exception as e:
                print(f"[warn] plays week={i} {year}: {e}", file=sys.stderr); continue

            for pl in plays_i:
                d = pl.to_dict(); d["season"] = year; d["game_id"] = pl.game_id

                # if the needed player stats aren't already available, fetch them from cfbd api
                # then search through stats from the appropriate game to find stats for the correct play
                # then add the stats and game ID to the list so they don't get requested again
                if pl.game_id not in play_stats_game_ids:
                    new_play_stats = get_play_stats(pl.id) # check if this is the correct way to reference play ID
                    for ps in new_play_stats:
                        # add stats if the current stats are for the correct play
                        if ps.play_id == pl.id:
                            # add passer details
                            if ps.stat_type.lower() in ("incompletion", "completion", "sack taken", "interception thrown"):
                                d["passer_player_name"] = ps.athlete_name
                                d["passer_player_id"] = ps.athlete_id
                                if ps.stat_type.lower() == "completion":
                                    d["completion"] = True
                            # add rusher details
                            elif ps.stat_type.lower() == "rush":
                                d["rusher_player_name"] = ps.athlete_name
                                d["rusher_played_id"] = ps.athlete_id
                            # add receiver details
                            elif ps.stat_type.lower() in ("target", "reception"):
                                d["receiver_player_name"] = ps.athlete_name
                                d["receiver_played_id"] = ps.athlete_id
                            # sack
                            elif ps.stat_type.lower() = "sack":
                                #TODO
                            # interception
                            elif ps.stat_type.lower() in (""):
                                #TODO
                            # forced fumble
                            elif ps.stat_type.lower() in (""):
                                #TODO
                            # fumble recovery
                            elif ps.stat_type.lower() in (""):
                                #TODO

                        # PLAY STAT TYPE NAMES
                        # 1 Incompletion
                        # 2 Target
                        # 3 Pass Breakup
                        # 4 Completion
                        # 5 Reception
                        # 6 Tackle
                        # 7 Rush
                        # 8 Fumble
                        # 9 Fumble Forced
                        # 10 Fumble Recovered
                        # 11 Sack Taken
                        # 12 Sack
                        # 13 Kickoff
                        # 14 Onside Kick
                        # 15 Kickoff Return
                        # 16 Punt
                        # 17 Punt Block
                        # 18 FG Attempt Blocked
                        # 19 Field Goal Block
                        # 20 Interception Thrown
                        # 21 Interception
                        # 22 Touchdown
                        # 23 Field Goal Attempt
                        # 24 Field Goal Made
                        # 25 Field Goal Missed

                # if the needed player stats are already available, search through all stats to find relevant ones
                else:
                    for ps in play_stats:
                        if ps.play_id == pl.id:

                


                recs.append(d)

        
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
    for yr in args.years:
        outdir = rawroot / str(yr); outdir.mkdir(parents=True, exist_ok=True)
        rost = fetch_rosters(yr); rost.to_csv(outdir/"rosters.csv", index=False)
        pbp = fetch_pbp(yr)
        pq.write_table(pa.Table.from_pandas(pbp, preserve_index=False), outdir/"pbp_cfbd_raw.parquet")
        print("Wrote", outdir)

if __name__ == "__main__":
    main()
