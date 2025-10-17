#!/usr/bin/env python
from __future__ import annotations
import os, sys
from pathlib import Path
import pandas as pd, pyarrow as pa, pyarrow.parquet as pq
import random

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

# fetch rosters for a given year from CFBD API
def fetch_rosters(year: int) -> pd.DataFrame:
    with get_client() as api:
        teams_api = cfbd.TeamsApi(api)
        # players_api = cfbd.PlayersApi(api)
        rows = []
        teams = teams_api.get_teams(year=year)

        # loop through list of teams returned by get_teams API call
        for t in teams:
            print(t.school, t.conference)
            try:
                roster = teams_api.get_roster(year=year, team=t.school)
            except Exception as e:
                print(f"[warn] roster {t.school} {year}: {e}", file=sys.stderr); continue
            # loop through players in the current roster
            for p in roster:
                # for each player, add their details to the master list
                rows.append({
                    "season": year, "player_id": p.id,
                    "player_name": f"{(p.first_name or '').strip()} {(p.last_name or '').strip()}".strip(),
                    "team_id": t.id, "team_name": t.school, "conference": t.conference,
                    "position": p.position
                })
        # convert list of players to dataframe and return
        return pd.DataFrame(rows)

# for a given game ID, get play stats (used in combination with play by play to construct play details)
def get_play_stats(game_id: int) -> list:
    with get_client() as api:
        plays_api = cfbd.PlaysApi(api)
        try:
            # get play stats based on the requested game ID
            play_stats = plays_api.get_play_stats(game_id=game_id)
        except Exception as e:
            print(f"[warn] play stats game={game_id}: {e}", file=sys.stderr)
        # return the list of PlayStat objects
        return play_stats

# fetch all play by play details for a given year
def fetch_pbp(year: int) -> pd.DataFrame:
    with get_client() as api:
        games_api = cfbd.GamesApi(api); plays_api = cfbd.PlaysApi(api)
        recs = []

        # loop through 14 weeks in CFB regular season
        for i in range(1, 15):
            print('play by play week: ', i)

            # counters for logging
            play_count = 0
            play_stat_count = 0
            
            # initialize stats lists for the current week
            play_stats = []
            play_stats_game_ids = []
            
            try:
                # get all plays for the current week
                plays_i = plays_api.get_plays(year, i)
            except Exception as e:
                print(f"[warn] plays week={i} {year}: {e}", file=sys.stderr); continue

            # loop through plays to add stats
            for pl in plays_i:
                d = pl.to_dict(); d["season"] = year; d["game_id"] = pl.game_id

                # increment play counter
                play_count += 1

                # if the needed player stats aren't already available, fetch them from cfbd api
                # then search through stats from the appropriate game to find stats for the correct play
                # then add the stats and game ID to the list so they don't get requested again
                if pl.game_id not in play_stats_game_ids:
                    new_play_stats = get_play_stats(pl.game_id)
                    for ps in new_play_stats:

                        # increment play stat counter
                        play_stat_count += 1

                        # add stats if the current stats are for the correct play
                        if ps.play_id == pl.id:
                            # add passer details
                            if ps.stat_type.lower() in ("incompletion", "completion", "sack taken", "interception thrown"):
                                d["passer_player_name"] = ps.athlete_name
                                d["passer_player_id"] = ps.athlete_id
                                if ps.stat_type.lower() == "completion":
                                    d["completion"] = 1
                                if ps.stat_type.lower() == "sack taken":
                                    d["sack_yards"] = ps.stat
                            # add rusher details
                            elif ps.stat_type.lower() == "rush":
                                d["rusher_player_name"] = ps.athlete_name
                                d["rusher_player_id"] = ps.athlete_id
                            # add receiver details
                            elif ps.stat_type.lower() in ("target", "reception"):
                                d["receiver_player_name"] = ps.athlete_name
                                d["receiver_played_id"] = ps.athlete_id
                            # sack - TODO check if half sacks are recorded for multiple players
                            elif ps.stat_type.lower() == "sack":
                                d["sacker_name"] = ps.athlete_name
                                d["sacker_id"] = ps.athlete_id
                                d["sack"] = 1
                            # primary pass defender: pass breakup, interception
                            elif ps.stat_type.lower() in ("pass breakup", "interception"):
                                d["primary_pass_defender_name"] = ps.athlete_name
                                d["primary_pass_defender_id"] = ps.athlete_id
                                if ps.stat_type.lower() == "pass breakup":
                                    d["pass_breakup"] = 1
                                if ps.stat_type.lower() == "interception":
                                    d["interception"] = 1
                            # forced fumble
                            elif ps.stat_type.lower() == "fumble forced":
                                d["fumble_forced_name"] = ps.athlete_name
                                d["fumble_forced_id"] = ps.athlete_id
                                d["fumble"] = 1
                            # fumble recovery
                            elif ps.stat_type.lower() == "fumble recovered":
                                d["fumble_recovery_name"] = ps.athlete_name
                                d["fumble_recovery_id"] = ps.athlete_id
                                d["fumble"] = 1
                            # tackle
                            elif ps.stat_type.lower() == "tackle":
                                d["tackler_name"] = ps.athlete_name
                                d["tackler_id"] = ps.athlete_id
                            # touchdown
                            elif ps.stat_type.lower() == "touchdown":
                                d["touchdown"] = 1

                    # add all new play_stats objects to the list
                    play_stats.extend(new_play_stats)
                    # add new game_id to the list
                    play_stats_game_ids.append(pl.game_id)

                # if the needed player stats are already available, search through all stats to find relevant ones
                else:
                    for ps in play_stats:
                        # if current play stat is linked to the current play, add details to the play object
                        if ps.play_id == pl.id:
                            # add passer details
                            if ps.stat_type.lower() in ("incompletion", "completion", "sack taken",
                                                        "interception thrown"):
                                d["passer_player_name"] = ps.athlete_name
                                d["passer_player_id"] = ps.athlete_id
                                if ps.stat_type.lower() == "completion":
                                    d["completion"] = 1
                                if ps.stat_type.lower() == "sack taken":
                                    d["sack_yards"] = ps.stat
                            # add rusher details
                            elif ps.stat_type.lower() == "rush":
                                d["rusher_player_name"] = ps.athlete_name
                                d["rusher_played_id"] = ps.athlete_id
                            # add receiver details
                            elif ps.stat_type.lower() in ("target", "reception"):
                                d["receiver_player_name"] = ps.athlete_name
                                d["receiver_played_id"] = ps.athlete_id
                            # sack - TODO check if half sacks are recorded for multiple players
                            elif ps.stat_type.lower() == "sack":
                                d["sacker_name"] = ps.athlete_name
                                d["sacker_id"] = ps.athlete_id
                                d["sack"] = 1
                            # primary pass defender: pass breakup, interception
                            elif ps.stat_type.lower() in ("pass breakup", "interception"):
                                d["primary_pass_defender_name"] = ps.athlete_name
                                d["primary_pass_defender_id"] = ps.athlete_id
                                if ps.stat_type.lower() == "pass breakup":
                                    d["pass_breakup"] = 1
                                if ps.stat_type.lower() == "interception":
                                    d["interception"] = 1
                            # forced fumble
                            elif ps.stat_type.lower() == "fumble forced":
                                d["fumble_forced_name"] = ps.athlete_name
                                d["fumble_forced_id"] = ps.athlete_id
                                d["fumble"] = 1
                            # fumble recovery
                            elif ps.stat_type.lower() == "fumble recovered":
                                d["fumble_recovery_name"] = ps.athlete_name
                                d["fumble_recovery_id"] = ps.athlete_id
                            # tackle
                            elif ps.stat_type.lower() == "tackle":
                                d["tackler_name"] = ps.athlete_name
                                d["tackler_id"] = ps.athlete_id
                            # touchdown
                            elif ps.stat_type.lower() == "touchdown":
                                d["touchdown"] = 1

                recs.append(d)

            # display game count, play count, and play stat count for the week
            print('week ', i, 'games: ', len(play_stats_game_ids))
            print('week ', i, 'plays: ', play_count)
            print('week ', i, 'play stats: ', play_stat_count)
        
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
        print(random.choice(recs))
        print(random.choice(recs))
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
