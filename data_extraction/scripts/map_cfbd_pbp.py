#!/usr/bin/env python
from __future__ import annotations
from pathlib import Path
import pandas as pd, numpy as np, pyarrow.parquet as pq, pyarrow as pa

def calculate_next_possession(play):
    if play['interception'] == 1:
        return -1
    elif play['fumble_recovery_name']:
        return -1
    elif play['down'] == 4 and (play['is_rush'] == 1 or play['is_pass'] == 1) and play['yards_gained'] < play['distance']:
        return -1
    else:
        return 1

def calculate_next_down(play):
    next_down = 0
    if play['touchdown'] == 1 or play['safety'] == 1 or play['fg_attempt'] == 1 or play['punt_attempt'] == 1:
        next_down = np.nan
    elif play['next_possession'] == -1:
        next_down = 1
    elif play['down'] == 4:
        next_down = 1
    elif play['down'] in [1,2,3]:
        if play['yards_gained'] >= play['distance']:
            next_down = 1
        else:
            next_down = play['down'] + 1
    return next_down

def calculate_next_distance(play):
    if play['touchdown'] == 1 or play['safety'] == 1 or play['fg_attempt'] == 1 or play['punt_attempt'] == 1:
        return np.nan
    elif play['next_down'] == 1:
        if play['yardline_100'] - play['yards_gained'] < 10:
            return play['yardline_100'] - play['yards_gained']
        else:
            return 10
    else:
        return play['distance'] - play['yards_gained']

def calculate_next_yl(play):
    if play['touchdown'] == 1 or play['safety'] == 1 or play['fg_attempt'] == 1 or play['punt_attempt'] == 1:
        return np.nan
    else:
        return play['yardline_100'] - play['yards_gained']

def map_cfbd_to_standard(df: pd.DataFrame) -> pd.DataFrame:
    
    # COLUMNS OF DATAFRAME BEING PROCESSED
    # ['id', 'driveId', 'gameId', 'driveNumber', 'playNumber', 'offense', 'offenseConference', 
    # 'offenseScore', 'defense', 'home', 'away', 'defenseConference', 'defenseScore', 'period', 
    # 'offenseTimeouts', 'defenseTimeouts', 'yardline', 'yardsToGoal', 'down', 'distance', 'yardsGained', 
    # 'scoring', 'playType', 'playText', 'wallclock', 'ppa', 'season', 'game_id', 'clock.seconds', 'clock.minutes']

    # PLAY TYPE NAMES AND ABBREVIATIONS
    # 5 Rush RUSH (run)
    # 24 Pass Reception REC (pass)
    # 3 Pass Incompletion None (pass)
    # 53 Kickoff K (n/a)
    # 52 Punt PUNT (n/a)
    # 8 Penalty PEN (n/a)
    # 21 Timeout TO (n/a)
    # 7 Sack None (pass)
    # 68 Rushing Touchdown TD (run)
    # 67 Passing Touchdown TD (pass)
    # 2 End Period EP (n/a)
    # 59 Field Goal Good FG (n/a)
    # 26 Pass Interception Return INTR (pass)
    # 65 End of Half EH (n/a)
    # 9 Fumble Recovery (Own) None
    # 29 Fumble Recovery (Opponent) None
    # 60 Field Goal Missed FGM
    # 12 Kickoff Return (Offense) None
    # 36 Interception Return Touchdown TD
    # 18 Blocked Field Goal BFG
    # 17 Blocked Punt BP
    # 20 Safety SF
    # 32 Kickoff Return Touchdown TD
    # 39 Fumble Return Touchdown TD
    # 999 Uncategorized None
    # 57 Defensive 2pt Conversion D2P
    # 37 Blocked Punt Touchdown None
    # 40 Missed Field Goal Return AFG
    # 34 Punt Return Touchdown TD
    # 70 placeholder pla
    # 41 Missed Field Goal Return Touchdown None
    # 16 Two Point Rush None
    # 66 End of Game EG
    # 63 Interception INT
    # 38 Blocked Field Goal Touchdown None
    # 51 Pass PASS
    # 56 2pt Conversion 2P
    # 61 Extra Point Good XP
    # 62 Extra Point Missed EPM
    # 4 Pass Completion PASS
    # 6 Pass Interception INT
    # 78 Offensive 1pt Safety SF
    # 43 Blocked PAT None
    # 13 Kickoff Return (Defense) None
    # 14 Punt Return None
    # 15 Two Point Pass None
    # 79 End of Regulation ER
    # 800 Start of Period None

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


    out = pd.DataFrame(index=df.index)

    out['season'] = df.get('season') # from play data
    out['game_id'] = df.get('game_id') # from play data
    out['down'] = df.get('down') # from play data
    out['distance'] = df.get('distance') # from play data
    out['yardline_100'] = df.get('yardsToGoal', df.get('yardline')) # from play data
    out['yards_gained'] = df.get('yardsGained') # from play data

    # Participants
    out['passer_player_id'] = df.get('passer_player_id') # from play stat data
    out['passer_player_name'] = df.get('passer_player_name')
    out['rusher_player_id'] = df.get('rusher_player_id') # from play stat data
    out['rusher_player_name'] = df.get('rusher_player_name')
    out['receiver_player_id'] = df.get('receiver_player_id') # from play stat data
    out['receiver_player_name'] = df.get('receiver_player_name')
    out['fumble_recovery_id'] = df.get('fumble_recovery_id')
    out['fumble_recovery_name'] = df.get('fumble_recovery_name')

    # Outcomes
    pt = (df.get('playType')).astype(str).str.lower() # from play data
    out['is_pass'] = pt.str.contains('pass|sack|interception|incomplet').astype('int64') # from play data (play type)
    out['is_rush'] = pt.str.contains('rush|run|kneel|draw').astype('int64') & (~out['is_pass'].astype('int64')) # from play data (play type)
    out['complete'] = df.get('completion', 0) # from play stat data
    out['touchdown'] = df.get('touchdown', 0) # from play stat data
    out['safety'] = pt.str.contains('safety').astype('int64')
    out['points_scored'] = np.where(out['touchdown'] > 0, 7, np.where(out['safety'] > 0, -2, 0)) # from play stat data (td and safety)
    out['interception'] = df.get('interception', 0) # from play stat data
    out['sack'] = df.get('sack', 0) # from play stat data
    out['sack_yards'] = df.get('sack_yards', 0) # from play stat data
    out['fumble'] = df.get('fumble', 0)
    out['fg_attempt'] = pt.str.contains('field goal').astype('int64')
    out['punt_attempt'] = pt.str.contains('punt').astype('int64')
    # out['scramble'] = df.get('scramble', 0) # TODO

    # Air/YAC if present
    # out['air_yards'] = df.get('air_yards') # TODO
    # out['yac'] = df.get('yards_after_catch') # TODO


    # next down, distance, and yardline calculation are needed for EPA calculation
    # next state scenarios:
    # 1) base case: next down = 1 if yards_gained > distance, else if down<4 down+1, else 1 (if down = 4)
    # 2) touchdown: next down, next distance, next_yardline_100 = NaN, points scored = 7
    # 3) safety: next down, next distance, next_yardline_100 = NaN, points scored = -2
    # 4) turnover on downs: down and distance is 1st and 10, from resulting yardline, with flipped possession flag
    # 5) interception: down and distance is 1st and 10, from resulting yardline, with flipped possession flag
    # 6) fumbles: resulting down if recovered by offense (fumble recovery player is nan) otherwise first and 10 at new spot
    # 7) FG made: resulting down and distance = NaN, points scored = 3
    # 8) Penalty:
    # 9) No Play:
    # note: need a turnover flag that can be passed to the EPA model to subtract the EP after value

    # points scored for touchdowns and safeties already set above, so need to do the following:
    # 1) check if play is a passing or running play (don't worry about others)
    # 2) if a turnover (downs, fumble, or int) flip the possession and set to 1st and 10
    # 3) update yardline based on yards gained and current yardline
    # 4) update down and distance based on yards gained and previous down and distance
    # 5) check for goal to go

    out['next_possession'] = out.apply(calculate_next_possession, axis=1)
    out['next_down'] = out.apply(calculate_next_down, axis=1)
    out['next_distance'] = out.apply(calculate_next_distance, axis=1)
    out['next_yardline_100'] = out.apply(calculate_next_yl, axis=1)

    # Next state (not available -> NaN) - commented out for now
    # out['next_down'] = df.get('next_down')
    # out['next_distance'] = df.get('next_distance')
    # out['next_yardline_100'] = df.get('next_yardline_100')

    out['ppa_connelly'] = df['ppa']

    return out

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--year', type=int, required=True)
    ap.add_argument('--rawdir', type=str, default='data/raw')
    args = ap.parse_args()
    p = Path(args.rawdir)/str(args.year)/'pbp_cfbd_raw.parquet'
    df = pd.read_parquet(p)
    

    # print(list(df.columns))
    # print(df.head(10))
    # for i in range(10):
    #     print(df['playText'][i])
    

    mapped = map_cfbd_to_standard(df)

    print(list(mapped.columns))
    print('Number of plays in dataset: ', len(mapped))

    # print random plays for debugging
    for i in range(10):
        print(mapped.sample(n=1))

    outp = Path(args.rawdir)/str(args.year)/'pbp.parquet'
    pq.write_table(pa.Table.from_pandas(mapped, preserve_index=False), outp)
    print('Wrote', outp)

if __name__ == '__main__':
    main()
