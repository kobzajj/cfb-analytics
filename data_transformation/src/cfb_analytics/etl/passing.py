import pandas as pd, numpy as np
from ..ep_model import compute_epa, EPModelStub

# columns in pbp file
# ['season', 'game_id', 'down', 'distance', 'yardline_100', 'yards_gained', 'passer_player_id', 'passer_player_name',
# 'rusher_player_id', 'rusher_player_name', 'receiver_player_id', 'receiver_player_name', 'is_pass', 'is_rush',
# 'complete', 'touchdown', 'points_scored', 'interception', 'sack', 'sack_yards']

def assemble_passing(pbp, rosters, parts, season):
    df = pbp.copy()
    df = df[df.get('is_pass', 0)==1].copy()
    g = df.groupby('passer_player_id', dropna=False)

    out = pd.DataFrame({
        'pass_attempts': g['is_pass'].sum(),
        'completions': g['complete'].sum() if 'complete' in df.columns else 0,
        'pass_yards': g['yards_gained'].sum(),
        'pass_td': g['touchdown'].sum(),
        'interceptions': g['interception'].sum(),
        'sacks_taken': (df.get('sack',0)==1).groupby(df['passer_player_id']).sum(),
        'sacks_yards_lost': (df.get('sack_yards',0)).groupby(df['passer_player_id']).sum() if 'sack_yards' in df.columns else 0,
        # 'air_yards': g['air_yards'].sum(min_count=1) if 'air_yards' in df.columns else 0, NEED TO ADD AIR YARDS
        # 'yac': g['yac'].sum(min_count=1) if 'yac' in df.columns else 0, NEED TO ADD YAC
        # 'pressures_faced': g['pressure'].sum(min_count=1) if 'pressure' in df.columns else 0, NEED TO ADD PRESSURES
    }).fillna(0)

    dropbacks = g['is_pass'].sum()
    if 'sack' in df.columns:
        dropbacks = dropbacks.add((df['sack']==1).groupby(df['passer_player_id']).sum(), fill_value=0)
    if 'scramble' in df.columns:
        dropbacks = dropbacks.add((df['scramble']==1).groupby(df['passer_player_id']).sum(), fill_value=0)
    out['dropbacks'] = dropbacks

    model = EPModelStub()
    df['epa'] = compute_epa(df, model)
    out = out.join(df.groupby('passer_player_id')['epa'].sum().rename('epa_total_pass'), how='left').fillna(0)
    out['epa_per_dropback'] = out['epa_total_pass'].div(out['dropbacks'].replace({0: np.nan}))

    out['completion_pct'] = out['completions'].div(out['pass_attempts'].replace({0: np.nan}))
    out['yards_per_att'] = out['pass_yards'].div(out['pass_attempts'].replace({0: np.nan}))
    out['adj_yards_per_att'] = (out['pass_yards'] + 20*out['pass_td'] - 45*out['interceptions']).div(out['pass_attempts'].replace({0: np.nan}))
    out['yards_per_dropback'] = out['pass_yards'].div(out['dropbacks'].replace({0: np.nan}))
    out['td_rate'] = out['pass_td'].div(out['pass_attempts'].replace({0: np.nan}))
    out['int_rate'] = out['interceptions'].div(out['pass_attempts'].replace({0: np.nan}))
    # out['air_yards_per_att'] = out['air_yards'].div(out['pass_attempts'].replace({0: np.nan})) if 'air_yards' in out.columns else np.nan
    # out['yac_per_comp'] = out['yac'].div(out['completions'].replace({0: np.nan})) if 'yac' in out.columns else np.nan
    # out['pressure_rate'] = out['pressures_faced'].div(out['dropbacks'].replace({0: np.nan}))
    out['sack_rate'] = out['sacks_taken'].div(out['dropbacks'].replace({0: np.nan}))

    out['success_rate'] = (df['epa']>0).groupby(df['passer_player_id']).mean()
    out['explosive_pass_rate'] = (df['yards_gained']>=20).groupby(df['passer_player_id']).mean()

    idx = rosters[rosters['season']==season].set_index('player_id')[['player_name','team_id','team_name','conference','position']]
    out = out.join(idx, how='left')
    out['season'] = season
    out = out.reset_index().rename(columns={'passer_player_id':'player_id'})

    if not parts.empty:
        psub = parts[['player_id','games','starts']].drop_duplicates('player_id')
        out = out.merge(psub, on='player_id', how='left')

    cols = ['season','player_id','player_name','team_id','team_name','conference','position',
            'games','starts','dropbacks','pass_attempts','sacks_taken','pressures_faced',
            'completions','pass_yards','pass_td','interceptions','sacks_yards_lost','air_yards','yac',
            'completion_pct','yards_per_att','adj_yards_per_att','yards_per_dropback','td_rate','int_rate',
            'air_yards_per_att','yac_per_comp','pressure_rate','sack_rate',
            'epa_total_pass','epa_per_dropback','success_rate','explosive_pass_rate']
    return out.reindex(columns=cols)
