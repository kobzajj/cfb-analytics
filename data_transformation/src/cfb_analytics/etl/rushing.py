import pandas as pd, numpy as np
from ..ep_model import compute_epa, EPModelStub

def assemble_rushing(pbp, rosters, parts, season):
    df = pbp.copy()
    df = df[df.get('is_rush',0)==1].copy()
    g = df.groupby('rusher_player_id', dropna=False)
    out = pd.DataFrame({
        'rush_att': g['is_rush'].sum(),
        'rush_yards': g['yards_gained'].sum(),
        'rush_td': g['touchdown'].sum(),
        'fumbles': g['fumble_lost'].sum() if 'fumble_lost' in df.columns else 0,
        'yards_before_contact': g['yards_before_contact'].sum(min_count=1) if 'yards_before_contact' in df.columns else 0,
        'broken_tackles': g['broken_tackles'].sum(min_count=1) if 'broken_tackles' in df.columns else 0,
        'forced_missed_tackles': g['forced_missed_tackles'].sum(min_count=1) if 'forced_missed_tackles' in df.columns else 0,
    }).fillna(0)

    model = EPModelStub()
    df['epa'] = compute_epa(df, model)
    out = out.join(df.groupby('rusher_player_id')['epa'].sum().rename('epa_total_rush'), how='left').fillna(0)
    out['epa_per_rush'] = out['epa_total_rush'].div(out['rush_att'].replace({0: np.nan}))

    out['yards_per_carry'] = out['rush_yards'].div(out['rush_att'].replace({0: np.nan}))
    out['td_rate'] = out['rush_td'].div(out['rush_att'].replace({0: np.nan}))
    out['fumble_rate'] = out['fumbles'].div(out['rush_att'].replace({0: np.nan}))

    out['success_rate'] = (df['epa']>0).groupby(df['rusher_player_id']).mean()
    out['explosive_rush_rate'] = (df['yards_gained']>=10).groupby(df['rusher_player_id']).mean()

    out['rpo_carry_rate'] = df['rpo'].groupby(df['rusher_player_id']).mean() if 'rpo' in df.columns else np.nan
    out['read_option_rate'] = df['read_option'].groupby(df['rusher_player_id']).mean() if 'read_option' in df.columns else np.nan
    out['yards_over_expected_per_att'] = np.nan  # placeholder for xYPC model

    out['epa_rush_early'] = df.loc[df['down'].isin([1,2])].groupby('rusher_player_id')['epa'].mean()
    out['epa_rush_short'] = df.loc[df['distance']<=2].groupby('rusher_player_id')['epa'].mean()
    out['epa_rush_red_zone'] = df.loc[df['yardline_100']<=20].groupby('rusher_player_id')['epa'].mean()

    if 'defenders_in_box' in df.columns:
        out['att_light_box_rate'] = (df['defenders_in_box']<=6).groupby(df['rusher_player_id']).mean()
        out['att_heavy_box_rate'] = (df['defenders_in_box']>=7).groupby(df['rusher_player_id']).mean()
    else:
        out['att_light_box_rate'] = np.nan
        out['att_heavy_box_rate'] = np.nan

    idx = rosters[rosters['season']==season].set_index('player_id')[['player_name','team_id','team_name','conference','position']]
    out = out.join(idx, how='left')
    out['season'] = season
    out = out.reset_index().rename(columns={'rusher_player_id':'player_id'})

    if not parts.empty:
        psub = parts[['player_id','games','snaps']].drop_duplicates('player_id')
        out = out.merge(psub, on='player_id', how='left')

    cols = ['season','player_id','player_name','team_id','team_name','conference','position',
            'games','snaps','rush_att','rush_yards','rush_td','fumbles','yards_before_contact',
            'broken_tackles','forced_missed_tackles',
            'yards_per_carry','td_rate','fumble_rate',
            'epa_total_rush','epa_per_rush','success_rate','explosive_rush_rate',
            'rpo_carry_rate','read_option_rate','yards_over_expected_per_att',
            'epa_rush_early','epa_rush_short','epa_rush_red_zone',
            'att_light_box_rate','att_heavy_box_rate']
    return out.reindex(columns=cols)
