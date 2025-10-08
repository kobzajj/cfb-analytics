import pandas as pd, numpy as np
from ..ep_model import compute_epa, EPModelStub

def assemble_receiving(pbp, rosters, parts, season):
    df = pbp.copy()
    df = df[df.get('is_pass',0)==1].copy()
    df = df[~df['receiver_player_id'].isna()].copy()
    g = df.groupby('receiver_player_id', dropna=False)
    out = pd.DataFrame({
        'targets': g['is_pass'].sum(),
        'receptions': g['complete'].sum() if 'complete' in df.columns else 0,
        'rec_yards': g['yards_gained'].sum(),
        'rec_td': g['touchdown'].sum(),
        'drops': g['drop'].sum(min_count=1) if 'drop' in df.columns else 0,
        'fumbles': g['fumble_lost'].sum(min_count=1) if 'fumble_lost' in df.columns else 0,
        'air_yards': g['air_yards'].sum(min_count=1) if 'air_yards' in df.columns else 0,
        'yac': g['yac'].sum(min_count=1) if 'yac' in df.columns else 0,
    }).fillna(0)

    model = EPModelStub()
    df['epa'] = compute_epa(df, model)
    out = out.join(df.groupby('receiver_player_id')['epa'].sum().rename('epa_total_recv'), how='left').fillna(0)
    out['epa_per_target'] = out['epa_total_recv'].div(out['targets'].replace({0: np.nan}))

    out['catch_pct'] = out['receptions'].div(out['targets'].replace({0: np.nan}))
    out['drop_rate'] = out['drops'].div(out['targets'].replace({0: np.nan}))
    out['yds_per_target'] = out['rec_yards'].div(out['targets'].replace({0: np.nan}))
    out['tds_per_target'] = out['rec_td'].div(out['targets'].replace({0: np.nan}))
    out['adot'] = out['air_yards'].div(out['targets'].replace({0: np.nan})) if 'air_yards' in out.columns else np.nan
    out['yac_per_rec'] = out['yac'].div(out['receptions'].replace({0: np.nan})) if 'yac' in out.columns else np.nan

    routes = parts.set_index('player_id')['routes'] if 'routes' in parts.columns else None
    snaps = parts.set_index('player_id')['snaps'] if 'snaps' in parts.columns else None
    out['routes'] = out.index.map(routes) if routes is not None else np.nan
    out['snaps'] = out.index.map(snaps) if snaps is not None else np.nan
    out['tgt_per_route'] = out['targets'].div(out['routes'].replace({0: np.nan}))
    out['yards_per_route_run'] = out['rec_yards'].div(out['routes'].replace({0: np.nan}))

    out['success_rate'] = (df['epa']>0).groupby(df['receiver_player_id']).mean()
    out['explosive_rec_rate'] = (df['yards_gained']>=20).groupby(df['receiver_player_id']).mean()

    if 'air_yards' in df.columns:
        deep = df['air_yards']>=20
        out['epa_per_target_deep'] = df.loc[deep].groupby('receiver_player_id')['epa'].mean()
        out['epa_per_target_short'] = df.loc[~deep].groupby('receiver_player_id')['epa'].mean()
    else:
        out['epa_per_target_deep'] = np.nan
        out['epa_per_target_short'] = np.nan

    for col, outcol in [('slot_aligned','slot_rate'), ('wide_aligned','wide_rate'), ('inline_aligned','inline_te_rate'),
                        ('vs_man','man_tgt_rate'), ('vs_zone','zone_tgt_rate')]:
        if col in df.columns:
            out[outcol] = df[col].groupby(df['receiver_player_id']).mean()

    if 'vs_man' in df.columns:
        out['epa_vs_man'] = df.loc[df['vs_man']==1].groupby('receiver_player_id')['epa'].mean()
    if 'vs_zone' in df.columns:
        out['epa_vs_zone'] = df.loc[df['vs_zone']==1].groupby('receiver_player_id')['epa'].mean()

    if 'separation' in df.columns:
        out['separation_avg_yards'] = df.groupby('receiver_player_id')['separation'].mean()

    idx = rosters[rosters['season']==season].set_index('player_id')[['player_name','team_id','team_name','conference','position']]
    out = out.join(idx, how='left')
    out['season'] = season
    out = out.reset_index().rename(columns={'receiver_player_id':'player_id'})

    if not parts.empty:
        psub = parts[['player_id','games']].drop_duplicates('player_id')
        out = out.merge(psub, on='player_id', how='left')

    out['targets_per_game'] = out['targets'].div(out['games'].replace({0: np.nan}))
    out['air_yards_share'] = np.nan  # requires team air yards table
    out['target_share'] = np.nan     # requires team pass attempts

    cols = ['season','player_id','player_name','team_id','team_name','conference','position',
            'games','snaps','routes','targets','receptions','rec_yards','rec_td','drops','fumbles','air_yards','yac',
            'tgt_per_route','adot','yards_per_route_run','targets_per_game','catch_pct','drop_rate','yds_per_target','tds_per_target','yac_per_rec',
            'air_yards_share','target_share',
            'epa_total_recv','epa_per_target','success_rate','explosive_rec_rate',
            'slot_rate','wide_rate','inline_te_rate','man_tgt_rate','zone_tgt_rate','epa_vs_man','epa_vs_zone','separation_avg_yards',
            'epa_per_target_deep','epa_per_target_short']
    return out.reindex(columns=cols)
