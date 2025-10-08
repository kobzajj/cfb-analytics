import pandas as pd, numpy as np
from ..ep_model import compute_epa, EPModelStub

def assemble_defense(pbp, rosters, parts, season):
    df = pbp.copy()
    model = EPModelStub()
    df['epa'] = compute_epa(df, model)
    out = pd.DataFrame()

    if 'primary_defender_id' in df.columns and 'is_pass' in df.columns:
        cover = df[(df['is_pass']==1) & (~df['primary_defender_id'].isna())].copy()
        g = cover.groupby('primary_defender_id')
        out = pd.DataFrame({
            'targets': g.size(),
            'receptions_allowed': g['complete'].sum() if 'complete' in cover.columns else 0,
            'yards_allowed': g['yards_gained'].sum(),
            'td_allowed': g['touchdown'].sum(),
        }).fillna(0)
        out['coverage_success_rate_allowed'] = (cover['epa']<=0).groupby(cover['primary_defender_id']).mean()
        out['explosive_allowed_rate'] = (cover['yards_gained']>=20).groupby(cover['primary_defender_id']).mean()

    if 'pressure' in df.columns and 'pass_rusher_id' in df.columns:
        pr = df[df['pressure']==1].groupby('pass_rusher_id').size().rename('pressures')
        out = pr.to_frame().join(out, how='outer') if not out.empty else pr.to_frame()
    if 'sack' in df.columns and 'sacker_id' in df.columns:
        sk = df[df['sack']==1].groupby('sacker_id').size().rename('sacks')
        out = out.join(sk, how='outer') if not out.empty else sk.to_frame()

    for src, dst in [('tackle_primary','solo_tackles'),('tackle_assist','assists'),('missed_tackle','missed_tackles'),
                     ('tfl','tackles_for_loss'),('stop','stops'),('forced_fumble','forced_fumbles'),
                     ('fumble_recovery','fumble_recoveries'),('defensive_td','defensive_tds')]:
        if src in df.columns and 'defender_id' in df.columns:
            s = df.groupby('defender_id')[src].sum().rename(dst)
            out = out.join(s, how='outer') if not out.empty else s.to_frame()

    if out.empty:
        out = pd.DataFrame(index=pd.Index([], name='player_id'))

    if {'solo_tackles','assists'}.issubset(out.columns):
        out['total_tackles'] = out['solo_tackles'].fillna(0) + out['assists'].fillna(0)
    if {'targets','receptions_allowed'}.issubset(out.columns):
        out['completion_pct_allowed'] = out['receptions_allowed']/out['targets'].replace({0: np.nan})
        out['yards_per_target_allowed'] = out.get('yards_allowed', np.nan)/out['targets'].replace({0: np.nan})
    out['passer_rating_allowed'] = np.nan
    out['pressure_rate'] = np.nan
    out['win_rate'] = np.nan
    out['missed_tackle_rate'] = out['missed_tackles'] / (out['solo_tackles']+out['assists']+out['missed_tackles']).replace({0: np.nan}) if {'missed_tackles','solo_tackles','assists'}.issubset(out.columns) else np.nan
    out['stop_rate'] = np.nan
    out['def_epa_saved_total'] = np.nan
    out['def_epa_saved_per_snap'] = np.nan

    idx = rosters[rosters['season']==season].set_index('player_id')[['player_name','team_id','team_name','conference','position_group']]
    out = out.join(idx, how='left')
    out['season'] = season
    out = out.reset_index().rename(columns={'index':'player_id'})

    if not parts.empty:
        cols = [c for c in ['player_id','games','starts','def_snaps'] if c in parts.columns]
        out = out.merge(parts[cols].drop_duplicates('player_id'), on='player_id', how='left')

    expected = ['season','player_id','player_name','team_id','team_name','conference','position_group',
                'games','starts','def_snaps',
                'total_tackles','solo_tackles','assists','missed_tackles',
                'pressures','sacks',
                'targets','receptions_allowed','yards_allowed','td_allowed','interceptions','pass_breakups',
                'stops','tackles_for_loss','forced_fumbles','fumble_recoveries','defensive_tds',
                'pressure_rate','win_rate','completion_pct_allowed','yards_per_target_allowed','passer_rating_allowed',
                'missed_tackle_rate','stop_rate',
                'def_epa_saved_total','def_epa_saved_per_snap','coverage_success_rate_allowed','explosive_allowed_rate']
    for c in expected:
        if c not in out.columns:
            out[c] = np.nan
    return out[expected]
