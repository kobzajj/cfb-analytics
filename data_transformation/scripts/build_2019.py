#!/usr/bin/env python
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse
from cfb_analytics.etl.common import load_rosters, load_participation, load_pbp
from cfb_analytics.etl.passing import assemble_passing
from cfb_analytics.etl.rushing import assemble_rushing
from cfb_analytics.etl.receiving import assemble_receiving
from cfb_analytics.etl.defense import assemble_defense
from cfb_analytics import validation

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--year', type=int, required=True)
    ap.add_argument('--outdir', type=str, required=True)
    ap.add_argument('--rawdir', type=str, default='../data_extraction/data/raw')
    args = ap.parse_args()

    year = args.year
    raw = Path(args.rawdir)/str(year)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    rosters = load_rosters(raw/'rosters.csv')
    print('Rosters loaded successfully')
    # parts   = load_participation(raw/'participation.csv') NEED TO ADD PARTICIPATION DATA
    pbp     = load_pbp(raw/'pbp.parquet')
    print('Play by play loaded successfully')

    passing   = assemble_passing(pbp, rosters, [], year)
    rushing   = assemble_rushing(pbp, rosters, [], year)
    receiving = assemble_receiving(pbp, rosters, [], year)
    # defense to be added later - need additional data points
    # defense   = assemble_defense(pbp, rosters, parts, year)

    print('Passing Stats Overview-------------------------')
    print('Number of players in passing dataset: ', len(passing))
    print('Max passing yards: ', passing.loc[passing['pass_yards'].idxmax(), 'player_name'], ' ', passing['pass_yards'].max())
    print('Max passing tds: ', passing.loc[passing['pass_td'].idxmax(), 'player_name'], ' ', passing['pass_td'].max())
    print('Max completion percentage: ', passing.loc[passing['completion_pct'].idxmax(), 'player_name'], ' ', passing['completion_pct'].max())
    print('Max PPA per dropback: ', passing.loc[passing['ppa_per_dropback'].idxmax(), 'player_name'], ' ', passing['ppa_per_dropback'].max())
    print('Max success rate: ', passing.loc[passing['success_rate'].idxmax(), 'player_name'], ' ', passing['success_rate'].max())

    print('Rushing Stats Overview-------------------------')
    print('Number of players in rushing dataset: ', len(rushing))
    print('Max rushing yards: ', rushing.loc[rushing['rush_yards'].idxmax(), 'player_name'], ' ', rushing['rush_yards'].max())
    print('Max rushing tds: ', rushing.loc[rushing['rush_td'].idxmax(), 'player_name'], ' ', rushing['rush_td'].max())
    print('Max yards per carry: ', rushing.loc[rushing['yards_per_carry'].idxmax(), 'player_name'], ' ', rushing['yards_per_carry'].max())
    print('Max PPA per rush: ', rushing.loc[rushing['ppa_per_rush'].idxmax(), 'player_name'], ' ', rushing['ppa_per_rush'].max())

    print('Receiving Stats Overview-------------------------')
    print('Number of players in receiving dataset: ', len(receiving))
    print('Max receiving yards: ', receiving.loc[receiving['rec_yards'].idxmax(), 'player_name'], ' ',
          receiving['rec_yards'].max())
    print('Max receiving tds: ', receiving.loc[receiving['rec_td'].idxmax(), 'player_name'], ' ', receiving['rec_td'].max())
    print('Max yards per reception: ', receiving.loc[receiving['yds_per_reception'].idxmax(), 'player_name'], ' ',
          receiving['yds_per_reception'].max())
    print('Max catch rate: ', receiving.loc[receiving['catch_pct'].idxmax(), 'player_name'], ' ', receiving['catch_pct'].max())
    print('Max PPA per target: ', receiving.loc[receiving['ppa_per_target'].idxmax(), 'player_name'], ' ',
          receiving['ppa_per_target'].max())

    # Validate (basic)
    issues = []
    issues += validation.validate_passing(passing)
    issues += validation.validate_rushing(rushing)
    issues += validation.validate_receiving(receiving)
    # issues += validation.validate_defense(defense)

    passing.to_csv(outdir/f'players_passing_{year}.csv', index=False)
    rushing.to_csv(outdir/f'players_rushing_{year}.csv', index=False)
    receiving.to_csv(outdir/f'players_receiving_{year}.csv', index=False)
    # defense.to_csv(outdir/f'players_defense_{year}.csv', index=False)

    print('Wrote outputs to', outdir)
    if issues:
        print('Validation issues:')
        for i in issues:
            print(' -', i)

if __name__ == '__main__':
    main()
