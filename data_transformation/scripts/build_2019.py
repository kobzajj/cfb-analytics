#!/usr/bin/env python
import argparse
from pathlib import Path
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
    ap.add_argument('--rawdir', type=str, default='data/raw')
    args = ap.parse_args()

    year = args.year
    raw = Path(args.rawdir)/str(year)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    rosters = load_rosters(raw/'rosters.csv')
    parts   = load_participation(raw/'participation.csv')
    pbp     = load_pbp(raw/'pbp.parquet')

    passing   = assemble_passing(pbp, rosters, parts, year)
    rushing   = assemble_rushing(pbp, rosters, parts, year)
    receiving = assemble_receiving(pbp, rosters, parts, year)
    defense   = assemble_defense(pbp, rosters, parts, year)

    # Validate (basic)
    issues = []
    issues += validation.validate_passing(passing)
    issues += validation.validate_rushing(rushing)
    issues += validation.validate_receiving(receiving)
    issues += validation.validate_defense(defense)

    passing.to_csv(outdir/f'players_passing_{year}.csv', index=False)
    rushing.to_csv(outdir/f'players_rushing_{year}.csv', index=False)
    receiving.to_csv(outdir/f'players_receiving_{year}.csv', index=False)
    defense.to_csv(outdir/f'players_defense_{year}.csv', index=False)

    print('Wrote outputs to', outdir)
    if issues:
        print('Validation issues:')
        for i in issues:
            print(' -', i)

if __name__ == '__main__':
    main()
