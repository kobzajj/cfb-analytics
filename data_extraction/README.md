

## Phase 1: Pulling from CFBD

Export your key:
```bash
export CFBD_API_KEY=your_key_here
```

Pull rosters + raw PBP:
```bash
python scripts/pull_cfbd.py --years 2019 2020 2021 2022 2023 2024
```

Map CFBD PBP to the ETL standard:
```bash
python scripts/map_cfbd_pbp.py --year 2019
# repeat per season
```

Then build:
```bash
python scripts/build_2019.py --year 2019 --outdir data/processed/2019
```
