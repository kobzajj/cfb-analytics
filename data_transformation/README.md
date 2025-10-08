# CFB Player Pipelines (Full Scaffold)

Reproducible pipeline to build **player-season** CSVs for FBS (starting with 2019):
- `players_passing_YYYY.csv`
- `players_rushing_YYYY.csv`
- `players_receiving_YYYY.csv`
- `players_defense_YYYY.csv`

Includes:
- A stub **Expected Points (EP)** model + **EPA** helper
- ETL modules per file type
- Simple validators
- CLI to build a season

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Put raw data under data/raw/2019/: rosters.csv, participation.csv, pbp.parquet
python scripts/build_2019.py --year 2019 --outdir data/processed/2019
```


## Multi-year builds
To build multiple seasons at once (2019–2024):
```bash
python scripts/build_range.py --start 2019 --end 2024 --outroot data/processed
# or with Make:
make build-all
```

Build a single year (e.g., 2022):
```bash
python scripts/build_2019.py --year 2022 --outdir data/processed/2022
# or with Make:
make build-2022
```
