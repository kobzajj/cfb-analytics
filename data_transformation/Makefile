SHELL := /bin/bash

# Single-year build targets
build-2019:
	python scripts/build_2019.py --year 2019 --outdir data/processed/2019

build-2020:
	python scripts/build_2019.py --year 2020 --outdir data/processed/2020

build-2021:
	python scripts/build_2019.py --year 2021 --outdir data/processed/2021

build-2022:
	python scripts/build_2019.py --year 2022 --outdir data/processed/2022

build-2023:
	python scripts/build_2019.py --year 2023 --outdir data/processed/2023

build-2024:
	python scripts/build_2019.py --year 2024 --outdir data/processed/2024

# Range builds
build-all:
	python scripts/build_range.py --start 2019 --end 2024 --outroot data/processed

build-2020-2024:
	python scripts/build_range.py --start 2020 --end 2024 --outroot data/processed
