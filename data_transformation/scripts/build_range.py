#!/usr/bin/env python
from __future__ import annotations
import argparse
from pathlib import Path
from subprocess import run, CalledProcessError

def run_year(year: int, outdir: Path, rawdir: Path):
    cmd = [
        "python", "scripts/build_2019.py",
        "--year", str(year),
        "--outdir", str(outdir / str(year)),
        "--rawdir", str(rawdir)
    ]
    outdir.joinpath(str(year)).mkdir(parents=True, exist_ok=True)
    print("\n=== Building", year, "===")
    rc = run(cmd)
    if rc.returncode != 0:
        raise CalledProcessError(rc.returncode, cmd)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=2019)
    ap.add_argument("--end", type=int, default=2024)  # inclusive
    ap.add_argument("--rawdir", type=str, default="data/raw")
    ap.add_argument("--outroot", type=str, default="data/processed")
    args = ap.parse_args()

    rawdir = Path(args.rawdir)
    outroot = Path(args.outroot)

    for yr in range(args.start, args.end + 1):
        run_year(yr, outroot, rawdir)

if __name__ == "__main__":
    main()
