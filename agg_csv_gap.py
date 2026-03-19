#!/usr/bin/env python3
"""
Aggregate county-level commercial gap model CSVs into state-level CSV files.

Reads the downloaded gap model CSVs (one per county) and sums the energy
consumption values by state and timestamp, producing one output CSV per state.

Input structure (from download_buildstock.py --gap-model):
    downloads/2025/comstock_amy2018_release_3_gap_model/
        upgrade=0/
            county=G0100010/up0-G0100010-gap.csv
            county=G0100030/up0-G0100030-gap.csv
            ...

Each county CSV has columns:
    timestamp, out.electricity.total.energy_consumption..kwh, upgrade, in.county

Output: one CSV per state with columns:
    timestamp, out.electricity.total.energy_consumption..kwh, state

Usage:
    python agg_csv_gap.py
    python agg_csv_gap.py --input-dir downloads/2025/comstock_amy2018_release_3_gap_model
    python agg_csv_gap.py --output-dir gap_by_state
    python agg_csv_gap.py --states CA TX NY
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

# FIPS state code -> two-letter abbreviation
FIPS_TO_STATE = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "72": "PR", "78": "VI",
}

ENERGY_COL = "out.electricity.total.energy_consumption..kwh"

DEFAULT_INPUT_DIR = "downloads/2025/comstock_amy2018_release_3_gap_model"
DEFAULT_OUTPUT_FILE = "gap_by_state.csv"


def county_to_state_abbr(county_code: str) -> str | None:
    """Extract state abbreviation from a GISJOIN county code like G0100010."""
    if len(county_code) >= 3 and county_code.startswith("G"):
        return FIPS_TO_STATE.get(county_code[1:3])
    return None


def find_gap_csvs(input_dir: Path) -> list[Path]:
    """Find all gap model CSV files under the input directory."""
    csvs = sorted(input_dir.rglob("*-gap.csv"))
    return csvs


def aggregate_by_state(
    csv_paths: list[Path],
    states_filter: set[str] | None = None,
) -> dict[str, dict[str, float]]:
    """
    Read county CSVs and sum energy consumption by state and timestamp.

    Returns: {state_abbr: {timestamp: total_kwh}}
    """
    # state -> timestamp -> accumulated kwh
    state_data: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    skipped = 0

    for i, csv_path in enumerate(csv_paths, 1):
        # Extract county code from the path (parent dir is county=GXXXXXXX)
        county_dir = csv_path.parent.name  # e.g. "county=G0100010"
        if "=" not in county_dir:
            skipped += 1
            continue
        county_code = county_dir.split("=", 1)[1]
        state_abbr = county_to_state_abbr(county_code)
        if state_abbr is None:
            print(f"  Warning: unknown county code {county_code}, skipping {csv_path.name}")
            skipped += 1
            continue

        if states_filter and state_abbr not in states_filter:
            continue

        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts = row["timestamp"]
                kwh = float(row[ENERGY_COL])
                state_data[state_abbr][ts] += kwh

        if i % 200 == 0 or i == len(csv_paths):
            print(f"  Processed {i:,}/{len(csv_paths):,} county files…", end="\r")

    print()  # newline after progress
    if skipped:
        print(f"  Skipped {skipped} files (unrecognized county codes)")

    return dict(state_data)


def write_combined_csv(
    state_data: dict[str, dict[str, float]],
    output_path: Path,
) -> int:
    """Write all states into a single long-format CSV, sorted by state then timestamp."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", ENERGY_COL, "State"])
        for state_abbr in sorted(state_data):
            for ts in sorted(state_data[state_abbr]):
                writer.writerow([ts, state_data[state_abbr][ts], state_abbr])
                total_rows += 1

    return total_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input-dir",
        default=DEFAULT_INPUT_DIR,
        help=f"Directory containing downloaded gap model CSVs (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "--output-file",
        default=DEFAULT_OUTPUT_FILE,
        help=f"Path for the output CSV file (default: {DEFAULT_OUTPUT_FILE})",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        type=str,
        metavar="XX",
        help="Only aggregate these states (e.g. --states CA TX NY). Defaults to all.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()
    output_path = Path(args.output_file).expanduser().resolve()
    states_filter = {s.upper() for s in args.states} if args.states else None

    if not input_dir.exists():
        print(f"Error: input directory does not exist: {input_dir}")
        print("Run download_buildstock.py --gap-model first to download the data.")
        return 1

    print(f"Scanning for gap model CSVs in: {input_dir}")
    csv_paths = find_gap_csvs(input_dir)
    print(f"Found {len(csv_paths):,} county CSV files\n")

    if not csv_paths:
        print("No gap CSV files found. Nothing to do.")
        return 0

    print("Aggregating by state…")
    state_data = aggregate_by_state(csv_paths, states_filter)

    print(f"\nWriting {len(state_data)} states to: {output_path}")
    total_rows = write_combined_csv(state_data, output_path)
    for state_abbr in sorted(state_data):
        print(f"  {state_abbr}: {len(state_data[state_abbr]):,} rows")

    print(f"\nDone. {total_rows:,} total rows ({len(state_data)} states) written to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
