#!/usr/bin/env python3
"""
Combine downloaded ComStock CSV files into a single Tableau .hyper file.

For each CSV the script:
  1. Reads all rows, keeping non-"out." columns plus "out.electricity.*" columns.
  2. Melts (unpivots) the "out.electricity.*" columns into two new columns:
       • end_use  - the human-readable end-use name (e.g. "cooling", "fans")
       • kWh      - the value
  3. Drops every other "out.*" column (district_cooling, natural_gas, etc.).

The resulting long-format dataframe is written to a Tableau Hyper file.

Usage examples:
    # Process CSVs for a specific ComStock release
    python build_hyper.py --input-dir ./downloads/2025/comstock_amy2018_release_3

    # Process CSVs for a specific ResStock release
    python build_hyper.py --input-dir ./downloads/2025/resstock_amy2018_release_1

    # Custom output file name
    python build_hyper.py --input-dir ./downloads/2025/comstock_amy2018_release_3 --output comstock.hyper

    # Only process files matching a glob (e.g. one state)
    python build_hyper.py --input-dir ./downloads/2025/comstock_amy2018_release_3 --glob "upgrade=0/state=CA/*.csv"

    # Use chunked processing to limit memory usage
    python build_hyper.py --input-dir ./downloads/2025/comstock_amy2018_release_3 --chunk-size 50
    python build_hyper.py --input-dir ./downloads/2025/resstock_amy2018_release_1 --chunk-size 50
"""

import argparse
import glob as globmod
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd
from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    Inserter,
    SqlType,
    TableDefinition,
    TableName,
    Telemetry,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_end_use(col_name: str) -> str:
    """
    Turn 'out.electricity.cooling.energy_consumption.kwh' → 'cooling'.

    Pattern: out.electricity.<end_use>.energy_consumption.kwh
    If the pattern doesn't match perfectly we fall back to everything between
    the second and last dot-separated tokens.
    """
    parts = col_name.split(".")
    # Expected: ['out', 'electricity', '<end_use>', 'energy_consumption', 'kwh']
    if len(parts) >= 5:
        return parts[2]
    # Fallback: join the middle parts
    return ".".join(parts[2:-1]) if len(parts) > 3 else col_name


def discover_csv_files(input_dir: Path, pattern: str) -> list[Path]:
    """Return a sorted list of CSV paths matching *pattern* under *input_dir*."""
    full_pattern = str(input_dir / pattern)
    files = sorted(Path(p) for p in globmod.glob(full_pattern, recursive=True))
    return files


# ---------------------------------------------------------------------------
# Progress display
# ---------------------------------------------------------------------------

def _fmt_time(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


class Progress:
    """Single-line updating progress counter."""

    def __init__(self, total_files: int) -> None:
        self.total_files = total_files
        self.files_done = 0
        self.total_rows = 0
        self._start = time.monotonic()

    def update(self, file_path: Path, input_dir: Path, rows: int) -> None:
        self.files_done += 1
        self.total_rows += rows
        elapsed = time.monotonic() - self._start
        pct = 100 * self.files_done / self.total_files if self.total_files else 0
        rate = self.files_done / elapsed if elapsed > 0 else 0
        rel = file_path.relative_to(input_dir)
        line = (
            f"  {self.files_done:>6}/{self.total_files}  "
            f"({pct:5.1f}%)  "
            f"rows={self.total_rows:>12,}  "
            f"@ {rate:5.1f} files/s  "
            f"{_fmt_time(elapsed)}  "
            f"{rel}"
        )
        # Truncate to terminal width so the line never wraps
        try:
            cols = os.get_terminal_size().columns
        except OSError:
            cols = 80
        line = line[:cols - 1]
        # \r returns to column 0; \033[K erases to end of line
        sys.stdout.write(f"\r\033[K{line}")
        sys.stdout.flush()

    def finish(self) -> None:
        elapsed = time.monotonic() - self._start
        print(
            f"\n  Processed {self.files_done:,} files  "
            f"({self.total_rows:,} rows)  "
            f"in {_fmt_time(elapsed)}"
        )


def process_csv(path: Path) -> pd.DataFrame:
    """
    Read one CSV, melt electricity end-use columns, drop other out.* columns.

    Returns a long-format DataFrame.
    """
    df = pd.read_csv(path)

    # Identify column groups
    elec_cols = [c for c in df.columns if c.startswith("out.electricity.")]
    other_out_cols = [
        c for c in df.columns
        if c.startswith("out.") and not c.startswith("out.electricity.")
    ]
    id_cols = [c for c in df.columns if c not in elec_cols and c not in other_out_cols]

    # Drop non-electricity output columns
    df = df.drop(columns=other_out_cols)

    # Melt electricity columns into long format
    df_long = df.melt(
        id_vars=id_cols,
        value_vars=elec_cols,
        var_name="end_use",
        value_name="kWh",
    )

    # Clean up end_use labels
    df_long["end_use"] = df_long["end_use"].map(_extract_end_use)

    return df_long


# ---------------------------------------------------------------------------
# Hyper export
# ---------------------------------------------------------------------------

# Map pandas dtypes → Hyper SqlTypes
_DTYPE_MAP = {
    "int64": SqlType.big_int,
    "int32": SqlType.int,
    "float64": SqlType.double,
    "float32": SqlType.double,
    "object": SqlType.text,
    "bool": SqlType.bool,
    "datetime64[ns]": SqlType.timestamp,
}


def _sql_type_for(dtype) -> SqlType:
    """Return an appropriate Hyper SqlType for a pandas dtype."""
    name = str(dtype)
    if name in _DTYPE_MAP:
        return _DTYPE_MAP[name]()
    if name.startswith("datetime"):
        return SqlType.timestamp()
    return SqlType.text()


def _build_table_definition(df: pd.DataFrame, table_name: str = "Extract") -> TableDefinition:
    """Create a Hyper TableDefinition from a DataFrame's columns & dtypes."""
    columns = []
    for col_name in df.columns:
        sql_type = _sql_type_for(df[col_name].dtype)
        columns.append(TableDefinition.Column(col_name, sql_type))
    return TableDefinition(TableName("Extract", table_name), columns)


def write_chunk_hyper(df: pd.DataFrame, hyper_path: Path, create_mode: CreateMode) -> None:
    """
    Write *df* to a Tableau .hyper file at *hyper_path*.

    Uses CREATE_AND_REPLACE for new files or CREATE_IF_NOT_EXISTS for appends.
    A single HyperProcess is started per call to keep chunk writes independent.
    """
    hyper_path.parent.mkdir(parents=True, exist_ok=True)

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database=str(hyper_path),
            create_mode=create_mode,
        ) as connection:
            table_def = _build_table_definition(df)
            if create_mode == CreateMode.CREATE_AND_REPLACE:
                connection.catalog.create_schema_if_not_exists("Extract")
                connection.catalog.create_table(table_def)

            # Convert DataFrame rows to tuples (replacing NaN → None)
            df_clean = df.where(pd.notnull(df), None)

            with Inserter(connection, table_def) as inserter:
                inserter.add_rows(df_clean.itertuples(index=False, name=None))
                inserter.execute()


def merge_hyper_files(chunk_paths: list[Path], output_path: Path) -> int:
    """
    Merge multiple .hyper chunk files into a single *output_path* using
    Hyper SQL ``INSERT INTO … SELECT FROM``.

    Returns the total row count in the final file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Use the first chunk as the base for the output file
    shutil.copy2(chunk_paths[0], output_path)

    if len(chunk_paths) == 1:
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(hyper.endpoint, str(output_path), CreateMode.NONE) as conn:
                row_count = conn.execute_scalar_query(
                    'SELECT COUNT(*) FROM "Extract"."Extract"'
                )
        return int(row_count)

    table = TableName("Extract", "Extract")

    # When databases are attached, even the target table needs a fully-qualified
    # 3-part name: "database"."schema"."table".  The main database name is the
    # filename stem of the connected .hyper file.
    main_db = output_path.stem
    target_table = f'"{main_db}"."Extract"."Extract"'

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database=str(output_path),
            create_mode=CreateMode.NONE,
        ) as connection:
            for i, chunk_path in enumerate(chunk_paths[1:], 2):
                db_alias = f"chunk_{i}"
                connection.catalog.attach_database(str(chunk_path), db_alias)
                connection.execute_command(
                    f'INSERT INTO {target_table} '
                    f'SELECT * FROM "{db_alias}"."Extract"."Extract"'
                )
                connection.catalog.detach_database(db_alias)
                sys.stdout.write(
                    f"\r  Merged chunk {i}/{len(chunk_paths)} into {output_path.name}"
                )
                sys.stdout.flush()

            row_count = connection.execute_scalar_query(
                f"SELECT COUNT(*) FROM {table}"
            )

    print()  # newline after progress
    return int(row_count)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input-dir",
        default="downloads",
        help="Root directory containing downloaded CSV files (default: ./downloads)",
    )
    parser.add_argument(
        "--glob",
        default="**/*.csv",
        help='Glob pattern relative to --input-dir (default: "**/*.csv")',
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output .hyper file path "
             "(default: <input-dir folder name>.hyper, e.g. comstock_amy2018_release_3.hyper)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=0,
        metavar="N",
        help="If set, process N CSV files at a time and write each chunk to a "
             "separate temp .hyper file, then merge at the end. "
             "0 = process all at once (default).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    input_dir = Path(args.input_dir).expanduser().resolve()
    output_name = args.output if args.output else f"{input_dir.name}.hyper"
    output_path = Path(output_name).expanduser().resolve()

    if not input_dir.is_dir():
        print(f"Error: input directory does not exist: {input_dir}", file=sys.stderr)
        return 1

    # ----- discover files -----
    csv_files = discover_csv_files(input_dir, args.glob)
    if not csv_files:
        print(f"No CSV files found matching '{args.glob}' under {input_dir}")
        return 1

    print(f"Found {len(csv_files):,} CSV file(s) under {input_dir}")

    # ----- process -----
    t0 = time.monotonic()
    chunk_size = args.chunk_size if args.chunk_size > 0 else len(csv_files)
    chunks = [csv_files[i : i + chunk_size] for i in range(0, len(csv_files), chunk_size)]
    progress = Progress(len(csv_files))
    printed_columns = False

    # Temp directory for per-chunk .hyper files
    tmp_dir = Path(tempfile.mkdtemp(prefix="buildstock_hyper_"))
    chunk_paths: list[Path] = []

    try:
        for chunk_idx, chunk_files in enumerate(chunks, 1):
            frames: list[pd.DataFrame] = []
            for csv_path in chunk_files:
                df_part = process_csv(csv_path)
                progress.update(csv_path, input_dir, len(df_part))
                frames.append(df_part)

            df = pd.concat(frames, ignore_index=True)
            del frames  # free memory

            if not printed_columns:
                progress.finish()
                print(f"  Columns: {list(df.columns)}")
                printed_columns = True

            # Write this chunk to its own temp .hyper file
            chunk_hyper = tmp_dir / f"chunk_{chunk_idx:04d}.hyper"
            write_chunk_hyper(df, chunk_hyper, CreateMode.CREATE_AND_REPLACE)
            chunk_paths.append(chunk_hyper)
            print(f"  Wrote chunk {chunk_idx}/{len(chunks)} → {chunk_hyper.name}  "
                  f"({len(df):,} rows)")
            del df

        # ----- merge -----
        t_merge = time.monotonic()
        print(f"\nMerging {len(chunk_paths)} chunk(s) into {output_path.name} …")
        total_rows = merge_hyper_files(chunk_paths, output_path)
        merge_elapsed = time.monotonic() - t_merge
        print(f"  Merge complete: {total_rows:,} rows in {_fmt_time(merge_elapsed)}")

    finally:
        # Clean up temp chunk files
        shutil.rmtree(tmp_dir, ignore_errors=True)

    elapsed = time.monotonic() - t0
    print(f"\nDone in {_fmt_time(elapsed)}  →  {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
