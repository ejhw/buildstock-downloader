#!/usr/bin/env python3
"""
Convert all Parquet files in a folder into a single Tableau .hyper file.

The script:
  1. Discovers every *.parquet file in the specified input folder.
  2. Converts each one to a temporary .hyper file using Hyper's COPY FROM
     PARQUET (schema is auto-detected from the Parquet metadata).
  3. Unions all the temporary .hyper files into a single output .hyper file.

Usage examples:
    # Convert all parquet files in a folder
    python parquet_to_hyper.py --input-dir ./annual/resstock_amy2018_release_1\\ annual

    # Specify a custom output file name
    python parquet_to_hyper.py --input-dir ./annual/comstock_amy2018_release_2\\ annual --output comstock_annual.hyper

    # Custom table name in the output Hyper file
    python parquet_to_hyper.py --input-dir ./annual/resstock_amy2018_release_1\\ annual --table-name annual_data

    # Convert ResStock annual parquet files
    python parquet_to_hyper.py --input-dir "./annual/resstock_amy2018_release_1 annual"

    # Convert ComStock annual parquet files with a custom output name
    python parquet_to_hyper.py --input-dir "./annual/comstock_amy2018_release_2 annual" --output comstock_annual.hyper
"""

import argparse
import os
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa
from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    Inserter,
    SqlType,
    TableDefinition,
    Telemetry,
    HyperException,
    TableName,
)


# ---------------------------------------------------------------------------
# Helpers
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


def discover_parquet_files(input_dir: Path) -> list[Path]:
    """Return a sorted list of .parquet files in *input_dir*."""
    files = sorted(input_dir.glob("*.parquet"))
    return files


_WRITE_BATCH_SIZE = 500_000


def _pa_type_to_sql(pa_type: pa.DataType) -> SqlType:
    """Map a PyArrow type to a Hyper SqlType."""
    if pa.types.is_int8(pa_type) or pa.types.is_int16(pa_type) or pa.types.is_int32(pa_type):
        return SqlType.int()
    elif pa.types.is_int64(pa_type):
        return SqlType.big_int()
    elif pa.types.is_float32(pa_type) or pa.types.is_float64(pa_type):
        return SqlType.double()
    elif pa.types.is_boolean(pa_type):
        return SqlType.bool()
    elif pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
        return SqlType.text()
    elif pa.types.is_date(pa_type):
        return SqlType.date()
    elif pa.types.is_timestamp(pa_type):
        return SqlType.timestamp()
    else:
        return SqlType.text()


def _build_table_def(schema: pa.Schema, table_name: str) -> TableDefinition:
    """Create a Hyper TableDefinition from a PyArrow schema."""
    columns = [
        TableDefinition.Column(field.name, _pa_type_to_sql(field.type))
        for field in schema
    ]
    return TableDefinition(TableName("public", table_name), columns)


def parquet_to_hyper(
    parquet_path: Path,
    hyper_path: Path,
    table_name: str,
) -> int:
    """
    Load a single Parquet file into a new .hyper file.

    Reads via PyArrow so column types (including float32) are properly mapped
    to Hyper SqlTypes.  Float32 values are cast to float64 since Hyper does
    not support 32-bit floats.

    Returns the number of rows inserted.
    """
    hyper_path.parent.mkdir(parents=True, exist_ok=True)

    table = pq.read_table(parquet_path)

    # Cast float32 columns → float64 (Hyper doesn't support float32)
    new_fields = []
    for field in table.schema:
        if pa.types.is_float32(field.type):
            new_fields.append(field.with_type(pa.float64()))
        else:
            new_fields.append(field)
    new_schema = pa.schema(new_fields)
    table = table.cast(new_schema)

    table_def = _build_table_def(table.schema, table_name)
    total_rows = table.num_rows

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database=str(hyper_path),
            create_mode=CreateMode.CREATE_AND_REPLACE,
        ) as connection:
            connection.catalog.create_table(table_def)

            # Convert to Python rows and write in batches
            columns = table.to_pydict()
            col_names = table.schema.names
            with Inserter(connection, table_def) as inserter:
                for start in range(0, total_rows, _WRITE_BATCH_SIZE):
                    end = min(start + _WRITE_BATCH_SIZE, total_rows)
                    batch = [
                        tuple(columns[c][i] for c in col_names)
                        for i in range(start, end)
                    ]
                    inserter.add_rows(batch)
                inserter.execute()

    return total_rows


def union_hyper_files(
    hyper_files: list[Path],
    output_path: Path,
    table_name: str,
) -> int:
    """
    Union all .hyper files into a single *output_path* using Hyper SQL.

    Returns the total row count.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    if len(hyper_files) == 1:
        # Nothing to union — just rename / copy the single file
        import shutil
        shutil.copy2(hyper_files[0], output_path)
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(hyper.endpoint, str(output_path), CreateMode.NONE) as conn:
                row_count = conn.execute_scalar_query(
                    f'SELECT COUNT(*) FROM "public"."{table_name}"'
                )
        return int(row_count)

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint) as conn:
            # Attach all input databases
            for idx, hf in enumerate(hyper_files):
                conn.catalog.attach_database(str(hf), alias=f"input{idx}")

            # Create and attach the output database
            conn.catalog.create_database(str(output_path))
            conn.catalog.attach_database(str(output_path), alias="output")

            # Build UNION ALL query across all inputs
            union_query = " UNION ALL\n".join(
                f'SELECT * FROM "input{idx}"."public"."{table_name}"'
                for idx in range(len(hyper_files))
            )
            create_sql = (
                f'CREATE TABLE "output"."public"."{table_name}" AS\n'
                f"{union_query}"
            )
            conn.execute_command(create_sql)

            row_count = conn.execute_scalar_query(
                f'SELECT COUNT(*) FROM "output"."public"."{table_name}"'
            )

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
        required=True,
        help="Folder containing .parquet files to convert",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output .hyper file path "
             "(default: <input-dir folder name>.hyper)",
    )
    parser.add_argument(
        "--table-name",
        default="Extract",
        help='Table name inside the Hyper file (default: "Extract")',
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    input_dir = Path(args.input_dir).expanduser().resolve()
    if not input_dir.is_dir():
        print(f"Error: input directory does not exist: {input_dir}", file=sys.stderr)
        return 1

    # Default output: <folder_name>.hyper in the current working directory
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
    else:
        output_path = Path(f"{input_dir.name}.hyper").resolve()

    table_name = args.table_name

    # Discover parquet files
    parquet_files = discover_parquet_files(input_dir)
    if not parquet_files:
        print(f"No .parquet files found in {input_dir}")
        return 1

    print(f"Found {len(parquet_files)} parquet file(s) in {input_dir}")

    # Step 1: Convert each parquet → temp .hyper
    t0 = time.monotonic()
    temp_hypers: list[Path] = []

    for i, pf in enumerate(parquet_files, 1):
        hyper_path = input_dir / f".tmp_{pf.stem}.hyper"
        print(f"  [{i}/{len(parquet_files)}] {pf.name} → {hyper_path.name} ...", end=" ", flush=True)
        t1 = time.monotonic()
        rows = parquet_to_hyper(pf, hyper_path, table_name)
        elapsed = time.monotonic() - t1
        print(f"{rows:,} rows in {_fmt_time(elapsed)}")
        temp_hypers.append(hyper_path)

    # Step 2: Union into final .hyper
    print(f"\nUnioning {len(temp_hypers)} file(s) into {output_path.name} ...")
    t_union = time.monotonic()
    total_rows = union_hyper_files(temp_hypers, output_path, table_name)
    union_elapsed = time.monotonic() - t_union
    print(f"  Union complete: {total_rows:,} rows in {_fmt_time(union_elapsed)}")

    # Clean up temp files
    for tmp in temp_hypers:
        try:
            tmp.unlink()
        except OSError:
            pass

    elapsed = time.monotonic() - t0
    print(f"\nDone in {_fmt_time(elapsed)}  →  {output_path}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except HyperException as ex:
        print(ex, file=sys.stderr)
        sys.exit(1)