#!/usr/bin/env python3
"""
Download CSV files from the NREL ComStock AMY2018 Release 3 dataset on AWS S3.

Bucket  : oedi-data-lake  (public, no credentials required)
Prefix  : nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/
          2025/comstock_amy2018_release_3/timeseries_aggregates/by_state/

Structure on S3:
    by_state/
        upgrade=0/
            state=AK/
                up0-ak-<building_type>.csv
                ...
            state=AL/
                ...
        upgrade=1/
            ...

Usage examples:
    # Download everything (warning: ~500 GB+)
    python download_buildstock.py

    # Dry run – list files without downloading
    python download_buildstock.py --dry-run

    # Download only upgrade 0, all states
    python download_buildstock.py --upgrades 0

    # Download upgrade 0 and 1 for CA and TX only
    python download_buildstock.py --upgrades 0 1 --states CA TX

    # Use 16 parallel workers and save to a custom directory
    python download_buildstock.py --workers 16 --output-dir /data/buildstock

    # Download a different release (ComStock or ResStock)
    python download_buildstock.py --release-name comstock_amy2018_release_3
    python download_buildstock.py --release-name resstock_amy2018_release_1

    # My saved usage
    python download_buildstock.py --upgrades 0 --dry-run --release-year 2025 --release-name comstock_amy2018_release_2
    python download_buildstock.py --upgrades 0 --dry-run --release-year 2025 --release-name resstock_amy2018_release_1
"""

import argparse
import concurrent.futures
import os
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from threading import Lock

# ---------------------------------------------------------------------------
# S3 configuration
# ---------------------------------------------------------------------------
BUCKET = "oedi-data-lake"
BASE_URL = f"https://{BUCKET}.s3.amazonaws.com"

DEFAULT_RELEASE_YEAR = 2025
DEFAULT_RELEASE_NAME = "comstock_amy2018_release_3"

# FIPS state code -> two-letter abbreviation (for gap model county-to-state mapping)
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
STATE_TO_FIPS = {v: k for k, v in FIPS_TO_STATE.items()}


def county_to_state_abbr(county_code: str) -> str | None:
    """Extract state abbreviation from a GISJOIN county code like G0100010."""
    if len(county_code) >= 3 and county_code.startswith("G"):
        return FIPS_TO_STATE.get(county_code[1:3])
    return None


def build_prefix(release_year: int, release_name: str, gap_model: bool = False) -> str:
    """Return the S3 key prefix for the given release year and release name."""
    if gap_model:
        return (
            f"nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/"
            f"{release_year}/{release_name}/"
            f"commercial_gap_model/by_county/"
        )
    return (
        f"nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/"
        f"{release_year}/{release_name}/"
        f"timeseries_aggregates/by_state/"
    )

# ---------------------------------------------------------------------------
# S3 listing helpers
# ---------------------------------------------------------------------------

def _s3_list_page(prefix: str, continuation_token: str | None = None) -> ET.Element:
    """Fetch one page of ListObjectsV2 results and return the parsed XML tree."""
    params = {
        "list-type": "2",
        "prefix": prefix,
        "max-keys": "1000",
    }
    if continuation_token:
        params["continuation-token"] = continuation_token

    query = "&".join(
        f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params.items()
    )
    url = f"{BASE_URL}/?{query}"

    with urllib.request.urlopen(url, timeout=30) as resp:
        return ET.fromstring(resp.read().decode("utf-8"))


def list_csv_files(prefix: str) -> list[dict]:
    """
    Recursively list all .csv objects under *prefix* using the S3 REST API.
    Returns a list of dicts with keys: ``key``, ``size``.
    Handles pagination automatically.
    """
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    files: list[dict] = []
    continuation_token: str | None = None

    while True:
        tree = _s3_list_page(prefix, continuation_token)

        for content in tree.findall(f"{ns}Contents"):
            key = content.find(f"{ns}Key").text  # type: ignore[union-attr]
            size = int(content.find(f"{ns}Size").text)  # type: ignore[union-attr]
            if key and key.endswith(".csv"):
                files.append({"key": key, "size": size})

        is_truncated = (tree.findtext(f"{ns}IsTruncated") or "false").lower() == "true"
        if not is_truncated:
            break

        next_token_elem = tree.find(f"{ns}NextContinuationToken")
        if next_token_elem is None or not next_token_elem.text:
            break
        continuation_token = next_token_elem.text

    return files


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def download_file(
    key: str,
    output_dir: Path,
    skip_existing: bool = True,
    prefix: str = "",
) -> tuple[str, str, str]:
    """
    Download a single S3 object to *output_dir*, preserving the relative path
    under *prefix*.

    Returns a tuple of (key, status, message) where status is one of:
    ``"skipped"``, ``"ok"``, ``"error"``.
    """
    # Strip the top-level prefix so we only keep upgrade=N/state=XX/file.csv
    relative_path = key[len(prefix):]
    local_path = output_dir / relative_path

    # Resume / skip-existing logic
    if skip_existing and local_path.exists() and local_path.stat().st_size > 0:
        return key, "skipped", "already exists"

    local_path.parent.mkdir(parents=True, exist_ok=True)

    url = f"{BASE_URL}/{urllib.parse.quote(key, safe='/')}"
    tmp_path = local_path.with_suffix(local_path.suffix + ".part")

    try:
        with urllib.request.urlopen(url, timeout=120) as response:
            with open(tmp_path, "wb") as fh:
                while True:
                    chunk = response.read(1 << 16)  # 64 KiB
                    if not chunk:
                        break
                    fh.write(chunk)
        tmp_path.rename(local_path)
        return key, "ok", "downloaded"
    except Exception as exc:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return key, "error", str(exc)


# ---------------------------------------------------------------------------
# Progress display
# ---------------------------------------------------------------------------

class Progress:
    """Thread-safe progress counter that prints a single updating line."""

    def __init__(self, total: int, total_bytes: int) -> None:
        self.total = total
        self.total_bytes = total_bytes
        self.done = 0
        self.skipped = 0
        self.errors = 0
        self.bytes_done = 0
        self._lock = Lock()
        self._start = time.monotonic()

    def update(self, status: str, size: int) -> None:
        with self._lock:
            self.done += 1
            self.bytes_done += size
            if status == "skipped":
                self.skipped += 1
            elif status == "error":
                self.errors += 1
            self._print()

    def _print(self) -> None:
        elapsed = time.monotonic() - self._start
        rate = self.bytes_done / elapsed if elapsed > 0 else 0
        pct = 100 * self.done / self.total if self.total else 0
        line = (
            f"  {self.done:>6}/{self.total}  "
            f"({pct:5.1f}%)  "
            f"{_fmt_bytes(self.bytes_done)}/{_fmt_bytes(self.total_bytes)}  "
            f"@ {_fmt_bytes(rate)}/s  "
            f"err={self.errors} skip={self.skipped}  "
            f"{_fmt_time(elapsed)}"
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
        self._print()
        print()  # newline after the progress line


def _fmt_bytes(b: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:6.1f} {unit}"
        b /= 1024
    return f"{b:6.1f} PB"


def _fmt_time(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Local directory to save files into "
             "(default: ./downloads/<release_year>/<release_name>)",
    )
    parser.add_argument(
        "--upgrades",
        nargs="+",
        type=int,
        metavar="N",
        help="Only download files for these upgrade numbers (e.g. --upgrades 0 1 2). "
             "Defaults to all upgrades.",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        type=str,
        metavar="XX",
        help="Only download files for these two-letter state codes "
             "(e.g. --states CA TX NY). Defaults to all states.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of parallel download threads (default: 8)",
    )
    parser.add_argument(
        "--no-skip-existing",
        dest="skip_existing",
        action="store_false",
        default=True,
        help="Re-download files that already exist locally (overwrites).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be downloaded without actually downloading them.",
    )
    parser.add_argument(
        "--release-year",
        type=int,
        default=DEFAULT_RELEASE_YEAR,
        metavar="YEAR",
        help=f"Dataset release year (default: {DEFAULT_RELEASE_YEAR})",
    )
    parser.add_argument(
        "--release-name",
        type=str,
        default=DEFAULT_RELEASE_NAME,
        metavar="NAME",
        help=f"Full release name, e.g. 'comstock_amy2018_release_3' or "
             f"'resstock_amy2018_release_1' (default: {DEFAULT_RELEASE_NAME})",
    )
    parser.add_argument(
        "--gap-model",
        action="store_true",
        help="Download the commercial gap model (by county) instead of "
             "timeseries aggregates (by state).",
    )
    parser.add_argument(
        "--counties",
        nargs="+",
        type=str,
        metavar="CODE",
        help="Only download files for these GISJOIN county codes "
             "(e.g. --counties G0100010 G0100030). Only used with --gap-model.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    default_subdir = args.release_name
    if args.gap_model:
        default_subdir = args.release_name + "_gap_model"
    output_dir = Path(
        args.output_dir
        if args.output_dir
        else os.path.join("downloads", str(args.release_year), default_subdir)
    ).expanduser().resolve()
    upgrades_filter = {str(u) for u in args.upgrades} if args.upgrades else None
    states_filter = {s.upper() for s in args.states} if args.states else None
    counties_filter = (
        {c.upper() for c in args.counties} if args.gap_model and args.counties else None
    )
    # When --gap-model + --states, convert state abbreviations to FIPS prefixes
    state_fips_prefixes: set[str] | None = None
    if args.gap_model and states_filter:
        state_fips_prefixes = set()
        for st in states_filter:
            fips = STATE_TO_FIPS.get(st)
            if fips:
                state_fips_prefixes.add(fips)
            else:
                print(f"Warning: unknown state abbreviation '{st}', skipping.")
    prefix = build_prefix(args.release_year, args.release_name, gap_model=args.gap_model)

    # ------------------------------------------------------------------ #
    # Step 1 – discover files                                             #
    # ------------------------------------------------------------------ #
    print(f"Listing CSV files under s3://{BUCKET}/{prefix}")
    print("(This may take a moment for large prefixes…)\n")

    all_files = list_csv_files(prefix)

    # Apply upgrade / state / county filters
    def _keep(entry: dict) -> bool:
        rel = entry["key"][len(prefix):]         # upgrade=N/state=XX/... or upgrade=N/county=G.../...
        parts = rel.split("/")
        if len(parts) < 3:
            return True  # unexpected layout – include by default

        upgrade_part = parts[0]   # "upgrade=N"
        upgrade_num = upgrade_part.split("=")[-1]
        if upgrades_filter and upgrade_num not in upgrades_filter:
            return False

        if args.gap_model:
            # Gap model: upgrade=N/county=GXXXXXXX/filename.csv
            county_part = parts[1]   # "county=GXXXXXXX"
            county_code = county_part.split("=")[-1].upper()
            if counties_filter and county_code not in counties_filter:
                return False
            if state_fips_prefixes:
                county_state_fips = county_code[1:3] if county_code.startswith("G") else ""
                if county_state_fips not in state_fips_prefixes:
                    return False
        else:
            # Standard: upgrade=N/state=XX/filename.csv
            state_part = parts[1]   # "state=XX"
            state_code = state_part.split("=")[-1].upper()
            if states_filter and state_code not in states_filter:
                return False
        return True

    files = [f for f in all_files if _keep(f)]

    total_bytes = sum(f["size"] for f in files)
    print(f"Found {len(all_files):,} CSV files total on S3.")
    if upgrades_filter or states_filter or counties_filter:
        print(f"After filtering: {len(files):,} files selected.")
    print(f"Total size: {_fmt_bytes(total_bytes).strip()}\n")

    if not files:
        print("No files match the current filters. Nothing to do.")
        return 0

    # ------------------------------------------------------------------ #
    # Step 2 – dry run or download                                        #
    # ------------------------------------------------------------------ #
    if args.dry_run:
        print("DRY RUN – files that would be downloaded:\n")
        for entry in files:
            rel = entry["key"][len(prefix):]
            print(f"  {rel}  ({_fmt_bytes(entry['size']).strip()})")
        print(f"\nTotal: {len(files):,} files / {_fmt_bytes(total_bytes).strip()}")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Saving to: {output_dir}")
    print(f"Workers  : {args.workers}")
    print(f"Skip existing: {args.skip_existing}\n")

    progress = Progress(len(files), total_bytes)

    failed: list[tuple[str, str]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_entry = {
            executor.submit(
                download_file, entry["key"], output_dir, args.skip_existing, prefix
            ): entry
            for entry in files
        }

        for future in concurrent.futures.as_completed(future_to_entry):
            entry = future_to_entry[future]
            try:
                key, status, message = future.result()
            except Exception as exc:
                key = entry["key"]
                status, message = "error", str(exc)

            progress.update(status, entry["size"] if status != "error" else 0)

            if status == "error":
                failed.append((key, message))

    progress.finish()

    # ------------------------------------------------------------------ #
    # Step 3 – summary                                                    #
    # ------------------------------------------------------------------ #
    downloaded = progress.done - progress.skipped - progress.errors
    print(f"\n{'='*60}")
    print(f"Downloaded : {downloaded:,}")
    print(f"Skipped    : {progress.skipped:,}  (already existed)")
    print(f"Errors     : {progress.errors:,}")
    print(f"Output dir : {output_dir}")
    print(f"{'='*60}")

    if failed:
        print("\nFailed files:")
        for key, msg in failed:
            rel = key[len(prefix):]
            print(f"  {rel}\n    → {msg}")

        # Write a retry list
        retry_path = output_dir / "failed_downloads.txt"
        with open(retry_path, "w") as fh:
            fh.write("\n".join(k for k, _ in failed) + "\n")
        print(f"\nFailed keys written to: {retry_path}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
