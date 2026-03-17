# BuildStock Downloader

Download and process [NREL BuildStock](https://www.nrel.gov/buildings/end-use-load-profiles.html) (ComStock & ResStock) datasets from the public AWS S3 bucket (`oedi-data-lake`) and optionally convert them into Tableau Hyper files for analysis.

## Overview

This repository contains two command-line tools:

| Script | Purpose |
|---|---|
| `download_buildstock.py` | Download CSV files from the NREL End-Use Load Profiles dataset on S3 |
| `build_hyper.py` | Combine downloaded CSVs into a single Tableau `.hyper` file (long format, electricity end-uses only) |

### Dataset structure on S3

```
s3://oedi-data-lake/
  nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/
    <year>/<release_name>/timeseries_aggregates/by_state/
      upgrade=0/
        state=AK/
          up0-ak-<building_type>.csv
          ...
        state=AL/
          ...
      upgrade=1/
        ...
```

The bucket is **public** — no AWS credentials are required.

---

## Requirements

- Python 3.10+
- [pandas](https://pandas.pydata.org/)
- [tableauhyperapi](https://help.tableau.com/current/api/hyper_api/en-us/index.html) (only for `build_hyper.py`)

Install dependencies:

```bash
pip install pandas tableauhyperapi
```

> **Note:** `download_buildstock.py` uses only the Python standard library and has no third-party dependencies.

---

## download_buildstock.py

Downloads CSV files from the NREL BuildStock dataset on AWS S3 with parallel workers, progress reporting, and automatic resume support.

### Basic usage

```bash
# Download everything for the default release (warning: can be 500 GB+)
python download_buildstock.py

# Dry run – list files without downloading
python download_buildstock.py --dry-run
```

### Filtering by upgrade and state

```bash
# Download only upgrade 0 (baseline), all states
python download_buildstock.py --upgrades 0

# Download upgrades 0 and 1 for California and Texas only
python download_buildstock.py --upgrades 0 1 --states CA TX
```

### Choosing a release

```bash
# ComStock
python download_buildstock.py --release-year 2025 --release-name comstock_amy2018_release_3

# ResStock
python download_buildstock.py --release-year 2025 --release-name resstock_amy2018_release_1
```

### All options

| Flag | Default | Description |
|---|---|---|
| `--output-dir DIR` | `./downloads/<year>/<release>` | Local directory to save files into |
| `--upgrades N [N ...]` | all | Only download these upgrade numbers |
| `--states XX [XX ...]` | all | Only download these two-letter state codes |
| `--workers N` | `8` | Number of parallel download threads |
| `--no-skip-existing` | skip | Re-download files that already exist locally |
| `--dry-run` | off | List files that would be downloaded without downloading |
| `--release-year YEAR` | `2025` | Dataset release year |
| `--release-name NAME` | `comstock_amy2018_release_3` | Full release name |

### Resume support

By default the downloader **skips files that already exist** locally (based on file size > 0). If a download is interrupted, simply re-run the same command to pick up where you left off. Use `--no-skip-existing` to force re-download.

### Error handling

If any files fail to download, their S3 keys are written to `failed_downloads.txt` in the output directory. The script exits with code `1` when there are failures and `0` on success.

---

## build_hyper.py

Reads the downloaded CSV files, filters to **electricity end-use** columns, melts (unpivots) them into a long-format table, and writes the result to a Tableau `.hyper` file.

For each CSV the script:

1. Reads all rows, keeping non-`out.` columns plus `out.electricity.*` columns.
2. Melts the `out.electricity.*` columns into two new columns:
   - **end_use** — the human-readable end-use name (e.g. `cooling`, `fans`)
   - **kWh** — the corresponding value
3. Drops every other `out.*` column (district cooling, natural gas, etc.).

### Basic usage

```bash
# Process CSVs for a ComStock release
python build_hyper.py --input-dir ./downloads/2025/comstock_amy2018_release_3

# Process CSVs for a ResStock release
python build_hyper.py --input-dir ./downloads/2025/resstock_amy2018_release_1
```

### Filtering and options

```bash
# Custom output file name
python build_hyper.py --input-dir ./downloads/2025/comstock_amy2018_release_3 --output comstock.hyper

# Only process files matching a glob (e.g. one state)
python build_hyper.py --input-dir ./downloads/2025/comstock_amy2018_release_3 --glob "upgrade=0/state=AL/*.csv"

# Use chunked processing to limit memory usage
python build_hyper.py --input-dir ./downloads/2025/comstock_amy2018_release_3 --chunk-size 50
```

---

## Typical workflow

```bash
# 1. Download baseline data for a few states
python download_buildstock.py \
    --release-year 2025 \
    --release-name comstock_amy2018_release_3 \
    --upgrades 0 \
    --states CA TX NY

# 2. Convert to a Hyper file for Tableau (default output: comstock_amy2018_release_3.hyper)
python build_hyper.py \
    --input-dir ./downloads/2025/comstock_amy2018_release_3

# 3. Open comstock_amy2018_release_3.hyper in Tableau Desktop or Tableau Public
```

---

## License

This project is provided as-is for working with publicly available NREL BuildStock data. The underlying datasets are produced by the [National Renewable Energy Laboratory (NREL)](https://www.nrel.gov/) and are subject to their own terms of use.
