#!/usr/bin/env python3
"""
Generate a county-group CSV from the ResStock upgrade0 parquet file.

The script:
1. Reads the parquet and keeps the county/state/PUMA/weight fields.
2. Derives county_fips5 and county_groups.
3. Adds county_name.
4. Adds one dummy row for each county_fips5 missing from the county reference,
   using a dominant 2010 PUMA lookup when available.
5. Writes a full CSV with quoted fields so FIPS-like strings stay intact.
6. Writes a second CSV with one row per county-to-county_group mapping.

Usage:
    python make_county_groups.py
    python make_county_groups.py --parquet /path/to/upgrade0.parquet
    python make_county_groups.py --output upgrade0_county_groups_with_fips.csv
    python make_county_groups.py --mapping-output county_group_mapping.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import pandas as pd


DEFAULT_PARQUET = Path(
    "/Users/ewilson/Documents/code/buildstock-downloader/annual/"
    "resstock_amy2018_release_1 annual/upgrade0.parquet"
)
DEFAULT_OUTPUT = Path("upgrade0_county_groups_with_fips.csv")
DEFAULT_MAPPING_OUTPUT = Path("county_group_mapping.csv")
COUNTY_REF_URL = "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"
TRACT_PUMA_2010_URL = "https://www2.census.gov/geo/docs/maps-data/data/rel/2010_Census_Tract_to_2010_PUMA.txt"

ACTUAL_COLS = ["bldg_id", "in.county", "in.state", "in.puma", "weight"]

STATE_TO_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08", "CT": "09", "DE": "10",
    "DC": "11", "FL": "12", "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18", "IA": "19",
    "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34", "NM": "35",
    "NY": "36", "NC": "37", "ND": "38", "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56",
}
FIPS_TO_STATE = {value: key for key, value in STATE_TO_FIPS.items()}

AK_NAME_TO_FIPS = {
    "AK, Aleutians East Borough": "02013",
    "AK, Aleutians West Census Area": "02016",
    "AK, Anchorage Municipality": "02020",
    "AK, Bethel Census Area": "02050",
    "AK, Bristol Bay Borough": "02060",
    "AK, Denali Borough": "02068",
    "AK, Dillingham Census Area": "02070",
    "AK, Fairbanks North Star Borough": "02090",
    "AK, Haines Borough": "02100",
    "AK, Hoonah-Angoon Census Area": "02105",
    "AK, Juneau City and Borough": "02110",
    "AK, Kenai Peninsula Borough": "02122",
    "AK, Ketchikan Gateway Borough": "02130",
    "AK, Kodiak Island Borough": "02150",
    "AK, Kusilvak Census Area": "02158",
    "AK, Lake and Peninsula Borough": "02164",
    "AK, Matanuska-Susitna Borough": "02170",
    "AK, Nome Census Area": "02180",
    "AK, North Slope Borough": "02185",
    "AK, Northwest Arctic Borough": "02188",
    "AK, Petersburg Borough": "02195",
    "AK, Sitka City and Borough": "02220",
    "AK, Skagway Municipality": "02230",
    "AK, Southeast Fairbanks Census Area": "02240",
    "AK, Valdez-Cordova Census Area": "02261",
    "AK, Wrangell City and Borough": "02275",
    "AK, Yakutat City and Borough": "02282",
    "AK, Yukon-Koyukuk Census Area": "02290",
}

HI_NAME_TO_FIPS = {
    "HI, Hawaii County": "15001",
    "HI, Honolulu County": "15003",
    "HI, Kalawao County": "15005",
    "HI, Kauai County": "15007",
    "HI, Maui County": "15009",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--parquet",
        type=Path,
        default=DEFAULT_PARQUET,
        help=f"Input parquet path (default: {DEFAULT_PARQUET})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--mapping-output",
        type=Path,
        default=DEFAULT_MAPPING_OUTPUT,
        help=f"County-to-county_group mapping CSV path (default: {DEFAULT_MAPPING_OUTPUT})",
    )
    return parser.parse_args()


def load_selected_df(parquet_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)
    missing_cols = [column for column in ACTUAL_COLS if column not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing expected columns: {missing_cols}")

    selected_df = df[ACTUAL_COLS].copy()
    selected_df.columns = ["bldgid", "county", "state", "puma", "weight"]
    return selected_df


def add_county_fips5(selected_df: pd.DataFrame) -> pd.DataFrame:
    county_parts = selected_df["county"].str.extract(r"^G(?P<state>\d{2})(?P<county4>\d{4})0$")
    selected_df["county_fips5"] = county_parts["state"] + county_parts["county4"].str[-3:]

    ak_mask = selected_df["county_fips5"].isna() & selected_df["county"].str.startswith("AK, ", na=False)
    hi_mask = selected_df["county_fips5"].isna() & selected_df["county"].str.startswith("HI, ", na=False)

    selected_df.loc[ak_mask, "county_fips5"] = selected_df.loc[ak_mask, "county"].map(AK_NAME_TO_FIPS)
    selected_df.loc[hi_mask, "county_fips5"] = selected_df.loc[hi_mask, "county"].map(HI_NAME_TO_FIPS)
    selected_df["county_fips5"] = selected_df["county_fips5"].astype("string").str.zfill(5)
    return selected_df


def assign_county_groups(selected_df: pd.DataFrame) -> pd.DataFrame:
    base = selected_df[["county", "state", "puma", "weight"]].copy()
    weight_by_cp = base.groupby(["county", "puma"], as_index=False)["weight"].sum()

    counties_per_puma = (
        base.groupby("puma", as_index=False)["county"]
        .nunique()
        .rename(columns={"county": "num_counties"})
    )
    multi_county_pumas = set(counties_per_puma.loc[counties_per_puma["num_counties"] > 1, "puma"])

    pumas_per_county = (
        base.groupby("county", as_index=False)["puma"]
        .nunique()
        .rename(columns={"puma": "num_pumas"})
    )

    multi_puma_counties: set[str] = set()
    for county in pumas_per_county.loc[pumas_per_county["num_pumas"] > 1, "county"]:
        county_pumas = set(base.loc[base["county"] == county, "puma"].dropna().unique())
        if all(puma not in multi_county_pumas for puma in county_pumas):
            multi_puma_counties.add(county)

    county_to_puma_group: dict[str, str | None] = {}
    for county in base["county"].dropna().unique():
        if county in multi_puma_counties:
            county_to_puma_group[county] = None
            continue

        county_pumas = set(base.loc[base["county"] == county, "puma"].dropna().unique())
        candidates = [puma for puma in county_pumas if puma in multi_county_pumas]

        if len(candidates) == 0:
            county_to_puma_group[county] = None
        elif len(candidates) == 1:
            county_to_puma_group[county] = candidates[0]
        else:
            candidate_weights: dict[str, float] = {}
            for puma in candidates:
                weight = weight_by_cp.loc[
                    (weight_by_cp["county"] == county) & (weight_by_cp["puma"] == puma),
                    "weight",
                ].sum()
                candidate_weights[puma] = weight
            county_to_puma_group[county] = max(candidate_weights, key=candidate_weights.get)

    puma_to_group_id: dict[str, str] = {}
    group_counter = 0
    county_to_group: dict[str, str] = {}
    for county, puma in county_to_puma_group.items():
        if puma is not None:
            if puma not in puma_to_group_id:
                puma_to_group_id[puma] = f"county_group_{group_counter}"
                group_counter += 1
            county_to_group[county] = puma_to_group_id[puma]
        else:
            county_to_group[county] = f"independent_county_{county}"

    selected_df["county_groups"] = selected_df["county"].map(county_to_group)

    ct_missing_mask = (
        (selected_df["state"] == "CT")
        & (selected_df["county_groups"].isna() | (selected_df["county_groups"].astype(str).str.len() == 0))
    )
    selected_df.loc[ct_missing_mask, "county_groups"] = (
        "independent_county_" + selected_df.loc[ct_missing_mask, "county"].astype(str)
    )

    county_group_size = (
        selected_df[["county", "county_groups"]]
        .drop_duplicates()
        .groupby("county_groups", as_index=False)["county"]
        .nunique()
        .rename(columns={"county": "num_counties"})
    )
    singleton_groups = set(
        county_group_size.loc[
            county_group_size["county_groups"].astype(str).str.startswith("county_group_")
            & (county_group_size["num_counties"] == 1),
            "county_groups",
        ]
    )
    singleton_mask = selected_df["county_groups"].isin(singleton_groups)
    selected_df.loc[singleton_mask, "county_groups"] = (
        "independent_county_" + selected_df.loc[singleton_mask, "county"].astype(str)
    )
    return selected_df


def load_county_reference() -> pd.DataFrame:
    county_ref = pd.read_csv(COUNTY_REF_URL, sep="|", dtype=str)
    county_ref = county_ref[county_ref["STATEFP"].isin(set(STATE_TO_FIPS.values()))].copy()
    county_ref["county_fips5"] = county_ref["STATEFP"] + county_ref["COUNTYFP"]
    county_ref["state"] = county_ref["STATEFP"].map(FIPS_TO_STATE)
    county_ref["county"] = "G" + county_ref["STATEFP"] + county_ref["COUNTYFP"] + "0"
    return county_ref


def add_county_name(selected_df: pd.DataFrame, county_ref: pd.DataFrame) -> pd.DataFrame:
    county_name_lookup = county_ref.set_index("county_fips5")["COUNTYNAME"].to_dict()
    selected_df["county_name"] = selected_df["county_fips5"].map(county_name_lookup).astype("string")
    return selected_df


def add_missing_counties(selected_df: pd.DataFrame, county_ref: pd.DataFrame) -> pd.DataFrame:
    existing_fips5 = set(selected_df["county_fips5"].dropna().astype(str).unique())
    missing_ref = county_ref[~county_ref["county_fips5"].isin(existing_fips5)].copy()
    if missing_ref.empty:
        return selected_df

    tract_puma = pd.read_csv(TRACT_PUMA_2010_URL, dtype=str)
    tract_puma["county_fips5"] = tract_puma["STATEFP"] + tract_puma["COUNTYFP"]
    tract_puma["puma_g"] = "G" + tract_puma["STATEFP"] + tract_puma["PUMA5CE"].str.zfill(6)

    county_puma_counts = (
        tract_puma.groupby(["county_fips5", "puma_g"], as_index=False)
        .size()
        .rename(columns={"size": "tract_count"})
    )
    county_puma_primary = county_puma_counts.sort_values(
        ["county_fips5", "tract_count", "puma_g"],
        ascending=[True, False, True],
    ).drop_duplicates("county_fips5")
    county_to_puma = dict(zip(county_puma_primary["county_fips5"], county_puma_primary["puma_g"]))

    puma_group_weights = (
        selected_df.dropna(subset=["puma", "county_groups"])
        .groupby(["puma", "county_groups"], as_index=False)["weight"]
        .sum()
    )
    puma_group_majority = puma_group_weights.sort_values(
        ["puma", "weight", "county_groups"],
        ascending=[True, False, True],
    ).drop_duplicates("puma")
    puma_to_group = dict(zip(puma_group_majority["puma"], puma_group_majority["county_groups"]))

    output_columns = [
        "bldgid",
        "county",
        "state",
        "puma",
        "weight",
        "county_fips5",
        "county_groups",
        "county_name",
    ]
    selected_df = selected_df[output_columns].copy()

    dummy_rows: list[dict[str, object]] = []
    for _, row in missing_ref.iterrows():
        county_fips5 = row["county_fips5"]
        county_code = row["county"]
        puma_value = county_to_puma.get(county_fips5)
        county_group = puma_to_group.get(puma_value, f"independent_county_{county_code}")

        dummy_rows.append(
            {
                "bldgid": f"dummy_{county_fips5}",
                "county": county_code,
                "state": row["state"],
                "puma": puma_value,
                "weight": 0.0,
                "county_fips5": county_fips5,
                "county_groups": county_group,
                "county_name": row["COUNTYNAME"],
            }
        )

    dummy_df = pd.DataFrame(dummy_rows, columns=output_columns)
    return pd.concat([selected_df, dummy_df], ignore_index=True)


def build_output_dataframe(parquet_path: Path) -> pd.DataFrame:
    selected_df = load_selected_df(parquet_path)
    selected_df = add_county_fips5(selected_df)
    selected_df = assign_county_groups(selected_df)
    county_ref = load_county_reference()
    selected_df = add_county_name(selected_df, county_ref)
    selected_df = add_missing_counties(selected_df, county_ref)
    return selected_df[
        ["bldgid", "county", "state", "puma", "weight", "county_fips5", "county_groups", "county_name"]
    ]


def build_mapping_dataframe(output_df: pd.DataFrame) -> pd.DataFrame:
    mapping_df = output_df[
        ["state", "county", "county_name", "county_fips5", "county_groups"]
    ].drop_duplicates()
    return mapping_df.sort_values(["state", "county_fips5", "county_groups"]).reset_index(drop=True)


def main() -> int:
    args = parse_args()
    parquet_path = args.parquet.expanduser().resolve()
    output_path = args.output.expanduser().resolve()
    mapping_output_path = args.mapping_output.expanduser().resolve()

    if not parquet_path.exists():
        print(f"Error: parquet file does not exist: {parquet_path}")
        return 1

    output_df = build_output_dataframe(parquet_path)
    mapping_df = build_mapping_dataframe(output_df)
    output_df.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)
    mapping_df.to_csv(mapping_output_path, index=False, quoting=csv.QUOTE_ALL)

    print(f"Wrote {len(output_df):,} rows to {output_path}")
    print(f"Wrote {len(mapping_df):,} county mappings to {mapping_output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())