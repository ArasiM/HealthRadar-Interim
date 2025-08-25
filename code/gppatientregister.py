"""
HealthRadar – GP Patient Registers Aggregation
Author: <Your Name>
Purpose:
    Summarize monthly GP patient registers (24 months) into a tidy, aggregated table:
        YearMonth (YYYYMM), PracticeCode, NumberOfPatients
Output:
    <BASE>\processed\GPPatientRegisters_<MINYYYYMM>-<MAXYYYYMM>.csv

Notes:
    - Designed for reliability and clarity (enterprise style).
    - Strict column selection; defensive parsing of dates and numerics.
    - File range in output name is derived from the data (not filenames).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import pandas as pd


# ----------------------------
# Configuration
# ----------------------------
BASE = Path(r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation")
SRC  = BASE / "sourcedata" / "GP_Patient_Registers"
OUT  = BASE / "processed"
OUT.mkdir(parents=True, exist_ok=True)

# Expected schema (subset we actually use)
USECOLS = [
    "EXTRACT_DATE",          # e.g., 01-Sep-23
    "CODE",                  # practice code (e.g., A84002)
    "NUMBER_OF_PATIENTS",    # integer
]

DATE_FMT = "%d-%b-%y"        # strict parsing (01-Sep-23 → 202309)


# ----------------------------
# Logging (console only)
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ----------------------------
# Helpers
# ----------------------------
def parse_yearmonth(date_series: pd.Series) -> pd.Series:
    """
    Convert EXTRACT_DATE to YYYYMM robustly.
    Handles: 01Sep2023, 01-Sep-23, 01-Sep-2023, 2023-09-01, 01/09/2023, etc.
    Raises with examples if anything can't be parsed.
    """
    s = date_series.astype(str).str.strip()

    # First try Pandas' mixed inference with day-first (covers most cases)
    dt = pd.to_datetime(s, format="mixed", dayfirst=True, errors="coerce")

    # Fallbacks for stubborn cases (vectorized, no assumptions about separators)
    fmts = [
        "%d%b%Y",   # 01Sep2023
        "%d%b%y",   # 01Sep23
        "%d-%b-%Y", # 01-Sep-2023
        "%d-%b-%y", # 01-Sep-23
        "%Y-%m-%d", # 2023-09-01
        "%d/%m/%Y", # 01/09/2023
        "%d/%m/%y", # 01/09/23
        "%m/%d/%Y", # 09/01/2023 (rare, but harmless)
        "%m/%d/%y",
    ]
    if dt.isna().any():
        for fmt in fmts:
            mask = dt.isna()
            if not mask.any():
                break
            dt.loc[mask] = pd.to_datetime(s[mask], format=fmt, errors="coerce")

    # If anything still failed, raise with examples for quick debugging
    if dt.isna().any():
        bad = s[dt.isna()].dropna().unique()[:5]
        raise ValueError(f"Unparseable EXTRACT_DATE values (first few): {list(bad)}")

    return dt.dt.strftime("%Y%m")



def to_int(series: pd.Series) -> pd.Series:
    """
    Convert NUMBER_OF_PATIENTS to int safely (strip commas/spaces).
    """
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="raise").astype("int64")


def read_one_csv(path: Path) -> pd.DataFrame:
    """
    Read a single monthly file with strict schema and return the reduced frame.
    """
    df = pd.read_csv(path, usecols=USECOLS, dtype=str)
    df = df.rename(columns={
        "CODE": "PracticeCode",
        "NUMBER_OF_PATIENTS": "NumberOfPatients",
    })
    # Derive normalized fields
    df["YearMonth"] = parse_yearmonth(df["EXTRACT_DATE"])
    df["NumberOfPatients"] = to_int(df["NumberOfPatients"])
    # Final projection
    return df[["YearMonth", "PracticeCode", "NumberOfPatients"]]


def aggregate_all(files: List[Path]) -> pd.DataFrame:
    """
    Read, concatenate, and aggregate across all input files.
    """
    parts: List[pd.DataFrame] = []
    for f in files:
        try:
            logging.info("Reading %s", f.name)
            parts.append(read_one_csv(f))
        except Exception as ex:
            # Fail fast with clear context: data integrity is more important than partial output
            logging.error("Failed to process %s: %s", f, ex)
            raise

    combined = pd.concat(parts, ignore_index=True)
    # Aggregate in case a practice appears multiple times within a month
    agg = (
        combined
        .groupby(["YearMonth", "PracticeCode"], as_index=False, sort=True)
        .agg(NumberOfPatients=("NumberOfPatients", "sum"))
        .sort_values(["YearMonth", "PracticeCode"])
        .reset_index(drop=True)
    )
    return agg


def main() -> None:
    # Discover monthly files
    files = sorted(SRC.glob("gp_patient_registration_*.csv"))
    if not files:
        raise FileNotFoundError(f"No files found in: {SRC}")

    logging.info("Discovered %d file(s).", len(files))

    # Build aggregated dataset
    agg = aggregate_all(files)

    # Compute range for dynamic filename
    min_ym = agg["YearMonth"].min()
    max_ym = agg["YearMonth"].max()

    out_path = OUT / f"GPPatientRegisters_{min_ym}-{max_ym}.csv"
    agg.to_csv(out_path, index=False)

    logging.info("Wrote %s (rows=%d)", out_path, len(agg))
    logging.info("Columns: %s", ", ".join(agg.columns))


if __name__ == "__main__":
    main()
