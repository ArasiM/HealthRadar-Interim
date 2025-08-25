r"""
HealthRadar — Build ONS Regional Mapping (Postcode→ Region/ICB/MSOA/LSOA/Country)

This script builds a single, comprehensive mapping file that links UK postcodes
to their corresponding administrative and health geographies. It starts with a
base ONS postcode file containing various codes and merges it with several
lookup files to append the human-readable names for each geography.

Inputs (update SRC_BASE if your folder names change):
    SRC_BASE/ONS_postcode.csv                               (columns: PCD2, PCDS, NHSER, ICB, MSOA21, LSOA21, CTRY, [LSOA11 or LSOA11CD])
    SRC_BASE/NHSER names and codes EN as at 04_24.csv       (columns: NHSER24CDH, NHSER24NM)
    SRC_BASE/MSOA (2021) names and codes EW as at 12_21.csv (columns: MSOA21CD, MSOA21NM)
    SRC_BASE/LSOA (2021) names and codes EW as at 12_21.csv (columns: LSOA21CD, LSOA21NM)
    SRC_BASE/ICB names and codes EN as at 04_23.csv         (columns: ICB23CDH, ICB23NM)
    SRC_BASE/Country names and codes UK as at 08_12.csv     (columns: CTRY12CD, CTRY12NM)
    SRC_BASE/LSOA (2011) names and codes UK as at 12_12.csv (columns: LSOA11CD, LSOA11NM)

Output:
    OUT_BASE/ONSRegionalMapping.csv

Final Columns:
    PCD2, PCDS,
    RegionCode, RegionName,
    ICBID, ICBName,
    MSOAID, MSOAName,
    LSOAID, LSOAName,
    LSOA11ID, LSOA11Name,
    CountryName
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import pandas as pd


# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
SRC_BASE = Path(r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\sourcedata\ONSPostCodeRegion")
OUT_BASE = Path(r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\processed")
OUT_BASE.mkdir(parents=True, exist_ok=True)

ONS_FILE      = SRC_BASE / "ONS_postcode.csv"
REGION_FILE   = SRC_BASE / "NHSER names and codes EN as at 04_24.csv"
MSOA_FILE     = SRC_BASE / "MSOA (2021) names and codes EW as at 12_21.csv"
LSOA_FILE     = SRC_BASE / "LSOA (2021) names and codes EW as at 12_21.csv"
ICB_FILE      = SRC_BASE / "ICB names and codes EN as at 04_23.csv"
COUNTRY_FILE  = SRC_BASE / "Country names and codes UK as at 08_12.csv"
# NEW: LSOA 2011 lookup
LSOA11_FILE   = SRC_BASE / "LSOA (2011) names and codes UK as at 12_12.csv"

OUT_FILE      = OUT_BASE / "ONSRegionalMapping.csv"

# Columns we expect in the ONS_postcode slice.
# NOTE: LSOA11/LSOA11CD is optional; we'll handle it if present.
ONS_KEEP_COLS = ["PCD2", "PCDS", "NHSER", "ICB", "MSOA21", "LSOA21", "CTRY"]


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
)


# ------------------------------------------------------------
# Utilities
# ------------------------------------------------------------
def _read_csv(path: Path, usecols: List[str] | None = None) -> pd.DataFrame:
    """Read CSV with trimmed column names and string dtypes."""
    if not path.exists():
        logging.error("Missing file: %s", path)
        raise FileNotFoundError(f"Missing file: {path}")
    df = pd.read_csv(path, dtype=str, low_memory=False, usecols=usecols)
    # Clean column names and data by stripping whitespace
    df = df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c)
    for c in df.columns:
        if df[c].dtype == 'object':
            df[c] = df[c].str.strip()
    return df


def _require_cols(df: pd.DataFrame, need: List[str], df_name: str) -> None:
    """Check if a dataframe contains all required columns."""
    missing = [c for c in need if c not in df.columns]
    if missing:
        logging.error("%s is missing columns: %s", df_name, missing)
        raise ValueError(f"{df_name} is missing columns: {missing}")


# ------------------------------------------------------------
# Main Script Logic
# ------------------------------------------------------------
def main() -> None:
    """Main function to execute the data processing pipeline."""
    logging.info("--- Starting ONS Regional Mapping Build ---")

    logging.info("Loading ONS_postcode slice: %s", ONS_FILE)
    ons = _read_csv(ONS_FILE)

    # Allow optional LSOA11/LSOA11CD in the ONS input
    ons_cols = set(ons.columns)
    lsoa11_key_in_ons = None
    if "LSOA11" in ons_cols:
        lsoa11_key_in_ons = "LSOA11"
    elif "LSOA11CD" in ons_cols:
        lsoa11_key_in_ons = "LSOA11CD"

    keep_cols = ONS_KEEP_COLS.copy()
    if lsoa11_key_in_ons:
        keep_cols.append(lsoa11_key_in_ons)

    _require_cols(ons, ONS_KEEP_COLS, "ONS_postcode.csv")
    ons = ons[keep_cols].copy()

    logging.info("Loading Region lookup: %s", REGION_FILE)
    region = _read_csv(REGION_FILE)
    _require_cols(region, ["NHSER24CDH", "NHSER24NM"], REGION_FILE.name)

    logging.info("Loading MSOA 2021 lookup: %s", MSOA_FILE)
    msoa = _read_csv(MSOA_FILE)
    _require_cols(msoa, ["MSOA21CD", "MSOA21NM"], MSOA_FILE.name)

    logging.info("Loading LSOA 2021 lookup: %s", LSOA_FILE)
    lsoa = _read_csv(LSOA_FILE)
    _require_cols(lsoa, ["LSOA21CD", "LSOA21NM"], LSOA_FILE.name)

    logging.info("Loading ICB lookup: %s", ICB_FILE)
    icb = _read_csv(ICB_FILE)
    _require_cols(icb, ["ICB23CDH", "ICB23NM"], ICB_FILE.name)

    logging.info("Loading Country lookup: %s", COUNTRY_FILE)
    country = _read_csv(COUNTRY_FILE)
    _require_cols(country, ["CTRY12CD", "CTRY12NM"], COUNTRY_FILE.name)

    # NEW: Load LSOA 2011 lookup
    lsoa11 = None
    try:
        logging.info("Loading LSOA 2011 lookup: %s", LSOA11_FILE)
        lsoa11 = _read_csv(LSOA11_FILE)
        _require_cols(lsoa11, ["LSOA11CD", "LSOA11NM"], LSOA11_FILE.name)
    except FileNotFoundError:
        logging.warning("LSOA 2011 lookup not found. Skipping LSOA2011 merge.")
    except ValueError as e:
        logging.warning("LSOA 2011 lookup missing required columns (%s). Skipping LSOA2011 merge.", e)

    # Merge the lookup files to add the human-readable names
    logging.info("Merging lookups into ONS slice...")
    merged = (
        ons
        .merge(region[["NHSER24CDH", "NHSER24NM"]], left_on="NHSER",  right_on="NHSER24CDH", how="left")
        .merge(icb[["ICB23CDH", "ICB23NM"]],       left_on="ICB",    right_on="ICB23CDH",   how="left")
        .merge(msoa[["MSOA21CD", "MSOA21NM"]],    left_on="MSOA21", right_on="MSOA21CD",  how="left")
        .merge(lsoa[["LSOA21CD", "LSOA21NM"]],    left_on="LSOA21", right_on="LSOA21CD",  how="left")
        .merge(country[["CTRY12CD", "CTRY12NM"]], left_on="CTRY",   right_on="CTRY12CD",  how="left")
    )

    # NEW: Optional merge for LSOA 2011 names if we have both the lookup and a join key in ONS
    if lsoa11 is not None and lsoa11_key_in_ons is not None:
        logging.info("Merging LSOA 2011 names using ONS key: %s", lsoa11_key_in_ons)
        merged = merged.merge(
            lsoa11[["LSOA11CD", "LSOA11NM"]],
            left_on=lsoa11_key_in_ons,
            right_on="LSOA11CD",
            how="left"
        )
    else:
        if lsoa11 is None:
            logging.warning("Skipping LSOA 2011 merge: lookup unavailable.")
        elif lsoa11_key_in_ons is None:
            logging.warning("Skipping LSOA 2011 merge: ONS_postcode.csv lacks LSOA11/LSOA11CD column.")

    # Rename columns to the final desired names and set the final column order
    logging.info("Renaming and reordering columns for final output...")
    rename_map = {
        "NHSER":     "RegionCode",
        "NHSER24NM": "RegionName",
        "ICB":       "ICBID",
        "ICB23NM":   "ICBName",
        "LSOA21":    "LSOAID",
        "LSOA21NM":  "LSOAName",
        "MSOA21":    "MSOAID",
        "MSOA21NM":  "MSOAName",
        "CTRY12NM":  "CountryName",
        # NEW
        "LSOA11CD":  "LSOA11ID",
        "LSOA11NM":  "LSOA11Name",
    }

    # Build final column list dynamically to handle optional LSOA2011
    final_cols = [
        "PCD2", "PCDS",
        "RegionCode", "RegionName",
        "ICBID", "ICBName",
        "MSOAID", "MSOAName",
        "LSOAID", "LSOAName",
    ]

    # Include LSOA 2011 columns if present after merge
    if "LSOA11CD" in merged.columns and "LSOA11NM" in merged.columns:
        final_cols += ["LSOA11ID", "LSOA11Name"]

    final_cols += ["CountryName"]

    final = merged.rename(columns=rename_map)[final_cols]

    # --- Quality Assurance Summary ---
    logging.info("--- QA Summary ---")
    logging.info("Total rows processed: %d", len(final))
    logging.info("Rows with missing RegionName: %d", final['RegionName'].isna().sum())
    logging.info("Rows with missing ICBName: %d", final['ICBName'].isna().sum())
    logging.info("Rows with missing MSOAName: %d", final['MSOAName'].isna().sum())
    logging.info("Rows with missing LSOAName: %d", final['LSOAName'].isna().sum())
    if "LSOA11Name" in final.columns:
        logging.info("Rows with missing LSOA11Name: %d", final['LSOA11Name'].isna().sum())
    logging.info("Rows with missing CountryName: %d", final['CountryName'].isna().sum())

    # Write the final dataframe to a CSV file
    final.to_csv(OUT_FILE, index=False)
    logging.info("--- Process Complete ---")
    logging.info("Wrote final output to: %s", OUT_FILE)


if __name__ == "__main__":
    main()
