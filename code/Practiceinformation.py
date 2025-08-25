#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
GP -> ONS mapping with England filter (CountryName from mapping)
- Reads GP.csv and ONSRegionalMapping.csv
- Cleans/normalizes postcodes
- Merges by full postcode, falls back to outcode
- Filters to England AFTER merge using mapping.CountryName
- Writes final CSV

Usage:
  python Practiceinformation.py \
    --gp "C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\sourcedata\Practice_Doctor_Dimension\GP.csv" \
    --map "C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\processed\ONSRegionalMapping.csv" \
    --out "C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\processed\GP_with_region_mapping_england.csv"
"""

import argparse
import logging
import sys
import re
from pathlib import Path

import numpy as np
import pandas as pd


# ----------------------------- Logging ---------------------------------
def setup_logger(verbosity: int = 1) -> logging.Logger:
    """
    verbosity: 0=WARNING, 1=INFO, 2=DEBUG
    """
    level = logging.WARNING if verbosity <= 0 else logging.INFO if verbosity == 1 else logging.DEBUG
    logger = logging.getLogger("gp_ons_mapping")
    logger.setLevel(level)
    if not logger.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        ch.setFormatter(fmt)
        logger.addHandler(ch)
    return logger


# ----------------------------- Helpers ---------------------------------
def format_uk_postcode(pc: str) -> str:
    """
    Normalize UK postcode to 'OUTCODE INCODE' (upper, single space).
    If value isn't a string, returns NaN.
    """
    if not isinstance(pc, str):
        return np.nan
    s = re.sub(r"\s+", "", pc.upper())
    if len(s) >= 5:
        return s[:-3] + " " + s[-3:]
    return s


def first_nonnull(*vals):
    """Return the first non-null/non-empty string in vals."""
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return np.nan


def pick_col(df: pd.DataFrame, candidates):
    """
    Return the actual column name in df matching any of 'candidates'
    case-insensitively, ignoring extra spaces. Returns None if not found.
    """
    norm = {re.sub(r"\s+", " ", c.strip().lower()): c for c in df.columns}
    for cand in candidates:
        key = re.sub(r"\s+", " ", cand.strip().lower())
        if key in norm:
            return norm[key]
    return None


def build_address(*parts):
    """Join non-empty address parts with single spaces; collapse extra spaces."""
    cleaned = [p.strip() for p in parts if isinstance(p, str) and p.strip()]
    return re.sub(r"\s+", " ", " ".join(cleaned)).strip() if cleaned else np.nan


# ----------------------------- Core Logic ------------------------------
def run(gp_file: Path, map_file: Path, out_file: Path, logger: logging.Logger):
    logger.info("Loading GP file: %s", gp_file)
    gp_raw = pd.read_csv(gp_file, dtype=str, keep_default_na=False, na_values=["", "NA", "NaN"])
    logger.info("GP rows: %s | cols: %s", f"{len(gp_raw):,}", f"{len(gp_raw.columns)}")

    # GP column mapping
    want_map = {
        "PracticeCode": ["Organisation Code", "Organization Code", "Org Code", "ODS Code", "Practice Code"],
        "PracticeName": ["Name", "Practice Name"],
        "Addr1": ["Address Line 1", "Address1", "Address Line1"],
        "Addr2": ["Address Line 2", "Address2", "Address Line2"],
        "Addr3": ["Address Line 3", "Address3", "Address Line3"],
        "Addr4": ["Address Line 4", "Address4", "Address Line4"],
        "Addr5": ["Address Line 5", "Address5", "Address Line5", "Address Line"],
        "PostCode": ["Postcode", "Post Code", "Postal Code", "PostCode"],
        "Status": ["Status"],
        "CountryName": ["CountryName", "Country", "Nation"],  # optional in GP
    }
    cols = {out: pick_col(gp_raw, cands) for out, cands in want_map.items()}
    required = ["PracticeCode", "PracticeName", "PostCode", "Status", "Addr1"]
    missing = [k for k in required if cols.get(k) is None]
    if missing:
        logger.error("Missing required columns in GP.csv for: %s", missing)
        logger.error("Column mapping: %s", cols)
        raise SystemExit(2)

    gp = pd.DataFrame({
        "PracticeCode": gp_raw[cols["PracticeCode"]].astype(str),
        "PracticeName": gp_raw[cols["PracticeName"]].astype(str),
        "PostCode": gp_raw[cols["PostCode"]].astype(str),
        "Status": gp_raw[cols["Status"]].astype(str) if cols["Status"] else np.nan,
        "Addr1": gp_raw[cols["Addr1"]].astype(str) if cols["Addr1"] else "",
        "Addr2": gp_raw[cols["Addr2"]].astype(str) if cols["Addr2"] else "",
        "Addr3": gp_raw[cols["Addr3"]].astype(str) if cols["Addr3"] else "",
        "Addr4": gp_raw[cols["Addr4"]].astype(str) if cols["Addr4"] else "",
        "Addr5": gp_raw[cols["Addr5"]].astype(str) if cols["Addr5"] else "",
    })

    gp["PostCode"] = gp["PostCode"].apply(format_uk_postcode)
    gp["Address"] = [
        build_address(a1, a2, a3, a4, a5, pc)
        for a1, a2, a3, a4, a5, pc in zip(gp["Addr1"], gp["Addr2"], gp["Addr3"], gp["Addr4"], gp["Addr5"], gp["PostCode"])
    ]
    gp_core = gp[["PracticeCode", "PracticeName", "Address", "PostCode", "Status"]].copy()
    logger.info("Sample GP rows:\n%s", gp_core.head(3).to_string(index=False))

    # Mapping file
    logger.info("Loading ONS mapping: %s", map_file)
    mapping = pd.read_csv(map_file, dtype=str, keep_default_na=False, na_values=["", "NA", "NaN"])
    logger.info("Mapping rows: %s | cols: %s", f"{len(mapping):,}", f"{len(mapping.columns)}")

    # Postcode columns
    PCD2_col = pick_col(mapping, ["PCD2", "Postcode", "PCD", "PCD7", "pcd2"])
    if not PCD2_col:
        logger.error("Could not find a postcode column (PCD2/Postcode) in mapping.")
        raise SystemExit(3)
    mapping["_PCD2_fmt"] = mapping[PCD2_col].apply(format_uk_postcode)

    OUTCODE_col = pick_col(mapping, ["Outcode", "OUTCODE", "outcode"])
    if not OUTCODE_col:
        mapping["_Outcode"] = mapping["_PCD2_fmt"].str.split().str[0]
        OUTCODE_col = "_Outcode"
    else:
        mapping[OUTCODE_col] = mapping[OUTCODE_col].str.upper().str.replace(r"\s+", "", regex=True)

    # Geo columns we want (includes LSOA11ID/LSOA11Name)
    name_map = {
        "RegionCode": ["RegionCode", "NHSER24CD", "NHSER24CDH", "REGION_CODE", "REGIONCODE"],
        "RegionName": ["RegionName", "NHSER24NM", "REGION_NAME", "REGIONNAME"],
        "ICBID":      ["ICBID", "ICB23CDH", "ICB24CD", "ICB_CODE", "ICB Code", "ICBIDCODE"],
        "ICBName":    ["ICBName", "ICB23NM", "ICB24NM", "ICB_NAME", "ICB Name"],
        "MSOAID":     ["MSOAID", "MSOA21CD", "MSOA11CD", "MSOACode"],
        "MSOAName":   ["MSOAName", "MSOA21NM", "MSOA11NM", "MSOANameText"],
        "LSOAID":     ["LSOAID", "LSOA21CD", "LSOACode"],
        "LSOAName":   ["LSOAName", "LSOA21NM", "LSOANameText"],
        "LSOA11ID":   ["LSOA11ID", "LSOA11CD"],     # <-- 2011 ID
        "LSOA11Name": ["LSOA11Name", "LSOA11NM"],   # <-- 2011 Name
        "CountryName": ["CountryName", "Country", "Nation"],
    }
    picked = {out: pick_col(mapping, cands) for out, cands in name_map.items()}
    logger.debug("Column mapping (ONS): %s", picked)

    keep_geo_cols = [c for c in picked.values() if c]
    map_cols = ["_PCD2_fmt", OUTCODE_col] + keep_geo_cols
    map_slim = mapping[map_cols].copy().rename(columns={OUTCODE_col: "_Outcode"})

    # Merge by full postcode
    logger.info("Merging by full postcode...")
    m1 = gp_core.merge(
        map_slim,
        how="left",
        left_on="PostCode",
        right_on="_PCD2_fmt",
        suffixes=("", "_map")
    )

    # Outcode fallback
    logger.info("Applying outcode fallback...")
    m1["_Outcode_gp"] = m1["PostCode"].str.split().str[0]
    m2 = m1.merge(
        map_slim.drop(columns=["_PCD2_fmt"]).drop_duplicates("_Outcode"),
        how="left",
        left_on="_Outcode_gp",
        right_on="_Outcode",
        suffixes=("", "_oc")
    )

    # Coalesce postcode match vs outcode match
    for out_name, col_pc in picked.items():
        if not col_pc:
            m2[out_name] = np.nan
            continue
        col_oc = col_pc + "_oc" if (col_pc + "_oc") in m2.columns else None
        if col_oc:
            m2[out_name] = m2.apply(lambda r: first_nonnull(r.get(col_pc), r.get(col_oc)), axis=1)
        else:
            m2[out_name] = m2[col_pc]

    # England filter
    if picked.get("CountryName"):
        logger.info("Filtering to England using mapping.CountryName...")
        country_norm = m2["CountryName"].astype(str).str.strip().str.upper()
        mask_england = (country_norm == "ENGLAND")
        m2_eng = m2[mask_england].copy()
    else:
        logger.warning("CountryName not found in mapping; no country filter applied.")
        m2_eng = m2.copy()

    # Final output columns (includes LSOA11ID/LSOA11Name if present)
    final_cols = [
        "PracticeCode", "PracticeName", "Address", "PostCode", "Status",
        "RegionCode", "RegionName", "ICBID", "ICBName",
        "MSOAID", "MSOAName",
        "LSOAID", "LSOAName",
        "LSOA11ID", "LSOA11Name",
        "CountryName"
    ]
    final_cols = [c for c in final_cols if c in m2_eng.columns]
    out_df = m2_eng[final_cols].copy()

    # Summary
    logger.info("=== Match Summary (AFTER filtering to England) ===")
    logger.info("Rows in final output: %s", f"{len(out_df):,}")
    if "RegionCode" in out_df.columns:
        logger.info("Unmatched rows (RegionCode missing): %s", f"{int(out_df['RegionCode'].isna().sum()):,}")

    # Write
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_file, index=False, encoding="utf-8-sig")
    logger.info("Wrote output: %s (rows=%s, cols=%s)", out_file, f"{len(out_df):,}", f"{len(out_df.columns)}")


# ----------------------------- CLI -------------------------------------
def parse_args():
    d_gp = r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\sourcedata\Practice_Doctor_Dimension\GP.csv"
    d_map = r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\processed\ONSRegionalMapping.csv"
    d_out = r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\processed\GP_with_region_mapping_england.csv"

    p = argparse.ArgumentParser(description="GP -> ONS mapping (England filter after merge)")
    p.add_argument("--gp",  type=str, default=d_gp,  help="Path to GP.csv")
    p.add_argument("--map", type=str, default=d_map, help="Path to ONSRegionalMapping.csv")
    p.add_argument("--out", type=str, default=d_out, help="Path to write output CSV")
    p.add_argument("-v", "--verbose", action="count", default=1,
                   help="Verbosity: -v=INFO (default), -vv=DEBUG, none=WARNING")
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logger(args.verbose)
    try:
        run(Path(args.gp), Path(args.map), Path(args.out), logger)
    except Exception as e:
        logger.exception("Failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
