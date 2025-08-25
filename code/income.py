#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Clean/normalize LSOA income column names.

Inputs (auto-detected by extension):
  - CSV or XLSX with columns like:
    'LSOA code','LSOA name','10th percentile (£)', ... '90th percentile (£)',
    'Percentage with income information (%)'

Outputs:
  - CSV with normalized column names:
    lsoa_code, lsoa_name, p10_income, p20_income, ..., p90_income, pct_income_info
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
import re

# ------------------- CONFIG: update if needed -------------------
SRC = Path(r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\sourcedata\IncomeData\income_lsoa.csv")
OUT = Path(r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\processed\income_lsoa_clean.csv")
# If your input is an Excel workbook, set SRC to .xlsx. Optionally set SHEET_NAME:
SHEET_NAME: str | int | None = None  # e.g., "Table 1" or 0
# ---------------------------------------------------------------

def _normalize(text: str) -> str:
    """Generic header normalization: strip, lowercase, non-alnum -> underscore."""
    s = re.sub(r"\s+", " ", text.strip())
    s = s.lower()
    s = re.sub(r"[^0-9a-z]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def _canonical_rename(col: str) -> str:
    """
    Map known headers to clean, semantic names.
    - Percentiles -> p10_income ... p90_income
    - Coverage -> pct_income_info
    - LSOA code/name -> lsoa_code / lsoa_name
    """
    raw = col.strip()

    # LSOA identifiers
    if re.fullmatch(r"lsoa\s*code", raw, flags=re.I):
        return "lsoa_code"
    if re.fullmatch(r"lsoa\s*name", raw, flags=re.I):
        return "lsoa_name"

    # Percentiles like '10th percentile (£)'
    m = re.match(r"^\s*(\d{2})\s*(st|nd|rd|th)\s*percentile\b", raw, flags=re.I)
    if m:
        p = m.group(1)  # e.g., '10'
        return f"p{p}_income"

    # Coverage column
    if re.search(r"percentage\s+with\s+income\s+information", raw, flags=re.I):
        return "pct_income_info"

    # Fallback: normalized generic
    return _normalize(raw)

def load_any(path: Path, sheet_name: str | int | None = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input not found: {path}")
    # Excel files avoid encoding issues entirely
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str, sheet_name=sheet_name)

    # CSV: try UTF-8 (with BOM), then strict UTF-8, then cp1252 fallback
    try:
        return pd.read_csv(path, dtype=str, low_memory=False, encoding="utf-8-sig")
    except UnicodeDecodeError:
        try:
            return pd.read_csv(path, dtype=str, low_memory=False, encoding="utf-8")
        except UnicodeDecodeError:
            # Most ONS/UK gov CSVs with £ use cp1252/latin1
            return pd.read_csv(path, dtype=str, low_memory=False, encoding="cp1252")


def main():
    df = load_any(SRC, SHEET_NAME)

    # Clean whitespace in headers first
    df = df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c)

    # Apply canonical renames
    new_cols = []
    seen = set()
    for c in df.columns:
        nc = _canonical_rename(str(c))
        # Ensure uniqueness if duplicates arise
        if nc in seen:
            k = 2
            base = nc
            while f"{base}_{k}" in seen:
                k += 1
            nc = f"{base}_{k}"
        seen.add(nc)
        new_cols.append(nc)
    df.columns = new_cols

    # Write out
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False, encoding="utf-8-sig")
    print(f"✅ Wrote cleaned file: {OUT}")
    print("✅ Columns:", ", ".join(df.columns))

if __name__ == "__main__":
    main()
