r"""
HealthRadar — Process GP Workforce Data by Age (Wide Format Time Series)

This script reads all detailed, practice-level General Practice Workforce CSVs
from a specified folder. It processes each file, extracts the year and month
from the filename, and combines them into a single wide-format output file.
This output retains the age bands as columns and adds a total GP headcount.

Input Folder:
    SRC_BASE/ (containing multiple monthly files like '...July 2023...')

Output File:
    OUT_BASE/GPHeadcountAge.csv

Example Output Columns:
    YearMonth, PracticeCode, <25, 25-29, 30-34, ..., 75+, Unknown, TotalGP
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List

import pandas as pd


# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
SRC_BASE = Path(r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\sourcedata\Practitionercountbyage")
OUT_BASE = Path(r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\processed")
OUT_BASE.mkdir(parents=True, exist_ok=True)

# --- The output is now a single, combined file ---
OUT_FILE = OUT_BASE / "GPHeadcountAge.csv"


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
    df = pd.read_csv(path, dtype=str, low_memory=False, usecols=usecols, encoding='iso-8859-1')
    df = df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c)
    for c in df.columns:
        if df[c].dtype == 'object':
            df[c] = df[c].str.strip()
    return df


def _extract_year_month_from_filename(filename: str) -> str | None:
    """Extracts YYYYMM from filenames like '...July 2023...'."""
    month_map = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04',
        'May': '05', 'June': '06', 'July': '07', 'August': '08',
        'September': '09', 'October': '10', 'November': '11', 'December': '12'
    }
    match = re.search(r'([A-Za-z]+)\s(\d{4})', filename)
    if match:
        month_name, year = match.groups()
        month_num = month_map.get(month_name.title())
        if month_num:
            return f"{year}{month_num}"
    logging.warning("Could not extract YearMonth from filename: %s", filename)
    return None


# ------------------------------------------------------------
# Main Script Logic
# ------------------------------------------------------------
def main() -> None:
    """Main function to find, process, and combine all workforce files into a single wide format."""
    logging.info("--- Starting GP Workforce Age Analysis (Wide Format) ---")

    source_files = list(SRC_BASE.glob("*.csv"))
    if not source_files:
        logging.error("No CSV files found in directory: %s", SRC_BASE)
        return

    logging.info("Found %d files to process.", len(source_files))
    all_data = []

    for file_path in source_files:
        logging.info("Processing file: %s", file_path.name)
        
        year_month = _extract_year_month_from_filename(file_path.name)
        if not year_month:
            continue
            
        try:
            actual_columns = pd.read_csv(file_path, nrows=0, encoding='iso-8859-1').columns.tolist()
            actual_columns = [c.strip() for c in actual_columns]
        except Exception as e:
            logging.error("Could not read columns from %s. Error: %s", file_path.name, e)
            continue

        # --- FIX: More robust check for the practice code column ---
        # Looks for any column that contains both "PRAC" and "CODE", case-insensitively.
        practice_col_name = None
        for col in actual_columns:
            if "PRAC" in col.upper() and "CODE" in col.upper():
                practice_col_name = col
                break
        
        if not practice_col_name:
            logging.warning("File %s is missing a recognizable practice code column. Skipping.", file_path.name)
            continue

        age_cols_in_this_file = [col for col in actual_columns if col.startswith('TOTAL_GP_HC_')]
            
        if not age_cols_in_this_file:
            logging.warning("No GP age headcount columns (e.g., TOTAL_GP_HC_...) found in %s. Skipping.", file_path.name)
            continue

        cols_to_load = [practice_col_name] + age_cols_in_this_file
        df = _read_csv(file_path, usecols=cols_to_load)
        
        for col in age_cols_in_this_file:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
        df['TotalGP'] = df[age_cols_in_this_file].sum(axis=1)
        
        df['YearMonth'] = year_month

        rename_map = {practice_col_name: 'PracticeCode'}
        for col in age_cols_in_this_file:
            clean_name = col.replace('TOTAL_GP_HC_', '').replace('_', '-').replace('U25', '<25').replace('75OV', '75+').replace('UNKNOWN', 'Unknown')
            rename_map[col] = clean_name
            
        df.rename(columns=rename_map, inplace=True)
        all_data.append(df)

    if not all_data:
        logging.error("No data was successfully processed from any file.")
        return

    logging.info("Combining data from all processed files...")
    final_df = pd.concat(all_data, ignore_index=True)

    all_clean_age_cols = sorted(list(set(final_df.columns) - {'YearMonth', 'PracticeCode', 'TotalGP'}))
    
    final_cols = ['YearMonth', 'PracticeCode'] + all_clean_age_cols + ['TotalGP']
    final_df = final_df[final_cols]
    
    final_df.fillna(0, inplace=True)

    final_df.to_csv(OUT_FILE, index=False)
    logging.info("Wrote combined output to: %s", OUT_FILE)
    logging.info("--- Process Complete ---")


if __name__ == "__main__":
    main()
