r"""
HealthRadar — Extract Health Deprivation Data from IMD 2019

This script reads the comprehensive IMD 2019 file (which contains all ranks 
and scores) and extracts only the columns related to the Health Deprivation 
and Disability Domain.

Input:
- SRC_BASE/imd2019_file7_all_ranks_scores.csv

Output:
- OUT_BASE/Health_Deprivation_2019.csv

Final Columns:
- LSOA_Code, Health_Score, Health_Rank, Health_Decile
"""

from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd

# ------------------------------------------------------------
# Paths Configuration
# ------------------------------------------------------------
SRC_BASE = Path(r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\sourcedata\IMDData2019")
OUT_BASE = Path(r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\processed")
OUT_BASE.mkdir(parents=True, exist_ok=True)

INPUT_FILE = SRC_BASE / "File_7_-_All_IoD2019_Scores__Ranks__Deciles_and_Population_Denominators_3.csv"
OUT_FILE   = OUT_BASE / "Health_Deprivation_2019.csv"

# ------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# ------------------------------------------------------------
# Column Definitions & Utilities
# ------------------------------------------------------------

# A dictionary of target column names and a list of possible source names.
# This makes the script robust to small changes in the CSV header.
COL_CANDIDATES = {
    "LSOA_Code": [
        "LSOA code (2011)",
        "LSOA code",
        "LSOA Code (2011)",
        "LSOA Code",
    ],
    "Health_Score": [
        "Health Deprivation and Disability Score",
        "Health Deprivation & Disability Score",
        "Health Deprivation/Disability Score"
    ],
    "Health_Rank": [
        "Health Deprivation and Disability Rank (where 1 is most deprived)",
        "Health Deprivation & Disability Rank (where 1 is most deprived)",
        "Health Deprivation/Disability Rank (where 1 is most deprived)"
    ],
    "Health_Decile": [
        "Health Deprivation and Disability Decile (where 1 is most deprived 10% of LSOAs)",
        "Health Deprivation & Disability Decile (where 1 is most deprived 10% of LSOAs)",
        "Health Deprivation/Disability Decile (where 1 is most deprived 10% of LSOAs)"
    ]
}

def find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Finds the first matching column name from a list of candidates."""
    actual_cols = df.columns.tolist()
    for candidate in candidates:
        if candidate in actual_cols:
            return candidate
    
    # If no exact match, try a case-insensitive search
    lower_map = {col.lower().strip(): col for col in actual_cols}
    for candidate in candidates:
        if candidate.lower().strip() in lower_map:
            return lower_map[candidate.lower().strip()]
            
    return None

# ------------------------------------------------------------
# Main Script Logic
# ------------------------------------------------------------
def main():
    """Main function to execute the data extraction."""
    logging.info("--- Starting IMD 2019 Health Data Extraction ---")

    if not INPUT_FILE.exists():
        logging.error("Input file not found: %s", INPUT_FILE)
        return

    logging.info("Loading IMD data from: %s", INPUT_FILE)
    try:
        df = pd.read_csv(INPUT_FILE, dtype=str, low_memory=False)
    except UnicodeDecodeError:
        # Some ONS/IMD files may be ISO-8859-1 (latin1) encoded
        logging.warning("UTF-8 decoding failed. Trying latin1 encoding.")
        df = pd.read_csv(INPUT_FILE, dtype=str, encoding="latin1", low_memory=False)

    # Clean column names by stripping whitespace
    df = df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c)

    # Resolve the actual column names in the file
    resolved_cols = {}
    all_found = True
    for target, candidates in COL_CANDIDATES.items():
        found_col = find_column(df, candidates)
        if not found_col:
            logging.error("Could not find a source column for target '%s'.", target)
            all_found = False
        else:
            resolved_cols[target] = found_col
            logging.info("Found mapping: '%s' -> '%s'", target, found_col)

    if all_found:
        # Select and rename the columns to our standard names
        final_df = df[[
            resolved_cols["LSOA_Code"],
            resolved_cols["Health_Score"],
            resolved_cols["Health_Rank"],
            resolved_cols["Health_Decile"]
        ]].copy()
        
        final_df.columns = ["LSOA_Code", "Health_Score", "Health_Rank", "Health_Decile"]

        # Save the final, cleaned dataframe
        final_df.to_csv(OUT_FILE, index=False)
        logging.info("--- Process Complete ---")
        logging.info("Wrote %d rows to: %s", len(final_df), OUT_FILE)

if __name__ == "__main__":
    main()
