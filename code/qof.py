#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
QOF 2023/24 – Extract indicative patients at practice level
- Input: PREVALENCE_2324.csv
- Mapping: MAPPING_INDICATORS_2324.csv (GROUP_CODE -> GROUP_DESCRIPTION)
- Output: QOF_IndicativePatients_2324_practice.csv in 'processed' folder
"""

from pathlib import Path
import pandas as pd

# ------------------ CONFIG ------------------
SRC_BASE = Path(r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\sourcedata\QualityandOutcomesFramework")
OUT_BASE = Path(r"C:\Users\patta\OneDrive\DBA_Walsh\Term3\capstone\HealthRadar_Depreviation\processed")

INPUT_FILE = SRC_BASE / "PREVALENCE_2324.csv"
MAP_FILE   = SRC_BASE / "MAPPING_INDICATORS_2324.csv"
OUT_FILE   = OUT_BASE / "QOF_IndicativePatients_2324_practice.csv"
# --------------------------------------------

def main():
    # Load prevalence data
    df = pd.read_csv(INPUT_FILE, dtype=str, low_memory=False)
    df = df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c)

    # Filter to TOTAL list type only
    df = df[df["PATIENT_LIST_TYPE"].str.strip().str.upper() == "TOTAL"].copy()

    # Keep key columns
    df = df[["PRACTICE_CODE", "GROUP_CODE", "REGISTER"]].copy()
    df = df.rename(columns={"REGISTER": "IndicativePatients23_24"})

    # Load mapping file
    mapping = pd.read_csv(MAP_FILE, dtype=str, low_memory=False)
    mapping = mapping.rename(columns=lambda c: c.strip() if isinstance(c, str) else c)

    # Reduce to GROUP_CODE + GROUP_DESCRIPTION
    mapping = mapping[["GROUP_CODE", "GROUP_DESCRIPTION"]].drop_duplicates()
    mapping = mapping.rename(columns={"GROUP_DESCRIPTION": "DiseaseCategory"})

    # Merge
    df = df.merge(mapping, on="GROUP_CODE", how="left")

    # Save
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")

    print(f"✅ Wrote output: {OUT_FILE} (rows={len(df):,}, cols={df.shape[1]})")

if __name__ == "__main__":
    main()
