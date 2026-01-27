"""
IPEDS Multi-Year Merge Script
Merges IPEDS data from 2022, 2023, and 2024 into a single wide-format file.

Structure
- Static columns (institution name, address, etc.) from 2024 only
- Time-varying metrics with year suffixes (_2024, _2023, _2022)



Output:
    - hv_master_data/data/IPEDS/IPEDS_merged_2022_2024.csv
"""

import pandas as pd
import re
import numpy as np
from pathlib import Path

# =============================================================================
# FILE PATHS - Adjust these as needed
# =============================================================================
INPUT_DIR = Path("hv_master_data/data/IPEDS")
OUTPUT_DIR = Path("hv_master_data/data/IPEDS")

INPUT_FILES = {
    2024: INPUT_DIR / "IPEDS24.csv",
    2023: INPUT_DIR / "IPEDS23.csv",
    2022: INPUT_DIR / "IPEDS22.csv",
}

OUTPUT_FILE = OUTPUT_DIR / "IPEDS_merged_2022_2024.csv"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def is_static_column(col_name):
    """Check if column is a static identifier (doesn't change year-to-year)"""
    static_patterns = [
        'institution name', 'street address', 'city location', 
        'state abbreviation', 'zip code', 'telephone', 'employer identification',
        'unique entity', 'county name', 'longitude', 'latitude', 
        'sector of institution', 'land grant', 'control of institution', 
        'institutional control', 'urbanization', 'alias'
    ]
    col_lower = col_name.lower()
    if col_name in ['unitid', 'institution name', 'year', 'institution name.1']:
        return True
    return any(p in col_lower for p in static_patterns)


def convert_column_name(col_2024, target_year):
    """Convert 2024 column name to equivalent for another year"""
    col = col_2024
    
    if target_year == 2023:
        # Handle DRVCOST → DRVIC prefix change first (before year replacement)
        col = col.replace('DRVCOST2024', 'DRVIC2023')
        # Then do standard year replacements
        col = col.replace('2024', '2023').replace('2324', '2223')
        # Tuition academic years: 2024-25 → 2022-23, 2023-24 → 2021-22
        col = col.replace('2023-25', '2022-23').replace('2023-24', '2021-22')
        col = col.replace('fall 2023', 'fall 2022')
    elif target_year == 2022:
        # Handle DRVCOST → DRVIC prefix change first
        col = col.replace('DRVCOST2024', 'DRVIC2022')
        # Then do standard year replacements
        col = col.replace('2024', '2022').replace('2324', '2122')
        # Tuition academic years: 2024-25 → 2021-22, 2023-24 → 2020-21
        col = col.replace('2022-25', '2021-22').replace('2022-24', '2020-21')
        col = col.replace('fall 2023', 'fall 2021')
    
    return col


def clean_column_name(col):
    """Remove year-specific prefixes for cleaner output column names"""
    # Remove table prefixes like HD2024., DRVEF2024., F2324_F2.
    clean = re.sub(r'^[A-Z]+2024[A-Z]?\.|^F2324_F[123][A-Z]?\.', '', col)
    # Remove .1 suffix from duplicates
    clean = re.sub(r'\.1$', '', clean)
    return clean if clean else col


def merge_ipeds_years(df24, df23, df22):
    """
    Merge three years of IPEDS data into a single wide-format DataFrame.
    
    Parameters:
    -----------
    df24 : pandas.DataFrame - 2024 IPEDS data
    df23 : pandas.DataFrame - 2023 IPEDS data
    df22 : pandas.DataFrame - 2022 IPEDS data
    
    Returns:
    --------
    pandas.DataFrame - Merged data with year suffixes
    """
    # Index 2023 and 2022 by unitid for efficient lookups
    df23_idx = df23.set_index('unitid')
    df22_idx = df22.set_index('unitid')
    
    # Filter columns - skip .1 duplicates that exist in the IPEDS exports
    static_cols = [c for c in df24.columns if is_static_column(c) and not c.endswith('.1')]
    time_cols = [c for c in df24.columns if not is_static_column(c) and not c.endswith('.1')]
    
    print(f"Static columns: {len(static_cols)}")
    print(f"Time-varying columns: {len(time_cols)}")
    
    # Build result using lists for efficiency
    result_data = []
    col_names = []
    
    # Add static columns (from 2024 only, with cleaned names)
    for col in static_cols:
        clean = clean_column_name(col)
        if clean not in col_names:
            col_names.append(clean)
            result_data.append(df24[col].values)
    
    # Add time-varying columns with year suffixes
    missing_23, missing_22 = [], []
    
    for col_24 in time_cols:
        col_23 = convert_column_name(col_24, 2023)
        col_22 = convert_column_name(col_24, 2022)
        clean = clean_column_name(col_24)
        
        # 2024 data
        col_names.append(f"{clean}_2024")
        result_data.append(df24[col_24].values)
        
        # 2023 data
        col_names.append(f"{clean}_2023")
        if col_23 in df23_idx.columns:
            result_data.append(df24['unitid'].map(df23_idx[col_23]).values)
        else:
            result_data.append(np.full(len(df24), np.nan))
            missing_23.append(col_23)
        
        # 2022 data
        col_names.append(f"{clean}_2022")
        if col_22 in df22_idx.columns:
            result_data.append(df24['unitid'].map(df22_idx[col_22]).values)
        else:
            result_data.append(np.full(len(df24), np.nan))
            missing_22.append(col_22)
    
    print(f"\nColumns not found in 2023: {len(missing_23)}")
    if missing_23:
        for col in missing_23:
            print(f"  - {col}")
    
    print(f"\nColumns not found in 2022: {len(missing_22)}")
    if missing_22:
        for col in missing_22:
            print(f"  - {col}")
    
    # Create DataFrame
    merged = pd.DataFrame(dict(zip(col_names, result_data)))
    
    return merged


def main():
    print("=" * 60)
    print("IPEDS Multi-Year Merge")
    print("=" * 60)
    
    # Load files
    print("\nLoading IPEDS files...")
    df24 = pd.read_csv(INPUT_FILES[2024], low_memory=False)
    df23 = pd.read_csv(INPUT_FILES[2023], low_memory=False)
    df22 = pd.read_csv(INPUT_FILES[2022], low_memory=False)
    
    print(f"  2024: {len(df24):,} institutions, {len(df24.columns)} columns")
    print(f"  2023: {len(df23):,} institutions, {len(df23.columns)} columns")
    print(f"  2022: {len(df22):,} institutions, {len(df22.columns)} columns")
    
    # Merge
    print("\nMerging...")
    merged = merge_ipeds_years(df24, df23, df22)
    
    print(f"\nFinal merged shape: {merged.shape[0]:,} rows × {merged.shape[1]} columns")
    
    # Summary
    static = [c for c in merged.columns if not c.endswith(('_2024', '_2023', '_2022'))]
    cols_24 = [c for c in merged.columns if c.endswith('_2024')]
    cols_23 = [c for c in merged.columns if c.endswith('_2023')]
    cols_22 = [c for c in merged.columns if c.endswith('_2022')]
    
    print(f"\nColumn breakdown:")
    print(f"  Static (institutional info): {len(static)}")
    print(f"  2024 metrics: {len(cols_24)}")
    print(f"  2023 metrics: {len(cols_23)}")
    print(f"  2022 metrics: {len(cols_22)}")
    
    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save
    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved: {OUTPUT_FILE}")
    
    return merged


if __name__ == '__main__':
    main()