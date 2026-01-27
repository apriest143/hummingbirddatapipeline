#!/usr/bin/env python3
"""
IPEDS Data Trimming Script
Reduces the full merged IPEDS dataset to essential columns for distressed 
institution analysis with land potential.

Purpose:
    "Revitalize underutilized institutional assets—such as small colleges, 
    faith-based organizations, nonprofits, and Native American tribes—by 
    reimagining their business models and securing or structuring capital 
    for sustainable reinvention."

Input:
    - hv_master_data/data/IPEDS/IPEDS_merged_2022_2024.csv (598 columns)

Output:
    - hv_master_data/data/IPEDS/IPEDS_trimmed_2022_2024.csv (168 columns)

Usage:
    python trim_ipeds_data.py
"""

import pandas as pd
from pathlib import Path

# =============================================================================
# FILE PATHS
# =============================================================================
INPUT_DIR = Path("hv_master_data/data/IPEDS")
OUTPUT_DIR = Path("hv_master_data/data/IPEDS")

INPUT_FILE = INPUT_DIR / "IPEDS_merged_2022_2024.csv"
OUTPUT_FILE = OUTPUT_DIR / "IPEDS_trimmed_2022_2024.csv"


# =============================================================================
# COLUMN DEFINITIONS
# =============================================================================

# Static columns (institutional identifiers - no year suffix)
STATIC_COLUMNS = [
    'unitid',
    'institution name',
    'Institution name alias',
    'Employer Identification Number',
    'Unique Entity Identifier (UEI) Numbers',
    'Street address or post office box',
    'City location of institution',
    'State abbreviation',
    'ZIP code',
    'County name',
    'Latitude location of institution',
    'Longitude location of institution',
    'General information telephone number',
    'Sector of institution',
    'Control of institution',
    'Institutional control or affiliation',
    'Land Grant Institution',
    'Degree of urbanization (Urban-centric locale)',
]

# Time-varying columns (base names - script will add _2024, _2023, _2022 suffixes)
TIME_VARYING_COLUMNS = [
    # ----- STATUS & SIZE -----
    'Institution is active in current year',
    'Institution size category',
    'Multi-institution or multi-campus organization',
    
    # ----- ENROLLMENT (key distress indicators) -----
    'Total  enrollment',
    'Full-time enrollment',
    'Part-time enrollment',
    'Graduate enrollment',
    
    # ----- ADMISSIONS (demand indicators) -----
    'Applicants total',
    'Admissions total',
    'Enrolled total',
    'Percent admitted - total',
    'Admissions yield - total',
    
    # ----- STUDENT SUCCESS -----
    'Full-time retention rate, 2024',
    'Graduation rate, total cohort',
    'Transfer-out rate, total cohort',
    'Student-to-faculty ratio',
    
    # ----- STAFFING -----
    'Total FTE staff',
    
    # ----- FINANCIALS: BALANCE SHEET -----
    'Total assets',
    'Total liabilities',
    'Total net assets',
    'Net position',
    'Total unrestricted net assets',
    'Expendable net assets',
    'Long-term debt',
    'Total equity',
    
    # ----- FINANCIALS: OPERATIONS -----
    'Total revenues and investment return',
    'Total expenses',
    'Change in net assets',
    'Change in net position',
    'Core revenues, total dollars (GASB)',
    'Core revenues, total dollars (FASB)',
    'Core expenses, total dollars (GASB)',
    'Core expenses, total dollars (FASB)',
    
    # ----- FINANCIALS: RATIOS -----
    'Equity ratio (GASB)',
    'Equity ratio (FASB)',
    'Tuition and fees as a percent of core revenues (GASB)',
    'Tuition and fees as a percent of core revenues (FASB)',
    
    # ----- PROPERTY & LAND (key for land potential analysis) -----
    'Total Plant, Property, and Equipment',
    'Total for plant, property and equipment - Ending balance',
    'Land  improvements - End of year',
    'Land  improvements - Ending balance',
    'Land and land improvements',
    'Buildings - End of year',
    'Buildings - Ending balance',
    'Buildings',
    
    # ----- ENDOWMENT -----
    'Value of endowment assets at the end of the fiscal year',
    'Endowment assets (year end) per FTE enrollment (GASB)',
    'Endowment assets (year end) per FTE enrollment (FASB)',
    
    # ----- TUITION & COSTS -----
    'Tuition and fees, 2024-25',
    'Tuition and fees, 2023-24',
    
    # ----- FOR-PROFIT SPECIFIC FINANCIALS -----
    # (These use different accounting standards than public/nonprofit)
    'Core revenues, total dollars (for-profit institutions)',
    'Core expenses, total dollars (for-profit institutons)',  # Note: typo in original IPEDS
    'Equity ratio (for-profit institutions)',
    'Tuition and fees as a percent of core revenues (for-profit institutions)',
    'Total revenues',
    'Net income',
    'Pretax income',
    'Adjusted equity',
    'Total liabilities and equity',
]

YEARS = ['_2024', '_2023', '_2022']


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def trim_ipeds_data(input_file, output_file):
    """
    Load the full merged IPEDS dataset and trim to essential columns.
    
    Parameters:
    -----------
    input_file : Path - Path to full merged IPEDS CSV
    output_file : Path - Path for trimmed output CSV
    
    Returns:
    --------
    pandas.DataFrame - Trimmed dataset
    """
    print("=" * 60)
    print("IPEDS Data Trimming")
    print("=" * 60)
    
    # Load data
    print(f"\nLoading: {input_file}")
    df = pd.read_csv(input_file, low_memory=False)
    print(f"  Original shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    
    # Build list of columns to keep
    columns_to_keep = []
    
    # Add static columns
    for col in STATIC_COLUMNS:
        if col in df.columns:
            columns_to_keep.append(col)
        else:
            print(f"  ⚠️  Static column not found: {col}")
    
    # Add time-varying columns with year suffixes
    for base in TIME_VARYING_COLUMNS:
        for year in YEARS:
            col = f"{base}{year}"
            if col in df.columns:
                columns_to_keep.append(col)
            # Don't warn for missing years - some metrics don't exist in all years
    
    # Create trimmed dataframe
    df_trimmed = df[columns_to_keep].copy()
    
    # Summary
    static_count = len([c for c in df_trimmed.columns 
                        if not c.endswith(('_2024', '_2023', '_2022'))])
    time_24 = len([c for c in df_trimmed.columns if c.endswith('_2024')])
    time_23 = len([c for c in df_trimmed.columns if c.endswith('_2023')])
    time_22 = len([c for c in df_trimmed.columns if c.endswith('_2022')])
    
    print(f"\n  Trimmed shape: {df_trimmed.shape[0]:,} rows × {df_trimmed.shape[1]} columns")
    print(f"  Reduction: {df.shape[1] - df_trimmed.shape[1]} columns removed "
          f"({(1 - df_trimmed.shape[1]/df.shape[1])*100:.0f}% smaller)")
    
    print(f"\n  Column breakdown:")
    print(f"    Static: {static_count}")
    print(f"    _2024:  {time_24}")
    print(f"    _2023:  {time_23}")
    print(f"    _2022:  {time_22}")
    
    # Save
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df_trimmed.to_csv(output_file, index=False)
    print(f"\n✅ Saved: {output_file}")
    
    return df_trimmed


def main():
    df_trimmed = trim_ipeds_data(INPUT_FILE, OUTPUT_FILE)
    
    # Quick validation
    print("\n" + "=" * 60)
    print("VALIDATION")
    print("=" * 60)
    
    # Check a sample institution
    sample = df_trimmed[df_trimmed['unitid'] == 100654]  # Alabama A&M
    if len(sample) > 0:
        row = sample.iloc[0]
        print(f"\nSample: {row['institution name']}")
        print(f"  State: {row['State abbreviation']}")
        print(f"  Sector: {row['Sector of institution']}")
        print(f"  Enrollment 2024: {row['Total  enrollment_2024']:,.0f}")
        print(f"  Enrollment 2022: {row['Total  enrollment_2022']:,.0f}")
        print(f"  Land Grant: {row['Land Grant Institution']}")
    
    return df_trimmed


if __name__ == '__main__':
    main()