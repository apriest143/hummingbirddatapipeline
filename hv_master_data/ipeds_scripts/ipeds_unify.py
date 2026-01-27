#!/usr/bin/env python3
"""
IPEDS Unified Financial Metrics Script
Creates comparable financial metrics across institution types by mapping
GASB (public), FASB (private non-profit), and for-profit accounting standards
to unified column names.

Background:
    IPEDS reports financials using different accounting standards:
    - Public institutions: GASB (Governmental Accounting Standards Board)
    - Private Non-Profit: FASB (Financial Accounting Standards Board)  
    - Private For-Profit: Standard business accounting

    This script creates unified columns that pull from the appropriate source
    based on each institution's control type.

Input:
    - hv_master_data/data/IPEDS/IPEDS_trimmed_2022_2024.csv

Output:
    - hv_master_data/data/IPEDS/IPEDS_trimmed_unified_2022_2024.csv

New Unified Columns Created:
    - net_worth_{year}: Net position (public) / Total net assets (nonprofit) / Total equity (forprofit)
    - core_revenues_{year}: Core revenues from GASB or FASB
    - core_expenses_{year}: Core expenses from GASB or FASB
    - equity_ratio_{year}: Equity ratio from GASB or FASB
    - tuition_dependency_pct_{year}: Tuition as % of core revenues
    - endowment_per_fte_{year}: Endowment assets per FTE enrollment

Usage:
    python unify_ipeds_financials.py
"""

import pandas as pd
import numpy as np
from pathlib import Path


# =============================================================================
# FILE PATHS
# =============================================================================
INPUT_DIR = Path("hv_master_data/data/IPEDS")
OUTPUT_DIR = Path("hv_master_data/data/IPEDS")

INPUT_FILE = INPUT_DIR / "IPEDS_trimmed_2022_2024.csv"
OUTPUT_FILE = OUTPUT_DIR / "IPEDS_trimmed_unified_2022_2024.csv"


# =============================================================================
# CONFIGURATION
# =============================================================================

# Map IPEDS control values to simplified types
CONTROL_MAP = {
    'Public': 'public',
    'Private not-for-profit': 'nonprofit',
    'Private for-profit': 'forprofit'
}

# Years to process
YEARS = ['2024', '2023', '2022']

# Unified metrics mapping
# Format: (unified_name, public_source, nonprofit_source, forprofit_source)
# Use None if no source exists for that institution type
UNIFIED_METRICS = [
    # Net worth equivalents - the "bottom line" of the balance sheet
    (
        'net_worth',
        'Net position',           # GASB term for public
        'Total net assets',       # FASB term for nonprofit
        'Total equity'            # Business term for for-profit
    ),
    
    # Core revenues - primary operating revenues
    (
        'core_revenues',
        'Core revenues, total dollars (GASB)',
        'Core revenues, total dollars (FASB)',
        'Core revenues, total dollars (for-profit institutions)'
    ),
    
    # Core expenses - primary operating expenses  
    (
        'core_expenses',
        'Core expenses, total dollars (GASB)',
        'Core expenses, total dollars (FASB)',
        'Core expenses, total dollars (for-profit institutons)'  # Note: typo in original IPEDS
    ),
    
    # Equity ratio - financial leverage indicator (higher = less debt)
    (
        'equity_ratio',
        'Equity ratio (GASB)',
        'Equity ratio (FASB)',
        'Equity ratio (for-profit institutions)'
    ),
    
    # Tuition dependency - vulnerability indicator (higher = more dependent on tuition)
    (
        'tuition_dependency_pct',
        'Tuition and fees as a percent of core revenues (GASB)',
        'Tuition and fees as a percent of core revenues (FASB)',
        'Tuition and fees as a percent of core revenues (for-profit institutions)'
    ),
    
    # Endowment per FTE - financial cushion per student
    (
        'endowment_per_fte',
        'Endowment assets (year end) per FTE enrollment (GASB)',
        'Endowment assets (year end) per FTE enrollment (FASB)',
        None  # For-profits typically don't have endowments
    ),
    
    # Total revenues - all revenue sources combined
    (
        'total_revenues',
        'Total revenues and investment return',
        'Total revenues and investment return',
        'Total revenues'
    ),
    
    # Net income - annual surplus/deficit
    # For public/nonprofit this is "change in net assets/position"
    # For for-profit this is actual net income
    (
        'net_income',
        'Change in net position',
        'Change in net assets',
        'Net income'
    ),
]


# =============================================================================
# MAIN FUNCTIONS
# =============================================================================

def create_unified_metrics(df):
    """
    Create unified financial metrics by mapping institution-type-specific
    columns to common column names.
    
    Parameters:
    -----------
    df : pandas.DataFrame - IPEDS data with control_type column
    
    Returns:
    --------
    pandas.DataFrame - Data with new unified metric columns added
    """
    df = df.copy()
    
    # Ensure control_type column exists
    if 'control_type' not in df.columns:
        df['control_type'] = df['Control of institution'].map(CONTROL_MAP)
    
    created_cols = []
    
    for unified_name, public_col, nonprofit_col, forprofit_col in UNIFIED_METRICS:
        for year in YEARS:
            new_col = f"{unified_name}_{year}"
            
            # Initialize with NaN
            df[new_col] = np.nan
            
            # Public institutions
            if public_col:
                source_col = f"{public_col}_{year}"
                if source_col in df.columns:
                    mask = df['control_type'] == 'public'
                    df.loc[mask, new_col] = df.loc[mask, source_col]
            
            # Private non-profit
            if nonprofit_col:
                source_col = f"{nonprofit_col}_{year}"
                if source_col in df.columns:
                    mask = df['control_type'] == 'nonprofit'
                    df.loc[mask, new_col] = df.loc[mask, source_col]
            
            # For-profit
            if forprofit_col:
                source_col = f"{forprofit_col}_{year}"
                if source_col in df.columns:
                    mask = df['control_type'] == 'forprofit'
                    df.loc[mask, new_col] = df.loc[mask, source_col]
            
            created_cols.append(new_col)
    
    return df, created_cols


def print_coverage_report(df, unified_metrics=UNIFIED_METRICS, years=YEARS):
    """Print coverage statistics for unified metrics by institution type."""
    
    print("\n" + "=" * 60)
    print("UNIFIED METRICS COVERAGE BY INSTITUTION TYPE")
    print("=" * 60)
    
    for unified_name, _, _, _ in unified_metrics:
        print(f"\n{unified_name}:")
        for year in years:
            col = f"{unified_name}_{year}"
            if col not in df.columns:
                continue
            for ctype, label in [('public', 'Public'), 
                                  ('nonprofit', 'Non-Profit'), 
                                  ('forprofit', 'For-Profit')]:
                subset = df[df['control_type'] == ctype]
                valid = subset[col].notna().sum()
                total = len(subset)
                pct = valid/total*100 if total > 0 else 0
                print(f"   {year} {label:12}: {valid:,}/{total:,} ({pct:.0f}%)")


def main():
    print("=" * 60)
    print("IPEDS Unified Financial Metrics")
    print("=" * 60)
    
    # Load data
    print(f"\nLoading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    print(f"  Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    
    # Create control type mapping
    df['control_type'] = df['Control of institution'].map(CONTROL_MAP)
    
    print(f"\nInstitutions by type:")
    for ctype, count in df['control_type'].value_counts().items():
        print(f"  {ctype}: {count:,}")
    
    # Create unified metrics
    print(f"\nCreating unified metrics...")
    df, created_cols = create_unified_metrics(df)
    print(f"  Created {len(created_cols)} new columns")
    
    # Print coverage report
    print_coverage_report(df)
    
    # Save
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Saved: {OUTPUT_FILE}")
    print(f"   Final shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    
    return df


if __name__ == '__main__':
    main()  