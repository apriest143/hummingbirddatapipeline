#!/usr/bin/env python3
"""
IPEDS Financial Analysis & Distress Scoring Script

Purpose:
    Analyze IPEDS data to identify financially distressed institutions
    with potential for asset revitalization. Calculates year-over-year
    changes, financial risk flags, and a composite distress score.

Mission Context:
    "Revitalize underutilized institutional assets—such as small colleges,
    faith-based organizations, nonprofits, and Native American tribes—by
    reimagining their business models and securing or structuring capital
    for sustainable reinvention."

Input:
    - hv_master_data/data/IPEDS/IPEDS_trimmed_unified_2022_2024.csv

Output:
    - hv_master_data/data/IPEDS/IPEDS_analyzed_2022_2024.csv

New Columns Created:
    YoY Changes:
        - enrollment_change_1yr, enrollment_change_2yr, enrollment_change_pct_1yr, enrollment_change_pct_2yr
        - net_worth_change_1yr, net_worth_change_2yr, net_worth_change_pct_1yr, net_worth_change_pct_2yr
        - core_revenues_change_pct_1yr, core_revenues_change_pct_2yr
        - applicants_change_pct_1yr, applicants_change_pct_2yr
    
    Risk Flags (1 = at risk, 0 = ok):
        - flag_enrollment_decline: Enrollment dropped >10% over 2 years
        - flag_low_retention: Retention rate below 60%
        - flag_low_yield: Admissions yield below 20%
        - flag_low_equity_ratio: Equity ratio below 30%
        - flag_negative_net_worth: Net worth is negative
        - flag_declining_net_worth: Net worth declined 2 consecutive years
        - flag_high_tuition_dependency: Tuition is >85% of core revenues
        - flag_operating_losses: Net income negative 2+ years
        - flag_high_debt: Debt to assets ratio above 70%
    
    Composite Scores:
        - distress_score: 0-100 (higher = more distressed)
        - distress_category: 'Critical', 'High', 'Moderate', 'Low', 'Healthy'
    
    Land Potential:
        - flag_land_potential: Rural/town location with property assets

Usage:
    python ipeds_analysis.py
"""

import pandas as pd
import numpy as np
from pathlib import Path


# =============================================================================
# FILE PATHS
# =============================================================================
INPUT_DIR = Path("hv_master_data/data/IPEDS")
OUTPUT_DIR = Path("hv_master_data/data/IPEDS")

INPUT_FILE = INPUT_DIR / "IPEDS_trimmed_unified_2022_2024.csv"
OUTPUT_FILE = OUTPUT_DIR / "IPEDS_analyzed_2022_2024.csv"


# =============================================================================
# CONFIGURATION - RISK THRESHOLDS
# =============================================================================

THRESHOLDS = {
    # Enrollment
    'enrollment_decline_2yr_pct': -10,  # >10% decline over 2 years is concerning
    
    # Student success
    'low_retention_rate': 60,  # Below 60% is concerning
    'low_yield_rate': 20,  # Below 20% yield is concerning
    
    # Financial health
    'low_equity_ratio': 30,  # Below 30% means high leverage
    'high_tuition_dependency': 85,  # Above 85% is vulnerable
    'high_debt_ratio': 70,  # Debt/Assets above 70% is concerning
}

# Distress score weights (should sum to 100)
DISTRESS_WEIGHTS = {
    'flag_enrollment_decline': 15,
    'flag_low_retention': 10,
    'flag_low_yield': 5,
    'flag_low_equity_ratio': 15,
    'flag_negative_net_worth': 20,
    'flag_declining_net_worth': 10,
    'flag_high_tuition_dependency': 10,
    'flag_operating_losses': 10,
    'flag_high_debt': 5,
}


# =============================================================================
# YOY CHANGE CALCULATIONS
# =============================================================================

def calculate_yoy_changes(df):
    """
    Calculate year-over-year changes for key metrics.
    
    Creates both absolute changes and percentage changes.
    """
    df = df.copy()
    
    # Enrollment changes
    df['enrollment_change_1yr'] = df['Total  enrollment_2024'] - df['Total  enrollment_2023']
    df['enrollment_change_2yr'] = df['Total  enrollment_2024'] - df['Total  enrollment_2022']
    df['enrollment_change_pct_1yr'] = (df['enrollment_change_1yr'] / df['Total  enrollment_2023'] * 100).round(2)
    df['enrollment_change_pct_2yr'] = (df['enrollment_change_2yr'] / df['Total  enrollment_2022'] * 100).round(2)
    
    # Net worth changes (unified metric)
    df['net_worth_change_1yr'] = df['net_worth_2024'] - df['net_worth_2023']
    df['net_worth_change_2yr'] = df['net_worth_2024'] - df['net_worth_2022']
    df['net_worth_change_pct_1yr'] = (df['net_worth_change_1yr'] / df['net_worth_2023'].abs() * 100).round(2)
    df['net_worth_change_pct_2yr'] = (df['net_worth_change_2yr'] / df['net_worth_2022'].abs() * 100).round(2)
    
    # Core revenues changes
    df['core_revenues_change_1yr'] = df['core_revenues_2024'] - df['core_revenues_2023']
    df['core_revenues_change_2yr'] = df['core_revenues_2024'] - df['core_revenues_2022']
    df['core_revenues_change_pct_1yr'] = (df['core_revenues_change_1yr'] / df['core_revenues_2023'] * 100).round(2)
    df['core_revenues_change_pct_2yr'] = (df['core_revenues_change_2yr'] / df['core_revenues_2022'] * 100).round(2)
    
    # Applicants changes (demand indicator)
    df['applicants_change_1yr'] = df['Applicants total_2024'] - df['Applicants total_2023']
    df['applicants_change_2yr'] = df['Applicants total_2024'] - df['Applicants total_2022']
    df['applicants_change_pct_1yr'] = (df['applicants_change_1yr'] / df['Applicants total_2023'] * 100).round(2)
    df['applicants_change_pct_2yr'] = (df['applicants_change_2yr'] / df['Applicants total_2022'] * 100).round(2)
    
    # Core expenses changes
    df['core_expenses_change_1yr'] = df['core_expenses_2024'] - df['core_expenses_2023']
    df['core_expenses_change_2yr'] = df['core_expenses_2024'] - df['core_expenses_2022']
    df['core_expenses_change_pct_1yr'] = (df['core_expenses_change_1yr'] / df['core_expenses_2023'] * 100).round(2)
    df['core_expenses_change_pct_2yr'] = (df['core_expenses_change_2yr'] / df['core_expenses_2022'] * 100).round(2)
    
    return df


# =============================================================================
# RISK FLAG CALCULATIONS
# =============================================================================

def calculate_risk_flags(df):
    """
    Calculate binary risk flags based on threshold values.
    
    All flags: 1 = at risk, 0 = ok, NaN = insufficient data
    """
    df = df.copy()
    
    # ----- ENROLLMENT DECLINE -----
    # Flag if enrollment declined >10% over 2 years
    df['flag_enrollment_decline'] = (
        df['enrollment_change_pct_2yr'] < THRESHOLDS['enrollment_decline_2yr_pct']
    ).astype(float)
    df.loc[df['enrollment_change_pct_2yr'].isna(), 'flag_enrollment_decline'] = np.nan
    
    # ----- LOW RETENTION -----
    # Flag if retention rate below threshold
    # Handle year-specific column naming
    retention_col = 'Full-time retention rate, 2024_2024'
    if retention_col in df.columns:
        df['flag_low_retention'] = (
            df[retention_col] < THRESHOLDS['low_retention_rate']
        ).astype(float)
        df.loc[df[retention_col].isna(), 'flag_low_retention'] = np.nan
    else:
        df['flag_low_retention'] = np.nan
    
    # ----- LOW YIELD -----
    # Flag if admissions yield below threshold
    yield_col = 'Admissions yield - total_2024'
    if yield_col in df.columns:
        df['flag_low_yield'] = (
            df[yield_col] < THRESHOLDS['low_yield_rate']
        ).astype(float)
        df.loc[df[yield_col].isna(), 'flag_low_yield'] = np.nan
    else:
        df['flag_low_yield'] = np.nan
    
    # ----- LOW EQUITY RATIO -----
    # Flag if equity ratio below threshold (high leverage)
    df['flag_low_equity_ratio'] = (
        df['equity_ratio_2024'] < THRESHOLDS['low_equity_ratio']
    ).astype(float)
    df.loc[df['equity_ratio_2024'].isna(), 'flag_low_equity_ratio'] = np.nan
    
    # ----- NEGATIVE NET WORTH -----
    # Flag if net worth is negative
    df['flag_negative_net_worth'] = (df['net_worth_2024'] < 0).astype(float)
    df.loc[df['net_worth_2024'].isna(), 'flag_negative_net_worth'] = np.nan
    
    # ----- DECLINING NET WORTH -----
    # Flag if net worth declined in both years (consistent decline)
    df['flag_declining_net_worth'] = (
        (df['net_worth_change_1yr'] < 0) & 
        ((df['net_worth_2023'] - df['net_worth_2022']) < 0)
    ).astype(float)
    # Need data for all 3 years
    mask = df['net_worth_2024'].isna() | df['net_worth_2023'].isna() | df['net_worth_2022'].isna()
    df.loc[mask, 'flag_declining_net_worth'] = np.nan
    
    # ----- HIGH TUITION DEPENDENCY -----
    # Flag if tuition is >85% of core revenues
    df['flag_high_tuition_dependency'] = (
        df['tuition_dependency_pct_2024'] > THRESHOLDS['high_tuition_dependency']
    ).astype(float)
    df.loc[df['tuition_dependency_pct_2024'].isna(), 'flag_high_tuition_dependency'] = np.nan
    
    # ----- OPERATING LOSSES -----
    # Flag if net income negative in 2+ of 3 years
    losses_2024 = (df['net_income_2024'] < 0).astype(float)
    losses_2023 = (df['net_income_2023'] < 0).astype(float)
    losses_2022 = (df['net_income_2022'] < 0).astype(float)
    df['years_with_losses'] = losses_2024 + losses_2023 + losses_2022
    df['flag_operating_losses'] = (df['years_with_losses'] >= 2).astype(float)
    # Need at least 2 years of data
    valid_years = df['net_income_2024'].notna().astype(int) + \
                  df['net_income_2023'].notna().astype(int) + \
                  df['net_income_2022'].notna().astype(int)
    df.loc[valid_years < 2, 'flag_operating_losses'] = np.nan
    
    # ----- HIGH DEBT RATIO -----
    # Calculate debt to assets ratio
    # Use Total liabilities / Total assets
    df['debt_to_assets_ratio'] = (df['Total liabilities_2024'] / df['Total assets_2024'] * 100).round(2)
    df['flag_high_debt'] = (
        df['debt_to_assets_ratio'] > THRESHOLDS['high_debt_ratio']
    ).astype(float)
    df.loc[df['debt_to_assets_ratio'].isna(), 'flag_high_debt'] = np.nan
    
    return df


# =============================================================================
# DISTRESS SCORE CALCULATION
# =============================================================================

def calculate_distress_score(df):
    """
    Calculate composite distress score (0-100) based on weighted risk flags.
    
    Higher score = more distressed
    """
    df = df.copy()
    
    # Calculate weighted score
    flag_cols = list(DISTRESS_WEIGHTS.keys())
    
    # Initialize score
    df['distress_score'] = 0.0
    df['distress_flags_available'] = 0
    df['distress_max_possible'] = 0
    
    for flag_col, weight in DISTRESS_WEIGHTS.items():
        if flag_col in df.columns:
            # Add weighted flag value where data exists
            has_data = df[flag_col].notna()
            df.loc[has_data, 'distress_score'] += df.loc[has_data, flag_col] * weight
            df.loc[has_data, 'distress_max_possible'] += weight
            df.loc[has_data, 'distress_flags_available'] += 1
    
    # Normalize to 0-100 scale based on available flags
    df['distress_score'] = (df['distress_score'] / df['distress_max_possible'] * 100).round(1)
    
    # Set to NaN if fewer than 3 flags available
    df.loc[df['distress_flags_available'] < 3, 'distress_score'] = np.nan
    
    # Categorize distress level
    df['distress_category'] = pd.cut(
        df['distress_score'],
        bins=[-np.inf, 20, 40, 60, 80, np.inf],
        labels=['Healthy', 'Low', 'Moderate', 'High', 'Critical']
    )
    
    return df


# =============================================================================
# LAND POTENTIAL FLAGS
# =============================================================================

def calculate_land_potential(df):
    """
    Flag institutions with potential land assets for redevelopment.
    
    Criteria:
    - Located in rural, town, or suburban area (more likely to have land)
    - Has property/plant assets recorded
    - Smaller enrollment (under 2,000) suggests excess capacity
    """
    df = df.copy()
    
    # Urbanization categories suggesting more land
    rural_categories = [
        'Rural: Fringe',
        'Rural: Distant',
        'Rural: Remote',
        'Town: Fringe',
        'Town: Distant',
        'Town: Remote',
        'Suburb: Large',
        'Suburb: Midsize',
        'Suburb: Small',
    ]
    
    urbanization_col = 'Degree of urbanization (Urban-centric locale)'
    
    # Has property assets
    property_cols = [
        'Total Plant, Property, and Equipment_2024',
        'Total for plant, property and equipment - Ending balance_2024',
        'Land  improvements - End of year_2024',
        'Land  improvements - Ending balance_2024',
        'Buildings - End of year_2024',
    ]
    
    df['has_property_data'] = False
    for col in property_cols:
        if col in df.columns:
            df['has_property_data'] = df['has_property_data'] | df[col].notna()
    
    # Land potential flag
    df['flag_rural_suburban'] = df[urbanization_col].isin(rural_categories).astype(int)
    df['flag_small_enrollment'] = (df['Total  enrollment_2024'] < 2000).astype(int)
    
    # Composite land potential: rural/suburban + has property data
    df['flag_land_potential'] = (
        (df['flag_rural_suburban'] == 1) & 
        (df['has_property_data'] == True)
    ).astype(int)
    
    # High land potential: rural/suburban + property + small + distressed
    df['flag_high_land_potential'] = (
        (df['flag_land_potential'] == 1) &
        (df['flag_small_enrollment'] == 1) &
        (df['distress_score'] >= 40)
    ).astype(int)
    
    return df


# =============================================================================
# SUMMARY STATISTICS
# =============================================================================

def print_analysis_summary(df):
    """Print summary statistics of the analysis."""
    
    print("\n" + "=" * 60)
    print("ANALYSIS SUMMARY")
    print("=" * 60)
    
    # Distress distribution
    print("\n📊 DISTRESS SCORE DISTRIBUTION:")
    if 'distress_category' in df.columns:
        dist = df['distress_category'].value_counts().sort_index()
        for cat, count in dist.items():
            pct = count / len(df) * 100
            print(f"   {cat}: {count:,} ({pct:.1f}%)")
    
    # Risk flag summary
    print("\n🚩 RISK FLAG SUMMARY:")
    flag_cols = [c for c in df.columns if c.startswith('flag_') and c not in 
                 ['flag_land_potential', 'flag_high_land_potential', 'flag_rural_suburban', 'flag_small_enrollment']]
    for col in flag_cols:
        if col in df.columns:
            flagged = df[col].sum()
            valid = df[col].notna().sum()
            pct = flagged / valid * 100 if valid > 0 else 0
            print(f"   {col.replace('flag_', '').replace('_', ' ').title()}: {flagged:,.0f}/{valid:,} ({pct:.1f}%)")
    
    # Land potential
    print("\n🏞️ LAND POTENTIAL:")
    print(f"   Rural/Suburban location: {df['flag_rural_suburban'].sum():,}")
    print(f"   With property data: {df['has_property_data'].sum():,}")
    print(f"   Land potential (combined): {df['flag_land_potential'].sum():,}")
    print(f"   HIGH land potential (distressed + small + rural): {df['flag_high_land_potential'].sum():,}")
    
    # Top targets
    print("\n🎯 TOP DISTRESSED INSTITUTIONS WITH LAND POTENTIAL:")
    targets = df[df['flag_high_land_potential'] == 1].nlargest(10, 'distress_score')
    if len(targets) > 0:
        for _, row in targets.iterrows():
            print(f"   • {row['institution name']} ({row['State abbreviation']})")
            print(f"     Distress: {row['distress_score']:.0f} | Enrollment: {row['Total  enrollment_2024']:,.0f} | {row['Degree of urbanization (Urban-centric locale)']}")
    else:
        print("   No institutions meet all criteria")


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    print("=" * 60)
    print("IPEDS Financial Analysis & Distress Scoring")
    print("=" * 60)
    
    # Load data
    print(f"\nLoading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    print(f"  Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    
    # Calculate YoY changes
    print("\nCalculating year-over-year changes...")
    df = calculate_yoy_changes(df)
    
    # Calculate risk flags
    print("Calculating risk flags...")
    df = calculate_risk_flags(df)
    
    # Calculate distress score
    print("Calculating distress scores...")
    df = calculate_distress_score(df)
    
    # Calculate land potential
    print("Identifying land potential...")
    df = calculate_land_potential(df)
    
    # Print summary
    print_analysis_summary(df)
    
    # Save
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Saved: {OUTPUT_FILE}")
    print(f"   Final shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    
    return df


if __name__ == '__main__':
    main()