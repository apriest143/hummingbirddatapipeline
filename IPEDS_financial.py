### ipeds_financial_fill.py
### Fills missing financial data in Hummingbird from IPEDS
### 
### IPEDS has two financial data formats:
### - F2 columns: Non-profit institutions
### - F3 columns: For-profit institutions
###
### Maps to Hummingbird columns:
### - latest_revenue ← F2.Total unrestricted operating revenues OR F3.Total revenues
### - latest_expenses ← F2.Total expenses OR F3.Total expenses
### - latest_assets ← F3.Total assets (F2 doesn't have this)
### - latest_net_assets ← F2.Total net assets
### - latest_net_income ← F2.Change in net assets OR F3.Pretax income

import pandas as pd
import re

# ---- Configuration ----
HUMMINGBIRD_FILE = "hv_master_data/data/HummingbirdDataWorking_990_merged.csv"
IPEDS_FILE = "hv_master_data/data/IPEDS.csv"
OUTPUT_FILE = "hv_master_data/data/HummingbirdDataWorking_990_merged.csv"  # Overwrite in place

print("="*70)
print("IPEDS FINANCIAL DATA FILL")
print("="*70)

# ---- Helper functions ----
def is_missing(val):
    if pd.isna(val):
        return True
    s = str(val).strip()
    return s == '' or s.lower() == 'nan'

def normalize_name(name):
    """Normalize institution name for matching"""
    if not isinstance(name, str):
        return ""
    normalized = name.lower().strip()
    # Remove common variations
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized

# ---- Load data ----
print("\n1. Loading data...")
hb_df = pd.read_csv(HUMMINGBIRD_FILE, dtype=str)
ipeds_df = pd.read_csv(IPEDS_FILE, dtype=str)

print(f"   Hummingbird rows: {len(hb_df)}")
print(f"   IPEDS rows: {len(ipeds_df)}")

# Filter to just Private Colleges (IPEDS data is for higher ed)
private_colleges = hb_df[hb_df['institution_type'] == 'Private College'].copy()
print(f"   Private Colleges in Hummingbird: {len(private_colleges)}")

# ---- Build IPEDS lookup ----
print("\n2. Building IPEDS lookup...")

# Create lookup by normalized name AND by EIN
ipeds_by_name = {}
ipeds_by_ein = {}

for idx, row in ipeds_df.iterrows():
    # By name
    name_norm = normalize_name(row['institution name'])
    if name_norm:
        ipeds_by_name[name_norm] = row
    
    # By alias
    alias_norm = normalize_name(row['HD2024.Institution name alias'])
    if alias_norm and alias_norm != 'nan':
        ipeds_by_name[alias_norm] = row
    
    # By EIN
    ein = row['HD2024.Employer Identification Number']
    if not is_missing(ein):
        ein_clean = str(ein).replace('-', '').strip().zfill(9)
        ipeds_by_ein[ein_clean] = row

print(f"   IPEDS entries indexed by name: {len(ipeds_by_name)}")
print(f"   IPEDS entries indexed by EIN: {len(ipeds_by_ein)}")

# ---- Match and fill ----
print("\n3. Matching and filling financial data...")
print("-"*70)

stats = {
    'matched_by_ein': 0,
    'matched_by_name': 0,
    'no_match': 0,
    'already_filled': 0,
    'revenue_filled': 0,
    'expenses_filled': 0,
    'assets_filled': 0,
    'liabilities_filled': 0,
    'net_assets_filled': 0,
    'net_income_filled': 0
}

def get_ipeds_match(row):
    """Find IPEDS match by EIN first, then by name"""
    # Try EIN first
    ein = row.get('ein_number', '')
    if not is_missing(ein):
        ein_clean = str(ein).replace('-', '').strip().zfill(9)
        if ein_clean in ipeds_by_ein:
            return ipeds_by_ein[ein_clean], 'ein'
    
    # Try name
    name_norm = normalize_name(row['institution_name'])
    if name_norm in ipeds_by_name:
        return ipeds_by_name[name_norm], 'name'
    
    return None, None

def get_financial_value(ipeds_row, f2_col, f3_col):
    """Get financial value from either F2 or F3 column"""
    # Try F2 first (non-profit)
    if f2_col and f2_col in ipeds_row.index:
        val = ipeds_row[f2_col]
        if not is_missing(val):
            return val
    
    # Try F3 (for-profit)
    if f3_col and f3_col in ipeds_row.index:
        val = ipeds_row[f3_col]
        if not is_missing(val):
            return val
    
    return None

# Process each Private College
for idx in private_colleges.index:
    row = hb_df.loc[idx]
    
    # Find IPEDS match
    ipeds_row, match_type = get_ipeds_match(row)
    
    if ipeds_row is None:
        stats['no_match'] += 1
        continue
    
    if match_type == 'ein':
        stats['matched_by_ein'] += 1
    else:
        stats['matched_by_name'] += 1
    
    # Get all financial values from IPEDS
    revenue = get_financial_value(ipeds_row, 
                                   'F2324_F2.Total unrestricted operating revenues',
                                   'F2324_F3.Total revenues')
    expenses = get_financial_value(ipeds_row,
                                   'F2324_F2.Total expenses',
                                   'F2324_F3.Total expenses')
    assets = get_financial_value(ipeds_row, None, 'F2324_F3.Total assets')
    net_assets = get_financial_value(ipeds_row, 'F2324_F2.Total net assets', None)
    net_income = get_financial_value(ipeds_row,
                                      'F2324_F2.Change in net assets',
                                      'F2324_F3.Pretax income')
    
    # Only fill if IPEDS has at least revenue OR expenses (indicates valid financial data)
    # When we fill, we fill ALL fields together for accounting consistency
    if revenue or expenses:
        # Check if ANY latest_ field is missing - if so, fill ALL from IPEDS
        any_missing = (is_missing(row['latest_revenue']) or 
                       is_missing(row['latest_expenses']) or
                       is_missing(row['latest_assets']) or
                       is_missing(row['latest_net_assets']) or
                       is_missing(row['latest_net_income']) or
                       row['latest_net_income'] == '0')
        
        if any_missing:
            stats['institutions_filled'] = stats.get('institutions_filled', 0) + 1
            
            # Fill all financial fields from IPEDS for consistency
            if revenue:
                hb_df.at[idx, 'latest_revenue'] = revenue
                stats['revenue_filled'] += 1
            
            if expenses:
                hb_df.at[idx, 'latest_expenses'] = expenses
                stats['expenses_filled'] += 1
            
            if assets:
                hb_df.at[idx, 'latest_assets'] = assets
                stats['assets_filled'] += 1
            
            if net_assets:
                hb_df.at[idx, 'latest_net_assets'] = net_assets
                stats['net_assets_filled'] += 1
            
            if net_income:
                hb_df.at[idx, 'latest_net_income'] = net_income
                stats['net_income_filled'] += 1
            
            # Calculate net_income if we have revenue and expenses but no net_income
            if not net_income and revenue and expenses:
                try:
                    calc_net_income = float(revenue) - float(expenses)
                    hb_df.at[idx, 'latest_net_income'] = str(calc_net_income)
                    stats['net_income_filled'] += 1
                except:
                    pass

print("-"*70)

# ---- Save results ----
print(f"\n4. Saving to: {OUTPUT_FILE}")
hb_df.to_csv(OUTPUT_FILE, index=False)

# ---- Print summary ----
print("\n" + "="*70)
print("FILL SUMMARY")
print("="*70)
print(f"Private Colleges processed: {len(private_colleges)}")
print(f"\n--- Matching ---")
print(f"Matched by EIN: {stats['matched_by_ein']}")
print(f"Matched by name: {stats['matched_by_name']}")
print(f"No match found: {stats['no_match']}")
total_matched = stats['matched_by_ein'] + stats['matched_by_name']
print(f"Total matched: {total_matched} ({total_matched/len(private_colleges)*100:.1f}%)")

print(f"\n--- Fields Filled ---")
print(f"Institutions with financials updated: {stats.get('institutions_filled', 0)}")
print(f"Revenue filled: {stats['revenue_filled']}")
print(f"Expenses filled: {stats['expenses_filled']}")
print(f"Assets filled: {stats['assets_filled']}")
print(f"Net assets filled: {stats['net_assets_filled']}")
print(f"Net income filled: {stats['net_income_filled']}")

# Verify final coverage
print("\n" + "="*70)
print("FINAL COVERAGE (Private Colleges):")
print("="*70)
hb_verify = pd.read_csv(OUTPUT_FILE, dtype=str)
pc_verify = hb_verify[hb_verify['institution_type'] == 'Private College']

for col in ['latest_revenue', 'latest_expenses', 'latest_assets', 'latest_liabilities', 'latest_net_assets', 'latest_net_income']:
    filled = (~pc_verify[col].apply(is_missing)).sum()
    pct = filled / len(pc_verify) * 100
    print(f"   {col:<25} {filled:>5} filled ({pct:>5.1f}%)")

print("\n" + "="*70)
print("DONE!")
print("="*70)