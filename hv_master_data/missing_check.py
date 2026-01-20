### data_quality_check.py
### Checks for missing or suspect values in every column

import pandas as pd

# Load the data
df = pd.read_csv("hv_master_data/data/HummingbirdDataWorking_990_merged.csv", dtype=str)

print("="*70)
print("DATA QUALITY CHECK - HummingbirdDataWorking_990_merged.csv")
print("="*70)
print(f"Total rows: {len(df)}")
print(f"Total columns: {len(df.columns)}")
print("="*70)

def count_suspect(series):
    """Count missing, empty, or suspect values"""
    suspect_values = ['', 'nan', 'NaN', 'N/A', 'n/a', '#VALUE!', '#REF!', '#N/A', 'None', 'null']
    
    missing = series.isna().sum()
    
    empty = (series.astype(str).str.strip() == '').sum()
    
    suspect = series.astype(str).str.strip().isin(suspect_values).sum()
    
    return missing, empty, suspect

print(f"\n{'Column':<40} {'Missing':>10} {'Empty':>10} {'Suspect':>10} {'Valid':>10} {'% Valid':>10}")
print("-"*90)

for col in df.columns:
    missing, empty, suspect = count_suspect(df[col])
    
    # Valid = total - missing - empty (suspect overlaps with empty sometimes)
    valid = len(df) - missing - empty
    pct_valid = (valid / len(df)) * 100
    
    print(f"{col:<40} {missing:>10} {empty:>10} {suspect:>10} {valid:>10} {pct_valid:>9.1f}%")

print("-"*90)

# Summary by category
print("\n" + "="*70)
print("SUMMARY BY FILL RATE")
print("="*70)

fill_rates = {}
for col in df.columns:
    missing, empty, suspect = count_suspect(df[col])
    valid = len(df) - missing - empty
    fill_rates[col] = (valid / len(df)) * 100

# Sort by fill rate
sorted_rates = sorted(fill_rates.items(), key=lambda x: x[1])

print("\n🔴 LOWEST FILL RATES (< 50%):")
for col, rate in sorted_rates:
    if rate < 50:
        print(f"   {col}: {rate:.1f}%")

print("\n🟡 MEDIUM FILL RATES (50-80%):")
for col, rate in sorted_rates:
    if 50 <= rate < 80:
        print(f"   {col}: {rate:.1f}%")

print("\n🟢 HIGH FILL RATES (80%+):")
for col, rate in sorted_rates:
    if rate >= 80:
        print(f"   {col}: {rate:.1f}%")

print("\n" + "="*70)