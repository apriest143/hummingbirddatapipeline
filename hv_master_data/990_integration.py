### 990_integration.py
### Merges 990 financial data into Hummingbird dataset

import pandas as pd
import numpy as np

# ---- Configuration ----
HUMMINGBIRD_FILE = "hv_master_data/data/HummingbirdDataWorking_ein_exact_fuzzy.csv"
ANALYSIS_990_FILE = "hv_master_data/data/990s/990_analysis.csv"
OUTPUT_FILE = "hv_master_data/data/HummingbirdDataWorking_990_merged.csv"

print("="*60)
print("990 INTEGRATION - Merging Financial Data into Hummingbird")
print("="*60)

# ---- Load data ----
print("\n1. Loading datasets...")
hb_df = pd.read_csv(HUMMINGBIRD_FILE, dtype=str)
fin_df = pd.read_csv(ANALYSIS_990_FILE, dtype=str)

print(f"   Hummingbird rows: {len(hb_df)}")
print(f"   990 analysis rows: {len(fin_df)}")

# ---- Fix column typo: prevous_assets -> previous_assets ----
if "prevous_assets" in hb_df.columns:
    hb_df = hb_df.rename(columns={"prevous_assets": "previous_assets"})
    print("\n   Fixed column typo: 'prevous_assets' -> 'previous_assets'")

# ---- Normalize EINs for matching ----
print("\n2. Normalizing EINs for matching...")

def clean_ein(ein):
    if pd.isna(ein):
        return ""
    ein_str = str(ein).replace("-", "").replace(" ", "").strip()
    if ein_str.lower() == "nan" or ein_str == "":
        return ""
    return ein_str.zfill(9)

hb_df["ein_clean"] = hb_df["ein_number"].apply(clean_ein)
fin_df["ein_clean"] = fin_df["ein"].apply(clean_ein)

# ---- Build lookup from 990 data ----
print("\n3. Building 990 lookup...")
fin_lookup = fin_df.set_index("ein_clean").to_dict("index")
print(f"   990 records indexed: {len(fin_lookup)}")

# ---- Merge financial data ----
print("\n4. Merging financial data...")

stats = {
    "matched": 0,
    "not_matched": 0,
    "no_ein": 0
}

for idx, row in hb_df.iterrows():
    ein = row["ein_clean"]
    
    if ein == "":
        stats["no_ein"] += 1
        continue
    
    if ein in fin_lookup:
        fin_data = fin_lookup[ein]
        stats["matched"] += 1
        
        # Populate previous financials
        hb_df.at[idx, "previous_revenue"] = fin_data.get("total_revenue", "")
        hb_df.at[idx, "previous_expenses"] = fin_data.get("total_expenses", "")
        
        # Calculate previous net income (revenue - expenses)
        try:
            rev = float(fin_data.get("total_revenue", 0) or 0)
            exp = float(fin_data.get("total_expenses", 0) or 0)
            hb_df.at[idx, "previous_net_income"] = str(rev - exp)
        except (ValueError, TypeError):
            hb_df.at[idx, "previous_net_income"] = ""
        
        hb_df.at[idx, "previous_assets"] = fin_data.get("total_assets_end", "")
        hb_df.at[idx, "previous_liabilities"] = fin_data.get("total_liabilities_end", "")
        hb_df.at[idx, "previous_net_assets"] = fin_data.get("net_assets_end", "")
        
        # Add average columns
        hb_df.at[idx, "avg_revenue_3yr"] = fin_data.get("avg_revenue_3yr", "")
        hb_df.at[idx, "avg_expenses_3yr"] = fin_data.get("avg_expenses_3yr", "")
        
    else:
        stats["not_matched"] += 1

# ---- Drop temporary column ----
hb_df = hb_df.drop(columns=["ein_clean"])

# ---- Save output ----
print(f"\n5. Saving to: {OUTPUT_FILE}")
hb_df.to_csv(OUTPUT_FILE, index=False)

# ---- Print summary ----
print("\n" + "="*60)
print("INTEGRATION SUMMARY")
print("="*60)
print(f"Total Hummingbird rows: {len(hb_df)}")
print(f"Matched with 990 data: {stats['matched']}")
print(f"No match found: {stats['not_matched']}")
print(f"No EIN in Hummingbird: {stats['no_ein']}")
print(f"\nMatch rate (of those with EINs): {stats['matched'] / (stats['matched'] + stats['not_matched']) * 100:.1f}%")
print("="*60)
print("DONE!")
print("="*60)