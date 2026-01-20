### 990_analysis.py
### Takes combined 990 data and produces one row per EIN with most recent filing + historical averages

import pandas as pd
import numpy as np

# ---- Configuration ----
INPUT_FILE = "hv_master_data/data/990s/990_combined.csv"
HUMMINGBIRD_FILE = "hv_master_data/data/HummingbirdDataWorking_ein_exact_fuzzy.csv"
OUTPUT_FILE = "hv_master_data/data/990s/990_analysis.csv"

print("="*60)
print("990 ANALYSIS - Most Recent Filing + Historical Averages")
print("="*60)

# ---- Load Hummingbird data to get relevant EINs ----
print("\n1. Loading Hummingbird data to filter EINs...")
hb_df = pd.read_csv(HUMMINGBIRD_FILE, dtype=str)

# Get set of EINs from Hummingbird (normalized - remove dashes, spaces, zero-pad to 9 digits)
hb_df["ein_clean"] = hb_df["ein_number"].astype(str).str.replace("-", "").str.replace(" ", "").str.strip()
hb_df["ein_clean"] = hb_df["ein_clean"].apply(lambda x: x.zfill(9) if x and x != "nan" else x)
hb_eins = set(hb_df["ein_clean"].dropna())
hb_eins.discard("")
hb_eins.discard("nan")

print(f"   Unique EINs in Hummingbird: {len(hb_eins)}")

# ---- Load 990 data ----
print("\n2. Loading 990 combined data...")
df = pd.read_csv(INPUT_FILE, dtype=str)
print(f"   Total 990 filings loaded: {len(df)}")
print(f"   Unique EINs in 990 data: {df['ein'].nunique()}")

# ---- Filter to only EINs in Hummingbird ----
print("\n3. Filtering to Hummingbird EINs only...")
df["ein_clean"] = df["ein"].astype(str).str.replace("-", "").str.replace(" ", "").str.strip()
df["ein_clean"] = df["ein_clean"].apply(lambda x: x.zfill(9) if x and x != "nan" else x)
df = df[df["ein_clean"].isin(hb_eins)]

print(f"   Filings after filter: {len(df)}")
print(f"   Unique EINs after filter: {df['ein'].nunique()}")

# ---- Convert numeric columns ----
numeric_cols = ["total_revenue", "total_expenses", "total_assets_end", "total_liabilities_end", "net_assets_end"]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ---- Convert tax_period to sortable format ----
# tax_period is YYYYMM format, so string sorting works
df["tax_period"] = df["tax_period"].astype(str).str.strip()

# ---- Sort by EIN and tax_period (most recent first) ----
df = df.sort_values(["ein", "tax_period"], ascending=[True, False])

# ---- Determine cutoff for "last 3 years" ----
# Get the most recent tax_period in the dataset to establish baseline
most_recent_period = df["tax_period"].max()
most_recent_year = int(most_recent_period[:4])
cutoff_year = most_recent_year - 3  # e.g., if most recent is 2024, cutoff is 2021

print(f"\nMost recent tax period in data: {most_recent_period}")
print(f"Historical average cutoff year: {cutoff_year}")

# ---- Process each EIN ----
results = []

for ein, group in df.groupby("ein"):
    # Sort group by tax_period descending (most recent first)
    group = group.sort_values("tax_period", ascending=False)
    
    # Get most recent filing
    most_recent = group.iloc[0].copy()
    
    # Filter to last 3 years for historical average
    group["tax_year"] = group["tax_period"].str[:4].astype(int)
    historical = group[group["tax_year"] >= cutoff_year]
    
    # Calculate averages (only if more than 1 filing exists)
    if len(historical) >= 2:
        avg_revenue = historical["total_revenue"].mean()
        avg_expenses = historical["total_expenses"].mean()
    else:
        avg_revenue = np.nan
        avg_expenses = np.nan
    
    # Build result row
    result = {
        "ein": ein,
        "tax_period": most_recent["tax_period"],
        "form_type": most_recent["form_type"],
        "total_revenue": most_recent["total_revenue"],
        "total_expenses": most_recent["total_expenses"],
        "total_assets_end": most_recent["total_assets_end"],
        "total_liabilities_end": most_recent["total_liabilities_end"],
        "net_assets_end": most_recent["net_assets_end"],
        "avg_revenue_3yr": avg_revenue,
        "avg_expenses_3yr": avg_expenses,
        "num_filings_in_3yr": len(historical)
    }
    
    results.append(result)

# ---- Create output dataframe ----
df_output = pd.DataFrame(results)

# ---- Replace NaN with "N/A" for average columns ----
df_output["avg_revenue_3yr"] = df_output["avg_revenue_3yr"].apply(
    lambda x: "N/A" if pd.isna(x) else round(x, 2)
)
df_output["avg_expenses_3yr"] = df_output["avg_expenses_3yr"].apply(
    lambda x: "N/A" if pd.isna(x) else round(x, 2)
)

# ---- Save output ----
df_output.to_csv(OUTPUT_FILE, index=False)

# ---- Print summary ----
print("\n" + "-"*60)
print("OUTPUT SUMMARY")
print("-"*60)
print(f"Total unique EINs (output rows): {len(df_output)}")

has_avg = df_output[df_output["avg_revenue_3yr"] != "N/A"]
no_avg = df_output[df_output["avg_revenue_3yr"] == "N/A"]

print(f"EINs with historical averages: {len(has_avg)}")
print(f"EINs without averages (single filing): {len(no_avg)}")

print(f"\nBreakdown by form type (most recent):")
print(df_output["form_type"].value_counts().to_string())

print(f"\nBreakdown by number of filings in 3-year window:")
print(df_output["num_filings_in_3yr"].value_counts().sort_index().to_string())

print("-"*60)
print(f"Saved to: {OUTPUT_FILE}")
print("="*60)