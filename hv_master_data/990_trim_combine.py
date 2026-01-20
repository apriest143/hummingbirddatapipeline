### Trimming down unwanted columns in the 990s
import pandas as pd


###Standard 990
df = pd.read_csv("hv_master_data/data/990s/24eoextract990.csv", dtype=str)

keep_cols_990 = [
    "EIN",
    "tax_pd",
    "totrevenue",
    "totfuncexpns",
    "totassetsend",
    "totliabend",
    "totnetassetend"
]

df_990_core = df[keep_cols_990]
df_990_core.to_csv(
    "hv_master_data/data/990s/24eoextract990_core.csv",
    index=False
)


### 990pf
df = pd.read_csv("hv_master_data/data/990s/24eoextract990pf.csv", dtype=str)

keep_cols_990PF = [
    "EIN",
    "TAX_PRD",
    "TAX_YR",
    "TOTRCPTPERBKS",
    "TOTEXPNSPBKS",
    "TOTASSETSEND",
    "TOTLIABEND"
]

df_990PF_core = df[keep_cols_990PF]
df_990PF_core.to_csv(
    "hv_master_data/data/990s/24eoextract990pf_core.csv",
    index=False
)

### 990ez
df = pd.read_csv("hv_master_data/data/990s/24eoextract990EZ.csv", dtype=str)

keep_cols_990EZ = [
    "EIN",
    "taxpd",
    "totrevnue",
    "totexpns",
    "totassetsend",
    "totliabend",
    "totnetassetsend"
]

df_990EZ_core = df[keep_cols_990EZ]
df_990EZ_core.to_csv(
    "hv_master_data/data/990s/24eoextract990EZ_core.csv",
    index=False
)


### ============================================================
### COMBINE ALL 990 FORMS INTO UNIFIED FORMAT
### ============================================================

print("="*60)
print("COMBINING 990 FORMS INTO UNIFIED FORMAT")
print("="*60)

# Load the core files
df_990 = pd.read_csv("hv_master_data/data/990s/24eoextract990_core.csv", dtype=str)
df_990ez = pd.read_csv("hv_master_data/data/990s/24eoextract990EZ_core.csv", dtype=str)
df_990pf = pd.read_csv("hv_master_data/data/990s/24eoextract990pf_core.csv", dtype=str)

print(f"Regular 990 rows: {len(df_990)}")
print(f"990-EZ rows: {len(df_990ez)}")
print(f"990-PF rows: {len(df_990pf)}")

# Standardize column names for Regular 990
df_990_unified = df_990.rename(columns={
    "EIN": "ein",
    "tax_pd": "tax_period",
    "totrevenue": "total_revenue",
    "totfuncexpns": "total_expenses",
    "totassetsend": "total_assets_end",
    "totliabend": "total_liabilities_end",
    "totnetassetend": "net_assets_end"
})
df_990_unified["form_type"] = "990"

# Standardize column names for 990-EZ
df_990ez_unified = df_990ez.rename(columns={
    "EIN": "ein",
    "taxpd": "tax_period",
    "totrevnue": "total_revenue",
    "totexpns": "total_expenses",
    "totassetsend": "total_assets_end",
    "totliabend": "total_liabilities_end",
    "totnetassetsend": "net_assets_end"
})
df_990ez_unified["form_type"] = "990-EZ"

# Standardize column names for 990-PF
df_990pf_unified = df_990pf.rename(columns={
    "EIN": "ein",
    "TAX_PRD": "tax_period",
    "TOTRCPTPERBKS": "total_revenue",
    "TOTEXPNSPBKS": "total_expenses",
    "TOTASSETSEND": "total_assets_end",
    "TOTLIABEND": "total_liabilities_end"
})
df_990pf_unified["form_type"] = "990-PF"

# Calculate net_assets_end for 990-PF (assets - liabilities)
df_990pf_unified["total_assets_end"] = pd.to_numeric(df_990pf_unified["total_assets_end"], errors="coerce")
df_990pf_unified["total_liabilities_end"] = pd.to_numeric(df_990pf_unified["total_liabilities_end"], errors="coerce")
df_990pf_unified["net_assets_end"] = df_990pf_unified["total_assets_end"] - df_990pf_unified["total_liabilities_end"]

# Drop TAX_YR from 990-PF (not needed in unified format)
if "TAX_YR" in df_990pf_unified.columns:
    df_990pf_unified = df_990pf_unified.drop(columns=["TAX_YR"])

# Define final column order
final_columns = [
    "ein",
    "tax_period",
    "form_type",
    "total_revenue",
    "total_expenses",
    "total_assets_end",
    "total_liabilities_end",
    "net_assets_end"
]

# Ensure all dataframes have the same columns in the same order
df_990_unified = df_990_unified[final_columns]
df_990ez_unified = df_990ez_unified[final_columns]
df_990pf_unified = df_990pf_unified[final_columns]

# Combine all three
df_combined = pd.concat([df_990_unified, df_990ez_unified, df_990pf_unified], ignore_index=True)

# Save combined file
df_combined.to_csv("hv_master_data/data/990s/990_combined.csv", index=False)

print("-"*60)
print(f"Combined total rows: {len(df_combined)}")
print(f"\nBreakdown by form type:")
print(df_combined["form_type"].value_counts().to_string())

# Count duplicate EINs
ein_counts = df_combined["ein"].value_counts()
duplicate_eins = ein_counts[ein_counts > 1]
print(f"\nUnique EINs: {len(ein_counts)}")
print(f"EINs with multiple filings: {len(duplicate_eins)}")
print(f"Total duplicate rows: {duplicate_eins.sum() - len(duplicate_eins)}")

print("-"*60)
print("Saved to: hv_master_data/data/990s/990_combined.csv")
print("="*60)