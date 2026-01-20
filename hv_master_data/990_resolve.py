import pandas as pd
import numpy as np

# Pick one of the missing EINs to search for
test_ein = "751863443"

# Load the raw 990 combined data and look for this EIN
df_990 = pd.read_csv("hv_master_data/data/990s/990_combined.csv", dtype=str)
HUMMINGBIRD_FILE = "hv_master_data/data/HummingbirdDataWorking_ein_exact_fuzzy.csv"
hb_df = pd.read_csv(HUMMINGBIRD_FILE, dtype=str)


# Check various ways it might appear
print("Searching for EIN in 990 data...")
print(f"Exact match: {len(df_990[df_990['ein'] == test_ein])}")
print(f"Contains: {len(df_990[df_990['ein'].str.contains(test_ein, na=False)])}")

# Also show some sample EINs from 990 data to compare format
print("\nSample EINs from 990 data:")
print(df_990['ein'].head(10).tolist())

# And sample from Hummingbird
print("\nSample EINs from Hummingbird:")
print(hb_df['ein_number'].head(10).tolist())



####AAAAAHAAAA our HV EINs have lost leading zeros along the way (likely still in excel phase)
#####Will add them back in in the analysis step