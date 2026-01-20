import pandas as pd
import re
from pathlib import Path

# ---- paths ----
MASTER_CSV = Path("hv_master_data/data/HummingbirdDataWorking_ipeds_merged.csv")
INDEX_CSV  = Path("hv_master_data/data/NonEd990.csv")

OUT_MASTER = Path("hv_master_data/data/HummingbirdDataWorking_ein_exact.csv")

# ---- column names (EDIT THESE TO MATCH YOUR FILES) ----
MASTER_NAME_COL = "institution_name"   # <- change if needed
MASTER_EIN_COL  = "ein_number"         # <- change if needed

INDEX_NAME_COL  = "TAXPAYER_NAME"               # <- change if needed
INDEX_EIN_COL   = "EIN"                # <- change if needed

# ---- exact_name builder: lowercase, no spaces, no punctuation ----
NONALNUM = re.compile(r"[^a-z0-9]")

def make_exact_name(x):
    if not isinstance(x, str):
        return ""
    x = x.lower()
    x = NONALNUM.sub("", x)   # removes spaces + punctuation
    return x

def is_missing(x):
    if x is None:
        return True
    s = str(x).strip()
    return s == "" or s.lower() == "nan"

# ---- load ----
df = pd.read_csv(MASTER_CSV, dtype=str)
pp = pd.read_csv(INDEX_CSV, dtype=str)

df.columns = [c.strip() for c in df.columns]
pp.columns = [c.strip() for c in pp.columns]

# ---- add exact_name columns ----
df["exact_name"] = df[MASTER_NAME_COL].apply(make_exact_name)
pp["exact_name"] = pp[INDEX_NAME_COL].apply(make_exact_name)

# ---- build lookup: exact_name -> EIN ----
# If duplicates exist, keep first (simple). We can tighten later.
lookup = (
    pp.dropna(subset=[INDEX_EIN_COL])
      .loc[~pp[INDEX_EIN_COL].apply(is_missing), ["exact_name", INDEX_EIN_COL]]
      .drop_duplicates(subset=["exact_name"], keep="first")
      .set_index("exact_name")[INDEX_EIN_COL]
      .to_dict()
)

# ---- fill EINs only where missing ----
filled = 0
still_missing = 0

for i, row in df.iterrows():
    if is_missing(row.get(MASTER_EIN_COL)):
        key = row.get("exact_name", "")
        if key in lookup and not is_missing(lookup[key]):
            df.at[i, MASTER_EIN_COL] = lookup[key]
            filled += 1
        else:
            still_missing += 1

# ---- save + print ----
df.to_csv(OUT_MASTER, index=False)

print("=== Exact EIN Fill Summary ===")
print(f"Filled EINs: {filled}")
print(f"Still missing EINs: {still_missing}")
print(f"Wrote: {OUT_MASTER}")