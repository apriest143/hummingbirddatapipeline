import pandas as pd
from pathlib import Path

IN_FILE  = Path("hv_master_data/data/irs78bulk.csv")
OUT_FILE = Path("hv_master_data/data/irs78bulk_with_headers.csv")

# Official Publication 78 column order (minimal set we need)
cols = [
    "EIN",
    "NAME",
    "CITY",
    "STATE",
    "COUNTRY",
    "STATUS"
]

df = pd.read_csv(
    IN_FILE,
    sep="|",
    header=None,
    names=cols,
    dtype=str
)

df.to_csv(OUT_FILE, index=False)

print("Wrote:", OUT_FILE)
print("Rows:", len(df))
print("Columns:", df.columns.tolist())

import pandas as pd
import re
from pathlib import Path

# ---- paths (adjust as needed) ----
MASTER_CSV = Path("hv_master_data/data/HummingbirdDataWorking_ein_exact_fuzzy.csv")
PUB78_CSV  = Path("hv_master_data/data/irs78bulk_with_headers.csv")  # <-- your converted Pub 78 CSV
OUT_MASTER = Path("hv_master_data/data/HummingbirdDataWorking_ein_coreloc.csv")

# ---- columns (adjust to your real headers) ----
NAME_COL  = "institution_name"
CITY_COL  = "city"
STATE_COL = "state"
EIN_COL   = "ein_number"

# Pub78 columns (adjust after you check headers)
P78_NAME_COL  = "NAME"    # often something like NAME / ORG_NAME
P78_CITY_COL  = "CITY"
P78_STATE_COL = "STATE"
P78_EIN_COL   = "EIN"

NONALNUM = re.compile(r"[^a-z0-9]")
WS = re.compile(r"\s+")

def clean_text(x):
    if not isinstance(x, str):
        return ""
    x = x.lower().strip()
    x = x.replace("&", "and")
    x = NONALNUM.sub(" ", x)
    x = WS.sub(" ", x).strip()
    return x

def exact_key(x):
    # no spaces, no punctuation
    return NONALNUM.sub("", str(x).lower()) if isinstance(x, str) else ""

def core_name(x):
    """
    Conservative core-name extraction:
    - normalize
    - drop anything after a hyphen or comma (often campus/city)
    - remove trailing tokens like 'campus' 'center' 'location'
    """
    s = clean_text(x)
    # split on common separators that often append location
    for sep in [" - ", "-", ","]:
        if sep in s:
            s = s.split(sep)[0].strip()
    # drop trailing generic tokens
    s = re.sub(r"\b(campus|center|centre|location|site)\b$", "", s).strip()
    return s

def is_missing(x):
    if x is None:
        return True
    s = str(x).strip()
    return s == "" or s.lower() == "nan"

# ---- load ----
df = pd.read_csv(MASTER_CSV, dtype=str)
p78 = pd.read_csv(PUB78_CSV, dtype=str)

df.columns = [c.strip() for c in df.columns]
p78.columns = [c.strip() for c in p78.columns]

# ---- make keys ----
df["_core"] = df[NAME_COL].apply(core_name)
df["_city"] = df[CITY_COL].apply(clean_text)
df["_state"] = df[STATE_COL].astype(str).str.upper().str.strip()

p78["_core"] = p78[P78_NAME_COL].apply(core_name)
p78["_city"] = p78[P78_CITY_COL].apply(clean_text)
p78["_state"] = p78[P78_STATE_COL].astype(str).str.upper().str.strip()

# keep rows with EIN
p78 = p78[p78[P78_EIN_COL].notna() & (~p78[P78_EIN_COL].apply(is_missing))].copy()

# Build lookup by core name -> candidate rows
# (multiple orgs can share a core, so we keep a list and then require location match)
core_groups = {}
for _, r in p78.iterrows():
    k = r["_core"]
    if not k:
        continue
    core_groups.setdefault(k, []).append(r)

filled = 0
remaining_missing = 0

for idx, row in df.iterrows():
    if not is_missing(row.get(EIN_COL)):
        continue

    k = row["_core"]
    if not k or k not in core_groups:
        remaining_missing += 1
        continue

    # require city OR state to match one of the candidates
    city = row["_city"]
    state = row["_state"]

    chosen_ein = None
    for cand in core_groups[k]:
        if (state and cand["_state"] == state) or (city and cand["_city"] == city):
            chosen_ein = cand[P78_EIN_COL]
            break

    if chosen_ein:
        df.at[idx, EIN_COL] = chosen_ein
        filled += 1
    else:
        remaining_missing += 1

# cleanup + save
df.drop(columns=["_core", "_city", "_state"], inplace=True)
df.to_csv(OUT_MASTER, index=False)

print("=== Core+Location EIN Fill Summary ===")
print(f"Filled by core+city/state: {filled}")
print(f"Still missing EINs (from those attempted): {remaining_missing}")
print(f"Wrote: {OUT_MASTER}")



import pandas as pd
import re
from pathlib import Path

# ---- paths (adjust as needed) ----
MASTER_CSV = Path("hv_master_data/data/HummingbirdDataWorking_ein_exact_fuzzy.csv")
PUB78_CSV  = Path("hv_master_data/data/irs78bulk_with_headers.csv")  
OUT_MASTER = Path("hv_master_data/data/HummingbirdDataWorking_ein_coreloc.csv")

# ---- columns (adjust to your real headers) ----
NAME_COL  = "institution_name"
CITY_COL  = "city"
STATE_COL = "state"
EIN_COL   = "ein_number"

# Pub78 columns (adjust after you check headers)
P78_NAME_COL  = "NAME"    # often something like NAME / ORG_NAME
P78_CITY_COL  = "CITY"
P78_STATE_COL = "STATE"
P78_EIN_COL   = "EIN"

NONALNUM = re.compile(r"[^a-z0-9]")
WS = re.compile(r"\s+")

def clean_text(x):
    if not isinstance(x, str):
        return ""
    x = x.lower().strip()
    x = x.replace("&", "and")
    x = NONALNUM.sub(" ", x)
    x = WS.sub(" ", x).strip()
    return x

def exact_key(x):
    # no spaces, no punctuation
    return NONALNUM.sub("", str(x).lower()) if isinstance(x, str) else ""

def core_name(x):
    """
    Conservative core-name extraction:
    - normalize
    - drop anything after a hyphen or comma (often campus/city)
    - remove trailing tokens like 'campus' 'center' 'location'
    """
    s = clean_text(x)
    # split on common separators that often append location
    for sep in [" - ", "-", ","]:
        if sep in s:
            s = s.split(sep)[0].strip()
    # drop trailing generic tokens
    s = re.sub(r"\b(campus|center|centre|location|site)\b$", "", s).strip()
    return s

def is_missing(x):
    if x is None:
        return True
    s = str(x).strip()
    return s == "" or s.lower() == "nan"

# ---- load ----
df = pd.read_csv(MASTER_CSV, dtype=str)
p78 = pd.read_csv(PUB78_CSV, dtype=str)

df.columns = [c.strip() for c in df.columns]
p78.columns = [c.strip() for c in p78.columns]

# ---- make keys ----
df["_core"] = df[NAME_COL].apply(core_name)
df["_city"] = df[CITY_COL].apply(clean_text)
df["_state"] = df[STATE_COL].astype(str).str.upper().str.strip()

p78["_core"] = p78[P78_NAME_COL].apply(core_name)
p78["_city"] = p78[P78_CITY_COL].apply(clean_text)
p78["_state"] = p78[P78_STATE_COL].astype(str).str.upper().str.strip()

# keep rows with EIN
p78 = p78[p78[P78_EIN_COL].notna() & (~p78[P78_EIN_COL].apply(is_missing))].copy()

# Build lookup by core name -> candidate rows
# (multiple orgs can share a core, so we keep a list and then require location match)
core_groups = {}
for _, r in p78.iterrows():
    k = r["_core"]
    if not k:
        continue
    core_groups.setdefault(k, []).append(r)

filled = 0
remaining_missing = 0

for idx, row in df.iterrows():
    if not is_missing(row.get(EIN_COL)):
        continue

    k = row["_core"]
    if not k or k not in core_groups:
        remaining_missing += 1
        continue

    # require city OR state to match one of the candidates
    city = row["_city"]
    state = row["_state"]

    chosen_ein = None
    for cand in core_groups[k]:
        if (state and cand["_state"] == state) or (city and cand["_city"] == city):
            chosen_ein = cand[P78_EIN_COL]
            break

    if chosen_ein:
        df.at[idx, EIN_COL] = chosen_ein
        filled += 1
    else:
        remaining_missing += 1

# cleanup + save
df.drop(columns=["_core", "_city", "_state"], inplace=True)
df.to_csv(OUT_MASTER, index=False)

print("=== Core+Location EIN Fill Summary ===")
print(f"Filled by core+city/state: {filled}")
print(f"Still missing EINs (from those attempted): {remaining_missing}")
print(f"Wrote: {OUT_MASTER}")
