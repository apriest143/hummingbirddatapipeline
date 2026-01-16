### Fuzzy match EINs for missing entries in master data
### need: pip install rapidfuzz

import pandas as pd
import re
from pathlib import Path
from rapidfuzz import process, fuzz

MASTER_CSV = Path("hv_master_data/data/HummingbirdDataWorking_ein_exact.csv")  # use your post-exact file
INDEX_CSV  = Path("hv_master_data/data/NonEd990.csv")

OUT_MASTER = Path("hv_master_data/data/HummingbirdDataWorking_ein_exact_fuzzy.csv")
OUT_LOG    = Path("hv_master_data/data/ein_fuzzy_log.csv")

MASTER_NAME_COL = "institution_name"   # adjust if needed
MASTER_EIN_COL  = "ein_number"         # adjust if needed

INDEX_NAME_COL  = "TAXPAYER_NAME"               # adjust if needed
INDEX_EIN_COL   = "EIN"                # adjust if needed

NONALNUM = re.compile(r"[^a-z0-9]")

def make_exact_name(x):
    if not isinstance(x, str):
        return ""
    return NONALNUM.sub("", x.lower())

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

# ---- ensure exact_name exists ----
if "exact_name" not in df.columns:
    df["exact_name"] = df[MASTER_NAME_COL].apply(make_exact_name)
if "exact_name" not in pp.columns:
    pp["exact_name"] = pp[INDEX_NAME_COL].apply(make_exact_name)

# ---- build candidate list (skip ambiguous exact_name keys) ----
pp = pp[pp[INDEX_EIN_COL].notna() & (~pp[INDEX_EIN_COL].apply(is_missing))].copy()

counts = pp["exact_name"].value_counts()
ambiguous = set(counts[counts > 1].index)

pp_unique = pp[~pp["exact_name"].isin(ambiguous)].drop_duplicates("exact_name", keep="first")
candidate_keys = pp_unique["exact_name"].tolist()
ein_by_key = dict(zip(pp_unique["exact_name"], pp_unique[INDEX_EIN_COL]))

# ---- fuzzy match only for missing EINs ----
FUZZY_THRESHOLD = 90  # start high; can lower later if needed

filled_fuzzy = 0
remaining_missing = 0
log_rows = []

for idx, row in df.iterrows():
    if is_missing(row.get(MASTER_EIN_COL)):
        q = row.get("exact_name", "")
        if not q:
            remaining_missing += 1
            continue

        best = process.extractOne(q, candidate_keys, scorer=fuzz.ratio)
        if best is None:
            remaining_missing += 1
            continue

        best_key, score, _ = best

        if score >= FUZZY_THRESHOLD:
            df.at[idx, MASTER_EIN_COL] = ein_by_key[best_key]
            filled_fuzzy += 1
            log_rows.append({
                "row_index": idx,
                "institution": row.get(MASTER_NAME_COL, ""),
                "query_exact_name": q,
                "matched_exact_name": best_key,
                "score": score,
                "ein_filled": ein_by_key[best_key]
            })
        else:
            remaining_missing += 1

# ---- save ----
df.to_csv(OUT_MASTER, index=False)
pd.DataFrame(log_rows).to_csv(OUT_LOG, index=False)

print("=== Fuzzy EIN Fill Summary ===")
print(f"Filled by fuzzy (>= {FUZZY_THRESHOLD}): {filled_fuzzy}")
print(f"Still missing after fuzzy: {remaining_missing}")
print(f"Wrote: {OUT_MASTER}")
print(f"Wrote fuzzy log: {OUT_LOG}")
