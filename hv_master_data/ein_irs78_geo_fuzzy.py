import pandas as pd
import re
from pathlib import Path
from rapidfuzz import process, fuzz

# ---------- PATHS ----------
MASTER_IN = Path("hv_master_data/data/HummingbirdDataWorking_ein_exact_fuzzy.csv")
IRS78_IN  = Path("hv_master_data/data/irs78bulk_with_headers.csv")

MASTER_OUT = Path("hv_master_data/data/HummingbirdDataWorking_ein_irs78_geo_fuzzy.csv")
LOG_OUT    = Path("hv_master_data/data/ein_irs78_geo_fuzzy_log.csv")

# ---------- COLUMNS (edit if yours differ) ----------
NAME_COL  = "institution_name"
CITY_COL  = "city"
STATE_COL = "state"
EIN_COL   = "ein_number"

IRS_NAME_COL  = "NAME"
IRS_CITY_COL  = "CITY"
IRS_STATE_COL = "STATE"
IRS_EIN_COL   = "EIN"

# ---------- SETTINGS ----------
SCORE_THRESHOLD = 89      # start conservative; try 90 if too low
MAX_CANDIDATES_PER_STATE = 200000  # safety for huge states; adjust if needed

# ---------- NORMALIZATION ----------
NONALNUM_SPACE = re.compile(r"[^a-z0-9 ]")
WS = re.compile(r"\s+")
STOPWORDS = {"the"}  # keep minimal; add later if you want

def norm_for_match(s: str) -> str:
    """Lowercase, remove punctuation, normalize whitespace."""
    if not isinstance(s, str):
        return ""
    s = s.lower().strip()
    s = s.replace("&", "and")
    s = NONALNUM_SPACE.sub(" ", s)
    s = WS.sub(" ", s).strip()
    # optional tiny stopword removal
    parts = [p for p in s.split() if p not in STOPWORDS]
    return " ".join(parts)

def norm_city(s: str) -> str:
    return norm_for_match(s)

def norm_state(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return s.strip().upper()

def is_missing(x) -> bool:
    if x is None:
        return True
    s = str(x).strip()
    return s == "" or s.lower() == "nan"

# ---------- LOAD ----------
df = pd.read_csv(MASTER_IN, dtype=str)
irs = pd.read_csv(IRS78_IN, dtype=str)

df.columns = [c.strip() for c in df.columns]
irs.columns = [c.strip() for c in irs.columns]

# Ensure master has log columns
for c in ["EIN_Source", "EIN_Match_Type", "EIN_Match_Score", "EIN_Matched_IRS_Name"]:
    if c not in df.columns:
        df[c] = ""

# Normalize master keys
df["_q_name"]  = df[NAME_COL].apply(norm_for_match)
df["_q_city"]  = df[CITY_COL].apply(norm_city)
df["_q_state"] = df[STATE_COL].apply(norm_state)

# Normalize IRS keys
irs["_n_name"]  = irs[IRS_NAME_COL].apply(norm_for_match)
irs["_n_city"]  = irs[IRS_CITY_COL].apply(norm_city)
irs["_n_state"] = irs[IRS_STATE_COL].apply(norm_state)

# Keep only IRS rows with EIN
irs = irs[irs[IRS_EIN_COL].notna() & (~irs[IRS_EIN_COL].apply(is_missing))].copy()

# ---------- BUILD BLOCKS BY STATE ----------
# state -> list of IRS normalized names + parallel arrays for lookup
state_to_names = {}
state_to_idx = {}

for st, grp in irs.groupby("_n_state"):
    names = grp["_n_name"].tolist()
    idxs = grp.index.tolist()
    # optional: cap huge states for memory safety (rarely needed)
    if len(names) > MAX_CANDIDATES_PER_STATE:
        names = names[:MAX_CANDIDATES_PER_STATE]
        idxs = idxs[:MAX_CANDIDATES_PER_STATE]
    state_to_names[st] = names
    state_to_idx[st] = idxs

# A fallback all-states list (only used when master state missing)
all_names = irs["_n_name"].tolist()
all_idxs = irs.index.tolist()

# ---------- MATCH LOOP ----------
filled = 0
attempted = 0
remaining = 0
log_rows = []

for i, row in df.iterrows():
    if not is_missing(row.get(EIN_COL)):
        continue

    q_name = row["_q_name"]
    q_city = row["_q_city"]
    q_state = row["_q_state"]

    if not q_name:
        remaining += 1
        continue

    # choose candidate pool: by state if available, else all
    if q_state and q_state in state_to_names:
        pool_names = state_to_names[q_state]
        pool_idxs  = state_to_idx[q_state]
    else:
        pool_names = all_names
        pool_idxs  = all_idxs

    attempted += 1

    best = process.extractOne(q_name, pool_names, scorer=fuzz.token_sort_ratio)
    if best is None:
        remaining += 1
        continue

    best_name, score, pool_pos = best
    if score < SCORE_THRESHOLD:
        remaining += 1
        continue

    irs_idx = pool_idxs[pool_pos]
    cand = irs.loc[irs_idx]

    cand_city = cand["_n_city"]
    cand_state = cand["_n_state"]

    # Gate: accept only if city OR state matches
    city_ok = (q_city != "" and cand_city == q_city)
    state_ok = (q_state != "" and cand_state == q_state)

    if not (city_ok or state_ok):
        remaining += 1
        continue

    # Fill EIN
    df.at[i, EIN_COL] = cand[IRS_EIN_COL]
    df.at[i, "EIN_Source"] = "IRS Pub78"
    df.at[i, "EIN_Match_Type"] = "Fuzzy+GeoGate"
    df.at[i, "EIN_Match_Score"] = str(score)
    df.at[i, "EIN_Matched_IRS_Name"] = cand[IRS_NAME_COL]
    filled += 1

    log_rows.append({
        "row_index": i,
        "institution": row.get(NAME_COL, ""),
        "master_city": row.get(CITY_COL, ""),
        "master_state": row.get(STATE_COL, ""),
        "query_norm": q_name,
        "matched_irs_name": cand[IRS_NAME_COL],
        "irs_city": cand[IRS_CITY_COL],
        "irs_state": cand[IRS_STATE_COL],
        "score": score,
        "city_ok": city_ok,
        "state_ok": state_ok,
        "ein_filled": cand[IRS_EIN_COL],
    })

# ---------- SAVE ----------
df.drop(columns=["_q_name", "_q_city", "_q_state"], inplace=True, errors="ignore")
df.to_csv(MASTER_OUT, index=False)
pd.DataFrame(log_rows).to_csv(LOG_OUT, index=False)

# Count remaining missing EINs
missing_after = int((df[EIN_COL].isna() | df[EIN_COL].apply(is_missing)).sum())

print("=== IRS78 Fuzzy + Geo Gate Summary ===")
print(f"Attempted (blank EIN rows with name): {attempted}")
print(f"Filled: {filled}")
print(f"Remaining missing EINs (total): {missing_after}")
print(f"Wrote: {MASTER_OUT}")
print(f"Wrote log: {LOG_OUT}")
