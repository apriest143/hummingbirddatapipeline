"""
merge_acreage_to_master.py
==========================
Merges verified acreage data into the Hummingbird Master Distress file.

DESIGNED FOR REPEATED RUNS: As new acreage scraping results come in,
just re-run this script. It will:
  - Only update rows where new/better acreage data exists
  - Never drop any existing master data
  - Preserve all original columns
  - Log exactly what changed so you can audit

Usage:
    python merge_acreage_to_master.py

Inputs (edit paths below if needed):
    MASTER_PATH  = path to Hummingbird_Master_Distress_v2.csv
    ACREAGE_PATH = path to verified_acreage_enhanced.csv

Outputs:
    - Updated master CSV (overwrites in-place by default, or set OUTPUT_PATH)
    - A changelog CSV showing every row that was updated
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime

# ── CONFIG ──────────────────────────────────────────────────────────────────
MASTER_PATH  = "hv_master_data/data/Hummingbird_Master_FINAL_clean.csv"
ACREAGE_PATH = "hv_master_data/acreage_scripts/verified_acreage_enhanced.csv"
OUTPUT_PATH  = "hv_master_data/data/Hummingbird_Master_FINAL_clean.csv"  # same file = in-place update
CHANGELOG    = f"hv_master_data/data/merge_changelog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# Columns to merge from acreage file into master
# Maps: acreage_col -> master_col (new columns are created if they don't exist)
MERGE_COLUMNS = {
    "verified_acres":  "verified_acres",
    "confidence":      "acreage_confidence",
    "source":          "acreage_source",
    "status":          "acreage_status",
    "notes":           "acreage_notes",
    "detected_type":   "acreage_detected_type",
}

# ── MATCHING LOGIC ──────────────────────────────────────────────────────────

def normalize(name):
    """Lowercase, strip non-ASCII chars (handles encoding mismatches), collapse spaces."""
    if pd.isna(name):
        return ""
    ascii_only = re.sub(r'[^\x00-\x7F]+', ' ', str(name))
    return re.sub(r'\s+', ' ', ascii_only.strip().lower())


def extract_parent_name(name):
    """
    Some acreage names have a parent-child format separated by an em-dash
    (often garbled as non-ASCII due to encoding). Extract the parent name.
    """
    parts = re.split(r'[^\x00-\x7F]+', str(name))
    if len(parts) > 1:
        return parts[0].strip()
    return None


def match_acreage_to_master(master, acreage):
    """
    Returns a dict: acreage_index -> master_index
    Uses a multi-pass matching strategy:
      1. Exact name match (case-insensitive)
      2. Match via name_alias / exact_name columns in master
      3. Parent-name match for dash-separated acreage names (with state tiebreak)
    """
    matches = {}

    # Build master lookup: normalized institution_name -> list of master indices
    master_by_name = {}
    for idx, row in master.iterrows():
        key = normalize(row['institution_name'])
        master_by_name.setdefault(key, []).append(idx)

    # Also index by name_alias and exact_name
    master_by_alias = {}
    for idx, row in master.iterrows():
        for col in ['name_alias', 'exact_name']:
            if col in master.columns and pd.notna(row.get(col)):
                key = normalize(row[col])
                master_by_alias.setdefault(key, []).append(idx)

    def pick_best(candidates, a_state):
        """Pick candidate with matching state, else first."""
        if len(candidates) == 1:
            return candidates[0]
        for m_idx in candidates:
            if normalize(master.loc[m_idx].get('state', '')) == a_state:
                return m_idx
        return candidates[0]

    # ── Pass 1: Direct name match ───────────────────────────────────────
    for a_idx, a_row in acreage.iterrows():
        a_name = normalize(a_row['name'])
        a_state = normalize(a_row.get('state', ''))
        if a_name in master_by_name:
            matches[a_idx] = pick_best(master_by_name[a_name], a_state)

    # ── Pass 2: Alias / exact_name match ────────────────────────────────
    for a_idx in [i for i in acreage.index if i not in matches]:
        a_name = normalize(acreage.loc[a_idx, 'name'])
        if a_name in master_by_alias:
            matches[a_idx] = master_by_alias[a_name][0]

    return matches


# ── MERGE LOGIC ─────────────────────────────────────────────────────────────

CONF_RANK = {'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}

def should_update(existing_vals, acreage_row):
    """
    Decide whether the acreage row has data worth merging.
    Returns True if:
      - acreage has verified_acres and master doesn't
      - acreage has higher confidence than what's already there
    """
    new_acres = acreage_row.get('verified_acres')
    if pd.isna(new_acres):
        return False

    old_acres = existing_vals.get('verified_acres', '')
    if pd.isna(old_acres) or str(old_acres).strip() == '':
        return True

    # If both have data, prefer higher confidence
    old_conf = str(existing_vals.get('acreage_confidence', '')).strip().upper()
    new_conf = str(acreage_row.get('confidence', '')).strip().upper()
    if CONF_RANK.get(new_conf, 0) > CONF_RANK.get(old_conf, 0):
        return True

    return False


def merge(master_path, acreage_path, output_path, changelog_path):
    """Main merge routine."""
    print(f"Loading master:  {master_path}")
    master = pd.read_csv(master_path, low_memory=False, keep_default_na=False, na_values=[])
    orig_col_count = len(master.columns)
    print(f"  → {len(master)} rows, {orig_col_count} columns")

    print(f"Loading acreage: {acreage_path}")
    acreage = pd.read_csv(acreage_path, low_memory=False)
    acreage_has_data = acreage['verified_acres'].notna().sum()
    print(f"  → {len(acreage)} rows")
    print(f"  → {acreage_has_data} rows with verified_acres")

    # Track new columns added
    new_cols = []
    for acr_col, mst_col in MERGE_COLUMNS.items():
        if mst_col not in master.columns:
            master[mst_col] = ''
            new_cols.append(mst_col)
            print(f"  + Added new column to master: '{mst_col}'")

    # Match
    print("\nMatching acreage rows to master...")
    matches = match_acreage_to_master(master, acreage)
    print(f"  → {len(matches)} matched out of {len(acreage)} acreage rows")

    # Merge
    changelog = []
    updated = 0
    skipped_no_data = 0
    skipped_existing = 0

    for a_idx, m_idx in matches.items():
        a_row = acreage.loc[a_idx]

        # Get current master values for merge columns
        existing = {}
        for _, mst_col in MERGE_COLUMNS.items():
            existing[mst_col] = master.at[m_idx, mst_col]

        if not should_update(existing, a_row):
            if pd.isna(a_row.get('verified_acres')):
                skipped_no_data += 1
            else:
                skipped_existing += 1
            continue

        change_record = {
            'institution_name': master.at[m_idx, 'institution_name'],
            'acreage_name': a_row['name'],
            'timestamp': datetime.now().isoformat(),
        }

        # Update merge columns
        for acr_col, mst_col in MERGE_COLUMNS.items():
            new_val = a_row.get(acr_col)
            if pd.notna(new_val):
                old_val = master.at[m_idx, mst_col]
                change_record[f'{mst_col}_old'] = old_val
                change_record[f'{mst_col}_new'] = str(new_val)
                master.at[m_idx, mst_col] = str(new_val)

        # Also backfill acreage_raw if it's empty/zero
        current_raw = str(master.at[m_idx, 'acreage_raw']).strip()
        if current_raw in ('', '0', '0.0', 'nan', 'NaN'):
            if pd.notna(a_row.get('verified_acres')):
                master.at[m_idx, 'acreage_raw'] = str(a_row['verified_acres'])
                change_record['acreage_raw_old'] = current_raw
                change_record['acreage_raw_new'] = str(a_row['verified_acres'])

        changelog.append(change_record)
        updated += 1

    # Summary
    unmatched_count = len(acreage) - len(matches)
    unmatched_with_data = sum(
        1 for i in acreage.index
        if i not in matches and pd.notna(acreage.loc[i].get('verified_acres'))
    )

    print(f"\n{'='*55}")
    print(f"  MERGE SUMMARY")
    print(f"{'='*55}")
    print(f"  Matched rows:                    {len(matches):,}")
    print(f"  Updated with new/better data:    {updated:,}")
    print(f"  Skipped (no verified_acres):     {skipped_no_data:,}")
    print(f"  Skipped (already had equal+):    {skipped_existing:,}")
    print(f"  Unmatched acreage rows:          {unmatched_count:,}")
    print(f"    └─ with verified_acres:        {unmatched_with_data:,}")
    print(f"  Master columns: {orig_col_count} → {len(master.columns)}")
    if new_cols:
        print(f"    └─ new: {', '.join(new_cols)}")
    print(f"{'='*55}")

    # Save master
    master.to_csv(output_path, index=False)
    print(f"\n✓ Master saved to: {output_path}")

    # Save changelog
    if changelog:
        cl_df = pd.DataFrame(changelog)
        cl_df.to_csv(changelog_path, index=False)
        print(f"✓ Changelog saved to: {changelog_path}")
    else:
        print("  (No changes to log)")

    # Report unmatched rows that had data
    if unmatched_with_data > 0:
        unmatched_rows = acreage[
            (~acreage.index.isin(matches.keys())) &
            (acreage['verified_acres'].notna())
        ][['name', 'city', 'state', 'verified_acres', 'confidence', 'source']]
        unmatched_file = f"unmatched_with_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        unmatched_rows.to_csv(unmatched_file, index=False)
        print(f"⚠ {unmatched_with_data} unmatched rows WITH data saved to: {unmatched_file}")
        print(f"  Review these — they may be sub-entities or name mismatches.")

    return master


# ── RUN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    merge(MASTER_PATH, ACREAGE_PATH, OUTPUT_PATH, CHANGELOG)