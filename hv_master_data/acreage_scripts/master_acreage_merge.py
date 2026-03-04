"""
master_acreage_merge.py  v2
============================
Merges scraper + OSM acreage into the Hummingbird Master file.

Changes from v1:
  - Ingests both verified_acreage_enhanced.csv (scraper) AND osm_results.csv
  - Renames existing acreage columns: verified_acres → scraper_acres etc.
  - Drops acreage_raw entirely (data quality unknown, not recoverable)
  - Adds OSM columns alongside scraper columns — both shown, neither hidden
  - Adds acreage_primary: the figure the map should display prominently
      IPEDS colleges, enrollment >= 4000 → scraper_acres is primary
      IPEDS colleges, enrollment <  4000 → osm_acres is primary
      990 orgs                           → both shown, neither flagged primary
                                           (insufficient stress-testing yet)
  - Adds acreage_primary_source: which source is primary for this row
  - Cleans garbled unicode across all text columns before writing
  - Safe for repeated runs — never downgrades existing HIGH confidence data

Usage:
    python master_acreage_merge.py

Edit CONFIG block below for your paths.
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime

# ── CONFIG ──────────────────────────────────────────────────────────────────
MASTER_PATH  = "hv_master_data/data/Hummingbird_Master_combined_v6.csv"
SCRAPER_PATH = "hv_master_data/acreage_scripts/verified_acreage_enhanced.csv"
OSM_PATH     = "hv_master_data/acreage_scripts/osm_results.csv"
OUTPUT_PATH  = "hv_master_data/data/Hummingbird_Master_combined_v6.csv"
CHANGELOG    = f"hv_master_data/data/merge_changelog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# Enrollment threshold: at/above this, scraper is primary; below, OSM is primary
ENR_CUTOFF = 4000

# Confidence rank used to decide whether to overwrite existing data
CONF_RANK = {'HIGH': 3, 'MEDIUM': 2, 'LOW': 1, '': 0}

# Columns to pull from scraper → master column names
SCRAPER_COLUMNS = {
    'verified_acres': 'scraper_acres',
    'confidence':     'scraper_confidence',
    'source':         'scraper_source',
    'status':         'scraper_status',
    'notes':          'scraper_notes',
    'detected_type':  'scraper_detected_type',
}

# Columns to pull from OSM → master column names
OSM_COLUMNS = {
    'osm_acres':        'osm_acres',
    'osm_confidence':   'osm_confidence',
    'osm_feature_name': 'osm_feature_name',
    'osm_feature_type': 'osm_feature_type',
    'osm_source':       'osm_source',
}


# ── UNICODE CLEANUP ──────────────────────────────────────────────────────────
# Ordered replacements for common garbled UTF-8-in-latin1 sequences
_GARBLED = [
    (r'ÃÂÃÂ¢ÃÂÃÂÃÂ', '-'),
    (r'ÃÂÃÂ¢ÃÂÃÂ',   '-'),
    (r'ÃÂ¢ÃÂÃÂ',     '-'),
    (r'ÃÂ¢',          "'"),
    (r'ÃÂÃÂ',         ''),
    (r'ÃÂ',           ''),
    (r'Ã[À-ÿ]',       ''),
]
_GARBLED_RE = [(re.compile(p), r) for p, r in _GARBLED]

def fix_text(s):
    """Fix garbled unicode sequences and strip remaining non-ASCII."""
    if not s or not isinstance(s, str):
        return s
    for pattern, replacement in _GARBLED_RE:
        s = pattern.sub(replacement, s)
    s = re.sub(r'[^\x00-\x7F]+', ' ', s)   # strip any remaining non-ASCII
    s = re.sub(r'  +', ' ', s).strip()
    return s

# Columns to skip during unicode cleanup (numeric / ID fields)
_SKIP_CLEAN = {
    'unitid', 'unitid_clean', 'ein', 'ein_clean', 'uei', 'opeid',
    'latitude', 'longitude', 'zip_code',
}

def clean_unicode(df):
    """Apply fix_text to all object columns except numeric/ID fields."""
    cleaned = 0
    for col in df.select_dtypes(include='object').columns:
        if col.lower() in _SKIP_CLEAN:
            continue
        new_col = df[col].apply(lambda x: fix_text(x) if isinstance(x, str) else x)
        changed = (new_col != df[col]).sum()
        if changed:
            df[col] = new_col
            cleaned += changed
    return cleaned


# ── NAME MATCHING ────────────────────────────────────────────────────────────

def normalize(name):
    """Lowercase, strip non-ASCII, collapse whitespace."""
    if pd.isna(name):
        return ""
    s = re.sub(r'[^\x00-\x7F]+', ' ', str(name))
    return re.sub(r'\s+', ' ', s).strip().lower()


def match_to_master(source_df, source_name_col, master_df):
    """
    Multi-pass name match: source rows → master rows.
    Pass 1: exact normalized institution_name
    Pass 2: name_alias / exact_name columns
    Returns {source_idx: master_idx}
    """
    master_by_name = {}
    for idx, row in master_df.iterrows():
        key = normalize(row['institution_name'])
        master_by_name.setdefault(key, []).append(idx)

    master_by_alias = {}
    for idx, row in master_df.iterrows():
        for col in ['name_alias', 'exact_name']:
            if col in master_df.columns and pd.notna(row.get(col)):
                key = normalize(row[col])
                master_by_alias.setdefault(key, []).append(idx)

    def pick_best(candidates, s_state):
        if len(candidates) == 1:
            return candidates[0]
        for m_idx in candidates:
            if normalize(master_df.loc[m_idx].get('state', '')) == normalize(s_state):
                return m_idx
        return candidates[0]

    matches = {}
    for s_idx, s_row in source_df.iterrows():
        key     = normalize(s_row[source_name_col])
        s_state = s_row.get('state', '')
        if key in master_by_name:
            matches[s_idx] = pick_best(master_by_name[key], s_state)

    for s_idx in [i for i in source_df.index if i not in matches]:
        key = normalize(source_df.loc[s_idx, source_name_col])
        if key in master_by_alias:
            matches[s_idx] = master_by_alias[key][0]

    return matches


def should_update(old_val, old_conf, new_val, new_conf):
    """True if new data is worth writing (blank existing, or strictly higher confidence)."""
    if pd.isna(new_val) or str(new_val).strip() in ('', '0', '0.0', 'nan'):
        return False
    if str(old_val).strip() in ('', '0', '0.0', 'nan') or pd.isna(old_val):
        return True
    return CONF_RANK.get(str(new_conf).upper(), 0) > CONF_RANK.get(str(old_conf).upper(), 0)


def _sf(v, d=0.0):
    try:
        f = float(v)
        return f if not np.isnan(f) else d
    except (TypeError, ValueError):
        return d


# ── PRIMARY SOURCE LOGIC ─────────────────────────────────────────────────────

def assign_primary(row):
    """
    Returns (acreage_primary, acreage_primary_source) for a master row.
    IPEDS colleges: enrollment >= 4000 → scraper; < 4000 → OSM
    990 orgs: no primary assigned (both shown equally in map)
    """
    is_990  = str(row.get('data_source', '')) == 'Hummingbird_990'
    enr     = _sf(row.get('enrollment_2024', 0))
    s_ac    = _sf(row.get('scraper_acres'))
    o_ac    = _sf(row.get('osm_acres'))

    if is_990:
        return '', ''   # map shows both without flagging a primary

    # IPEDS college
    if enr >= ENR_CUTOFF:
        if s_ac > 0:
            return round(s_ac, 1), 'scraper'
        if o_ac > 0:
            return round(o_ac, 1), 'OSM'
    else:
        if o_ac > 0:
            return round(o_ac, 1), 'OSM'
        if s_ac > 0:
            return round(s_ac, 1), 'scraper'

    return '', ''


# ── MAIN ─────────────────────────────────────────────────────────────────────

def merge(master_path, scraper_path, osm_path, output_path, changelog_path):

    print(f'\n{"="*60}')
    print('Hummingbird Acreage Merge  v2')
    print(f'{"="*60}')

    # ── Load ─────────────────────────────────────────────────────────────
    print(f'\nLoading master:   {master_path}')
    master = pd.read_csv(master_path, low_memory=False,
                         keep_default_na=False, na_values=[])
    print(f'  {len(master):,} rows  |  {len(master.columns)} columns')

    print(f'Loading scraper:  {scraper_path}')
    scraper = pd.read_csv(scraper_path, low_memory=False)
    print(f'  {len(scraper):,} rows  |  {scraper["verified_acres"].notna().sum():,} with verified_acres')

    print(f'Loading OSM:      {osm_path}')
    osm = pd.read_csv(osm_path, low_memory=False)
    osm_with = osm['osm_acres'].apply(
        lambda x: str(x).strip() not in ('', 'nan', '0', '0.0')).sum()
    print(f'  {len(osm):,} rows  |  {osm_with:,} with osm_acres')

    # ── Column cleanup ────────────────────────────────────────────────────
    # Drop acreage_raw — unknown provenance, not recoverable
    if 'acreage_raw' in master.columns:
        master.drop(columns=['acreage_raw'], inplace=True)
        print('\n  Dropped: acreage_raw')

    # Rename v1 scraper columns that are already in master
    v1_renames = {
        'verified_acres':        'scraper_acres',
        'acreage_confidence':    'scraper_confidence',
        'acreage_source':        'scraper_source',
        'acreage_status':        'scraper_status',
        'acreage_notes':         'scraper_notes',
        'acreage_detected_type': 'scraper_detected_type',
    }
    renamed = []
    for old, new in v1_renames.items():
        if old in master.columns and new not in master.columns:
            master.rename(columns={old: new}, inplace=True)
            renamed.append(f'{old}→{new}')
    if renamed:
        print(f'  Renamed: {", ".join(renamed)}')

    # Ensure all new columns exist
    for col in list(SCRAPER_COLUMNS.values()) + list(OSM_COLUMNS.values()) + \
               ['acreage_primary', 'acreage_primary_source', 'acreage_conflict_flag']:
        if col not in master.columns:
            master[col] = ''

    # ── Unicode cleanup ───────────────────────────────────────────────────
    print('\nCleaning unicode...')
    n_cleaned = clean_unicode(master)
    print(f'  {n_cleaned:,} cell values cleaned')

    # ── Match + merge scraper ─────────────────────────────────────────────
    print('\nMatching scraper → master...')
    s_matches = match_to_master(scraper, 'name', master)
    print(f'  {len(s_matches):,} of {len(scraper):,} matched')

    s_updated = 0
    for s_i, m_i in s_matches.items():
        s_row    = scraper.loc[s_i]
        new_conf = str(s_row.get('confidence', '')).strip().upper()
        old_conf = str(master.at[m_i, 'scraper_confidence']).strip().upper()
        old_ac   = master.at[m_i, 'scraper_acres']

        if should_update(old_ac, old_conf, s_row.get('verified_acres'), new_conf):
            for src_col, dst_col in SCRAPER_COLUMNS.items():
                val = s_row.get(src_col)
                if pd.notna(val):
                    master.at[m_i, dst_col] = str(val)
            s_updated += 1
    print(f'  {s_updated:,} rows updated')

    # ── Match + merge OSM ─────────────────────────────────────────────────
    print('\nMatching OSM → master...')
    o_matches = match_to_master(osm, 'institution_name', master)
    print(f'  {len(o_matches):,} of {len(osm):,} matched')

    o_updated = 0
    for o_i, m_i in o_matches.items():
        o_row    = osm.loc[o_i]
        new_conf = str(o_row.get('osm_confidence', '')).strip().upper()
        old_conf = str(master.at[m_i, 'osm_confidence']).strip().upper()
        old_ac   = master.at[m_i, 'osm_acres']

        if should_update(old_ac, old_conf, o_row.get('osm_acres'), new_conf):
            for src_col, dst_col in OSM_COLUMNS.items():
                val = o_row.get(src_col)
                if pd.notna(val):
                    master.at[m_i, dst_col] = str(val)
            o_updated += 1
    print(f'  {o_updated:,} rows updated')

    # ── Assign primary + conflict flag ────────────────────────────────────
    print('\nAssigning primary acreage and flagging conflicts...')
    changelog  = []
    n_primary  = 0
    n_conflict = 0

    for m_i, row in master.iterrows():
        # Primary
        primary, primary_src = assign_primary(row)
        master.at[m_i, 'acreage_primary']        = primary
        master.at[m_i, 'acreage_primary_source']  = primary_src
        if primary:
            n_primary += 1

        # Conflict flag
        s_ac = _sf(row.get('scraper_acres'))
        o_ac = _sf(row.get('osm_acres'))
        flag = ''
        if s_ac > 0 and o_ac > 0:
            ratio = max(s_ac, o_ac) / min(s_ac, o_ac)
            if ratio > 3.0:
                flag = f'CONFLICT: scraper={s_ac:.0f} osm={o_ac:.0f} ratio={ratio:.1f}x'
                n_conflict += 1
        master.at[m_i, 'acreage_conflict_flag'] = flag

        # Changelog entry if primary changed
        old_primary = str(row.get('acreage_primary', '')).strip()
        if str(primary) != old_primary and primary:
            changelog.append({
                'institution_name':     row.get('institution_name', ''),
                'institution_type':     row.get('institution_type', ''),
                'enrollment_2024':      row.get('enrollment_2024', ''),
                'acreage_primary_new':  primary,
                'acreage_primary_src':  primary_src,
                'scraper_acres':        row.get('scraper_acres', ''),
                'scraper_confidence':   row.get('scraper_confidence', ''),
                'osm_acres':            row.get('osm_acres', ''),
                'osm_confidence':       row.get('osm_confidence', ''),
                'conflict_flag':        flag,
                'timestamp':            datetime.now().isoformat(),
            })

    print(f'  {n_primary:,} rows with primary acreage assigned')
    print(f'  {n_conflict:,} conflicts flagged (scraper vs OSM >3x)')

    # ── Summary ───────────────────────────────────────────────────────────
    total = len(master)
    has_scraper  = (master['scraper_acres'].apply(
                       lambda x: str(x).strip() not in ('', 'nan', '0'))).sum()
    has_osm      = (master['osm_acres'].apply(
                       lambda x: str(x).strip() not in ('', 'nan', '0'))).sum()
    has_both     = sum(
        1 for _, r in master.iterrows()
        if str(r.get('scraper_acres','')).strip() not in ('','nan','0') and
           str(r.get('osm_acres','')).strip() not in ('','nan','0')
    )
    scraper_primary = (master['acreage_primary_source'] == 'scraper').sum()
    osm_primary     = (master['acreage_primary_source'] == 'OSM').sum()

    print(f'\n{"="*60}')
    print(f'RESULTS')
    print(f'{"="*60}')
    print(f'  Total institutions:         {total:,}')
    print(f'  With scraper_acres:         {has_scraper:,}')
    print(f'  With osm_acres:             {has_osm:,}')
    print(f'  With both sources:          {has_both:,}')
    print(f'  With acreage_primary set:   {n_primary:,}  ({n_primary/total*100:.1f}%)')
    print(f'    scraper is primary:       {scraper_primary:,}')
    print(f'    OSM is primary:           {osm_primary:,}')
    print(f'  Conflicts flagged:          {n_conflict:,}')
    print(f'{"="*60}')

    # ── Save ──────────────────────────────────────────────────────────────
    master.to_csv(output_path, index=False)
    print(f'\n  Master saved: {output_path}')

    if changelog:
        pd.DataFrame(changelog).to_csv(changelog_path, index=False)
        print(f'  Changelog:    {changelog_path}  ({len(changelog):,} rows)')

    # Unmatched scraper rows with data
    unmatched = scraper[
        (~scraper.index.isin(s_matches.keys())) &
        (scraper['verified_acres'].notna())
    ][['name', 'city', 'state', 'verified_acres', 'confidence', 'source']]
    if len(unmatched):
        ts      = datetime.now().strftime('%Y%m%d_%H%M%S')
        um_path = f"hv_master_data/data/unmatched_scraper_{ts}.csv"
        unmatched.to_csv(um_path, index=False)
        print(f'  Unmatched:    {um_path}  ({len(unmatched):,} rows)')

    return master


# ── RUN ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    merge(MASTER_PATH, SCRAPER_PATH, OSM_PATH, OUTPUT_PATH, CHANGELOG)