"""
hifld_acreage_merge.py — HIFLD Campus Polygon Acreage Integration
=================================================================
Computes campus acreage from HIFLD polygon GeoJSON and merges into
the Hummingbird Master CSV.

Adds columns:
    hifld_acres         — computed polygon area in acres
    hifld_match_method  — 'exact_norm' | 'no_match'

Updates columns:
    acreage_primary        — best acreage estimate (see priority rules)
    acreage_primary_source — source of acreage_primary
    acreage_conflict_flag  — True if sources diverge >30% AND >20 acres

PRIORITY RULES:
    acreage_primary source priority: HIFLD > scraper > OSM
    Exception — enrollment-based scraper/OSM crossover still applies
    when HIFLD is absent:
        enrollment >= 5000 → scraper preferred over OSM
        enrollment <  5000 → OSM preferred over scraper

    HIFLD is treated as authoritative because it is a measured polygon
    boundary, not a self-reported or web-scraped value.

    HIFLD values < 5 acres are flagged but still stored — they likely
    represent a building footprint (branch/satellite campus) rather than
    a full campus boundary. acreage_primary will not use HIFLD in this
    case unless it is the only source available.

USAGE:
    python hifld_acreage_merge.py

INPUTS:
    hv_master_data/data/Hummingbird_Master_Combined_v6.csv
    hifld_campus/colleges-and-universities-campuses-geojson.geojson

OUTPUT:
    hv_master_data/data/Hummingbird_Master_Combined_v6.csv  (in-place update)
    hifld_acreage_matched.csv  (audit file — all matches and scores)
"""

import csv
import json
import math
import re
import os
from collections import defaultdict

csv.field_size_limit(100 * 1024 * 1024)

# =============================================================================
# CONFIGURATION
# =============================================================================

MASTER_FILE  = 'hv_master_data/data/Hummingbird_Master_Combined_v6.csv'
HIFLD_FILE   = 'hv_master_data/acreage_scripts/colleges-and-universities-campuses-geojson.geojson'
AUDIT_FILE   = 'hv_master_data/data/hifld_acreage_audit.csv'

# Enrollment threshold: above this, scraper is preferred over OSM when HIFLD absent
ENROLLMENT_SCRAPER_THRESHOLD = 5000

# HIFLD values below this are likely building footprints, not full campuses
HIFLD_MIN_MEANINGFUL_ACRES = 5.0

# Conflict flag threshold: sources diverge by more than this fraction AND more than this absolute
CONFLICT_PCT_THRESHOLD  = 0.30   # 30%
CONFLICT_ABS_THRESHOLD  = 20.0   # 20 acres

# =============================================================================
# STATE ABBREVIATION MAP
# =============================================================================

STATE_ABBR = {
    'Alabama':'AL','Alaska':'AK','Arizona':'AZ','Arkansas':'AR','California':'CA',
    'Colorado':'CO','Connecticut':'CT','Delaware':'DE','Florida':'FL','Georgia':'GA',
    'Hawaii':'HI','Idaho':'ID','Illinois':'IL','Indiana':'IN','Iowa':'IA','Kansas':'KS',
    'Kentucky':'KY','Louisiana':'LA','Maine':'ME','Maryland':'MD','Massachusetts':'MA',
    'Michigan':'MI','Minnesota':'MN','Mississippi':'MS','Missouri':'MO','Montana':'MT',
    'Nebraska':'NE','Nevada':'NV','New Hampshire':'NH','New Jersey':'NJ','New Mexico':'NM',
    'New York':'NY','North Carolina':'NC','North Dakota':'ND','Ohio':'OH','Oklahoma':'OK',
    'Oregon':'OR','Pennsylvania':'PA','Rhode Island':'RI','South Carolina':'SC',
    'South Dakota':'SD','Tennessee':'TN','Texas':'TX','Utah':'UT','Vermont':'VT',
    'Virginia':'VA','Washington':'WA','West Virginia':'WV','Wisconsin':'WI','Wyoming':'WY',
    'District of Columbia':'DC','Puerto Rico':'PR','Guam':'GU','Virgin Islands':'VI',
}

# =============================================================================
# HELPERS
# =============================================================================

def normalize_name(s):
    """Normalize institution name for fuzzy matching."""
    s = s.upper().strip()
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    for old, new in [
        ('UNIVERSITY', 'UNIV'), ('COLLEGE', 'COLL'), ('COMMUNITY', 'COMM'),
        ('TECHNICAL', 'TECH'), ('STATE', 'ST'), (' THE ', '  '),
        ('INSTITUTE', 'INST'), ('TECHNOLOGY', 'TECH'),
    ]:
        s = s.replace(old, new)
    return re.sub(r'\s+', ' ', s).strip()


def shoelace_m2(ring):
    """Shoelace polygon area in square meters (EPSG:3857 coords)."""
    n = len(ring)
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += ring[i][0] * ring[j][1]
        area -= ring[j][0] * ring[i][1]
    return abs(area) / 2.0


def safe_float(v):
    if v in ('', 'nan', 'None', None):
        return None
    try:
        x = float(v)
        return None if (math.isnan(x) or math.isinf(x)) else x
    except:
        return None


def conflict_flag(a, b, c):
    """True if any two non-None values diverge beyond thresholds."""
    vals = [v for v in [a, b, c] if v is not None and v > 0]
    if len(vals) < 2:
        return False
    mx, mn = max(vals), min(vals)
    if mx == 0:
        return False
    pct_diff = (mx - mn) / mx
    abs_diff = mx - mn
    return pct_diff > CONFLICT_PCT_THRESHOLD and abs_diff > CONFLICT_ABS_THRESHOLD


# =============================================================================
# LOAD AND COMPUTE HIFLD
# =============================================================================

def load_hifld(path):
    print(f"Loading HIFLD: {path}")
    with open(path, encoding='utf-8') as f:
        data = json.load(f)

    hifld = []
    multi_count = 0
    SQM_TO_ACRES = 0.000247105

    for feat in data['features']:
        p    = feat['properties']
        geom = feat['geometry']
        if not geom:
            continue

        if geom['type'] == 'Polygon':
            m2 = shoelace_m2(geom['coordinates'][0])
            ring = geom['coordinates'][0]
        elif geom['type'] == 'MultiPolygon':
            m2 = sum(shoelace_m2(poly[0]) for poly in geom['coordinates'])
            ring = geom['coordinates'][0][0]
            multi_count += 1
        else:
            continue

        acres = round(m2 * SQM_TO_ACRES, 1)
        hifld.append({
            'name':       p.get('NAME', '').strip(),
            'state_abbr': p.get('STATE', '').strip(),
            'city':       p.get('CITY', '').strip(),
            'acres':      acres,
            'norm':       normalize_name(p.get('NAME', '')),
            'is_footprint': acres < HIFLD_MIN_MEANINGFUL_ACRES,
        })

    print(f"  Loaded {len(hifld):,} features ({multi_count:,} MultiPolygon)")
    return hifld


# =============================================================================
# MATCH
# =============================================================================

def build_lookup(hifld):
    """Build dict keyed by (norm_name, state_abbr)."""
    lookup = {}
    for h in hifld:
        key = (h['norm'], h['state_abbr'])
        # Keep largest polygon if duplicate names in same state
        if key not in lookup or h['acres'] > lookup[key]['acres']:
            lookup[key] = h
    return lookup


def match_institution(name, state_full, lookup):
    state_abbr = STATE_ABBR.get(state_full, state_full[:2].upper() if state_full else '')
    norm = normalize_name(name)
    h = lookup.get((norm, state_abbr))
    if h:
        return h, 'exact_norm'
    return None, 'no_match'


# =============================================================================
# PRIORITY: pick acreage_primary from HIFLD / scraper / OSM
# =============================================================================

def pick_primary(hifld_ac, scraper_ac, osm_ac, enrollment):
    """
    Returns (primary_acres, source_label).

    Priority:
      1. HIFLD — if present and above footprint threshold
      2. Scraper — if enrollment >= threshold (scraper better for large campuses)
      3. OSM — if enrollment < threshold (OSM better for small/urban campuses)
      4. Whatever single source exists
      5. HIFLD even if footprint-sized (last resort)
    """
    enr = enrollment or 0

    # HIFLD meaningful
    if hifld_ac is not None and hifld_ac >= HIFLD_MIN_MEANINGFUL_ACRES:
        return round(hifld_ac, 1), 'HIFLD'

    # No HIFLD — apply enrollment-based scraper vs OSM rule
    if scraper_ac and osm_ac:
        if enr >= ENROLLMENT_SCRAPER_THRESHOLD:
            return round(scraper_ac, 1), 'scraper'
        else:
            return round(osm_ac, 1), 'OSM'

    if scraper_ac:
        return round(scraper_ac, 1), 'scraper'
    if osm_ac:
        return round(osm_ac, 1), 'OSM'

    # HIFLD footprint as last resort
    if hifld_ac is not None:
        return round(hifld_ac, 1), 'HIFLD_footprint'

    return None, None


# =============================================================================
# MAIN
# =============================================================================

def main():
    # Load HIFLD
    hifld = load_hifld(HIFLD_FILE)
    lookup = build_lookup(hifld)

    # Load master
    print(f"\nLoading master: {MASTER_FILE}")
    with open(MASTER_FILE, encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    print(f"  {len(rows):,} rows, {len(fieldnames)} columns")

    # Ensure new columns exist
    new_cols = ['hifld_acres', 'hifld_match_method']
    for col in new_cols:
        if col not in fieldnames:
            fieldnames = list(fieldnames) + [col]

    # Process
    stats = {'exact': 0, 'no_match': 0, 'hifld_primary': 0,
             'scraper_primary': 0, 'osm_primary': 0, 'footprint': 0,
             'conflict': 0, 'gap_filled': 0}

    audit_rows = []

    for row in rows:
        name       = row.get('institution_name', '')
        state      = row.get('state', '')
        enr        = safe_float(row.get('enrollment_2024'))
        scraper_ac = safe_float(row.get('scraper_acres'))
        osm_ac     = safe_float(row.get('osm_acres'))
        had_primary = bool(safe_float(row.get('acreage_primary')))

        # Match HIFLD
        h, method = match_institution(name, state, lookup)

        if h:
            stats['exact'] += 1
            row['hifld_acres']        = h['acres']
            row['hifld_match_method'] = method
            if h['is_footprint']:
                stats['footprint'] += 1
            hifld_ac = h['acres']
        else:
            stats['no_match'] += 1
            row['hifld_acres']        = ''
            row['hifld_match_method'] = 'no_match'
            hifld_ac = None

        # Pick primary
        primary, source = pick_primary(hifld_ac, scraper_ac, osm_ac, enr)

        if primary is not None:
            row['acreage_primary']        = primary
            row['acreage_primary_source'] = source

            if source == 'HIFLD':
                stats['hifld_primary'] += 1
            elif source == 'scraper':
                stats['scraper_primary'] += 1
            elif source in ('OSM', 'HIFLD_footprint'):
                stats['osm_primary'] += 1

            if not had_primary:
                stats['gap_filled'] += 1

            # Conflict flag — compare all three sources
            flag = conflict_flag(hifld_ac, scraper_ac, osm_ac)
            row['acreage_conflict_flag'] = 'True' if flag else ''
            if flag:
                stats['conflict'] += 1
        else:
            row['acreage_primary']        = row.get('acreage_primary', '')
            row['acreage_primary_source'] = row.get('acreage_primary_source', '')
            row['acreage_conflict_flag']  = ''

        # Audit row
        audit_rows.append({
            'institution_name':   name,
            'state':              state,
            'hifld_acres':        row['hifld_acres'],
            'hifld_match_method': method,
            'scraper_acres':      scraper_ac or '',
            'osm_acres':          osm_ac or '',
            'acreage_primary':    row.get('acreage_primary', ''),
            'acreage_source':     row.get('acreage_primary_source', ''),
            'conflict_flag':      row.get('acreage_conflict_flag', ''),
            'enrollment_2024':    enr or '',
        })

    # Write master
    print(f"\nWriting updated master: {MASTER_FILE}")
    with open(MASTER_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

    # Write audit
    print(f"Writing audit file: {AUDIT_FILE}")
    with open(AUDIT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(audit_rows[0].keys()))
        writer.writeheader()
        writer.writerows(audit_rows)

    # Summary
    total = len(rows)
    print(f"\n{'='*55}")
    print(f"HIFLD MERGE COMPLETE")
    print(f"{'='*55}")
    print(f"  HIFLD matches:        {stats['exact']:>6,} ({stats['exact']/total*100:.1f}%)")
    print(f"    of which footprint: {stats['footprint']:>6,} (< {HIFLD_MIN_MEANINGFUL_ACRES} ac, likely satellite)")
    print(f"  No match:             {stats['no_match']:>6,} ({stats['no_match']/total*100:.1f}%)")
    print(f"\nacreage_primary source breakdown:")
    print(f"  HIFLD:                {stats['hifld_primary']:>6,}")
    print(f"  Scraper:              {stats['scraper_primary']:>6,}")
    print(f"  OSM:                  {stats['osm_primary']:>6,}")
    print(f"\n  Gaps filled by HIFLD: {stats['gap_filled']:>6,} (had no prior acreage)")
    print(f"  Conflict flags:       {stats['conflict']:>6,}")
    print(f"{'='*55}")


if __name__ == '__main__':
    main()