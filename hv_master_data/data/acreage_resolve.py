"""
acreage_resolve.py  v2
======================
Single master-merging script for all acreage data.

Merges three sources into master CSV, then resolves primary acreage,
computes a weighted average, and refreshes land flags.

Steps (in order):
    1. Merge verified_acreage_enhanced.csv  → scraper_acres, scraper_confidence, etc.
    2. Merge osm_results.csv                → osm_acres, osm_confidence, etc.
    3. HIFLD polygon match                  → hifld_acres, hifld_match_method
    4. Resolve acreage_primary              → consensus/inflation/fallback logic
    5. Compute acreage_weighted             → average of agreeing sources (new column)
    6. Recompute land flags                 → flag_land_potential, flag_high_land_potential

Inputs:
    hv_master_data/data/Hummingbird_Master_Combined_v6.csv
    hv_master_data/acreage_scripts/verified_acreage_enhanced.csv
    hv_master_data/acreage_scripts/osm_results.csv
    hv_master_data/acreage_scripts/colleges-and-universities-campuses-geojson.geojson

Output:
    hv_master_data/data/Hummingbird_Master_Combined_v6.csv  (in-place)
    hv_master_data/data/acreage_audit.csv
"""

import csv, json, math, re, os
from difflib import SequenceMatcher

csv.field_size_limit(100 * 1024 * 1024)

# =============================================================================
# PATHS
# =============================================================================

MASTER_FILE   = 'hv_master_data/data/Hummingbird_Master_Combined_v6.csv'
SCRAPER_FILE  = 'hv_master_data/acreage_scripts/verified_acreage_enhanced.csv'
OSM_FILE      = 'hv_master_data/acreage_scripts/osm_results.csv'
HIFLD_FILE    = 'hv_master_data/acreage_scripts/colleges-and-universities-campuses-geojson.geojson'
AUDIT_FILE    = 'hv_master_data/data/acreage_audit.csv'

# =============================================================================
# CONFIGURATION
# =============================================================================

HIFLD_MIN_ACRES          = 5.0
HIFLD_EXTENDED_FP        = 20.0
HIFLD_MAX_ACRES          = 50_000
HIFLD_OVERBOUNDARY_RATIO = 1.8   # HIFLD > scraper x this AND scraper HIGH → HIFLD grabbed extra land
SCRAPER_INFLATION_RATIO  = 2.5   # lowered from 3.0: catches scraper grabbing adjacent nature preserves   # lowered from 3.0: catches scraper grabbing adjacent nature preserves (e.g. Juniata 2.18x)
CONSENSUS_RATIO         = 2.0
WEIGHTED_AGREE_PCT      = 0.15   # sources within 15% are considered agreeing for weighted avg
CONFLICT_PCT            = 0.30
CONFLICT_ABS            = 20.0
ZIP_FUZZY_MIN           = 0.90
CITY_TOKEN_MIN          = 0.85
ENROLLMENT_THRESHOLD    = 5000   # above → prefer scraper over OSM when HIFLD absent

VALID_OSM_FEATURE_TYPES = {
    'university', 'college', 'education', 'recreation_ground',
    'camp_site', 'institutional', 'public', 'grass',
}

# Land flag thresholds
LAND_POTENTIAL_MIN      = 50.0   # acres
HIGH_LAND_MIN           = 200.0  # acres
HIGH_LAND_URBAN         = {'Rural', 'Town'}

# =============================================================================
# STATE MAP
# =============================================================================

STATE_ABBR = {
    'Alabama':'AL','Alaska':'AK','Arizona':'AZ','Arkansas':'AR','California':'CA',
    'Colorado':'CO','Connecticut':'CT','Delaware':'DE','Florida':'FL','Georgia':'GA',
    'Hawaii':'HI','Idaho':'ID','Illinois':'IL','Indiana':'IN','Iowa':'IA',
    'Kansas':'KS','Kentucky':'KY','Louisiana':'LA','Maine':'ME','Maryland':'MD',
    'Massachusetts':'MA','Michigan':'MI','Minnesota':'MN','Mississippi':'MS',
    'Missouri':'MO','Montana':'MT','Nebraska':'NE','Nevada':'NV',
    'New Hampshire':'NH','New Jersey':'NJ','New Mexico':'NM','New York':'NY',
    'North Carolina':'NC','North Dakota':'ND','Ohio':'OH','Oklahoma':'OK',
    'Oregon':'OR','Pennsylvania':'PA','Rhode Island':'RI','South Carolina':'SC',
    'South Dakota':'SD','Tennessee':'TN','Texas':'TX','Utah':'UT','Vermont':'VT',
    'Virginia':'VA','Washington':'WA','West Virginia':'WV','Wisconsin':'WI',
    'Wyoming':'WY','District of Columbia':'DC','Puerto Rico':'PR',
    'Guam':'GU','Virgin Islands':'VI',
}

def norm_state(s):
    if not s: return ''
    s = s.strip()
    return STATE_ABBR.get(s, s.upper()[:2])

# =============================================================================
# HELPERS
# =============================================================================

def sf(v):
    if v in ('', 'nan', 'None', None): return None
    try:
        x = float(v)
        return None if (math.isnan(x) or math.isinf(x) or x <= 0) else x
    except: return None

def norm_name(s):
    if not s: return ''
    s = str(s).upper().strip()
    s = re.sub(r'[^A-Z0-9 ]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

def token_set(s):
    stopwords = {'THE','OF','AT','IN','AND','A','AN'}
    return {t for t in s.split() if t not in stopwords and len(t) > 2}

def token_overlap(a_norm, b_norm, city_match=False):
    a, b = token_set(a_norm), token_set(b_norm)
    if not a or not b: return 0.0
    score = len(a & b) / max(len(a), len(b))
    return score + (0.20 if city_match else 0)

def shoelace_m2(ring):
    n = len(ring)
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += ring[i][0] * ring[j][1]
        area -= ring[j][0] * ring[i][1]
    return abs(area) / 2.0

SQM_TO_ACRES = 0.000247105

# =============================================================================
# STEP 1 — BUILD SCRAPER LOOKUP from verified_acreage_enhanced.csv
# =============================================================================

def load_scraper(path):
    """
    Returns dict keyed by (name, city, state) → row.
    Falls back to name-only key for rows missing city/state.
    """
    if not os.path.exists(path):
        print(f'  WARNING: scraper file not found: {path}')
        return {}, {}

    by_composite = {}
    by_name      = {}

    with open(path, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            name  = row.get('name', '').strip()
            city  = row.get('city', '').strip()
            state = row.get('state', '').strip()
            acres = sf(row.get('verified_acres'))
            conf  = row.get('confidence', '').strip()

            if not name or acres is None:
                continue
            if conf.upper() not in ('HIGH', 'MEDIUM', 'LOW'):
                continue

            key_composite = (name, city, state)
            by_composite[key_composite] = row
            # Also index by name alone as fallback
            if name not in by_name:
                by_name[name] = row

    print(f'  Scraper: {len(by_composite):,} valid rows loaded')
    return by_composite, by_name


def merge_scraper(rows, by_composite, by_name):
    """Merge scraper data into master rows. Always overwrites to fix collision issues."""
    filled = 0
    for row in rows:
        name  = row.get('institution_name', '').strip()
        city  = row.get('city', '').strip()
        state = row.get('state', '').strip()

        # Always try composite key first — DO NOT fall back to name-only
        # Name-only fallback causes collisions (Sterling KS data → Sterling VT)
        s = by_composite.get((name, city, state))
        if not s:
            # Only use name fallback if no collision risk (unique name)
            candidates = [k for k in by_composite if k[0] == name]
            if len(candidates) == 1:
                s = by_composite[candidates[0]]
        if not s:
            continue

        acres = sf(s.get('verified_acres'))
        if not acres:
            continue

        row['scraper_acres']        = acres
        row['scraper_confidence']   = s.get('confidence', '')
        row['scraper_source']       = s.get('source', '')
        row['scraper_status']       = s.get('status', '')
        row['scraper_notes']        = s.get('notes', '')
        row['scraper_detected_type']= s.get('detected_type', '')
        filled += 1

    print(f'  Scraper merge: {filled:,} new rows filled')
    return rows


# =============================================================================
# STEP 2 — BUILD OSM LOOKUP from osm_results.csv
# =============================================================================

def load_osm(path):
    """
    Returns (by_composite, by_name) dicts for OSM acreage.
    by_composite keyed by (name, city, state) using lat/lon proximity to master.
    by_name keyed by institution_name as fallback.
    master_loc: dict of {institution_name: (city, state, lat, lon)} from master.
    """
    if not os.path.exists(path):
        print(f'  WARNING: OSM file not found: {path}')
        return {}, {}

    by_composite = {}
    by_name = {}
    # osm_results.csv now has city/state columns — use them directly
    with open(path, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            name  = row.get('institution_name', '').strip()
            city  = row.get('city', '').strip()
            state = row.get('state', '').strip()
            if not name or not sf(row.get('osm_acres')):
                continue
            by_name[name] = row
            by_composite[(name, city, state)] = row

    print(f'  OSM acreage: {len(by_name):,} valid rows  ({len(by_composite):,} composite keys)')
    return by_composite, by_name


def merge_osm(rows, by_composite, by_name):
    """Merge OSM acreage into master rows. Always overwrites. Uses composite key."""
    filled = 0
    for row in rows:
        name  = row.get('institution_name', '').strip()
        city  = row.get('city', '').strip()
        state = row.get('state', '').strip()

        # Try composite key first, then name only if unambiguous
        o = by_composite.get((name, city, state))
        if not o:
            candidates = [k for k in by_composite if k[0] == name]
            if len(candidates) == 1:
                o = by_composite[candidates[0]]
            elif not candidates:
                o = by_name.get(name)
        if not o:
            continue

        acres = sf(o.get('osm_acres'))
        if not acres:
            continue

        row['osm_acres']        = acres
        row['osm_confidence']   = o.get('osm_confidence', '')
        row['osm_feature_name'] = o.get('osm_feature_name', '')
        row['osm_feature_type'] = o.get('osm_feature_type', '')
        row['osm_source']       = o.get('osm_source', '')
        filled += 1

    print(f'  OSM merge: {filled:,} new rows filled')
    return rows


# =============================================================================
# STEP 3 — HIFLD MATCHING
# =============================================================================

def load_hifld(path):
    print(f'Loading HIFLD: {path}')
    with open(path, encoding='utf-8') as f:
        gj = json.load(f)

    records = []
    multi_count = 0
    for feat in gj['features']:
        p    = feat['properties']
        geom = feat['geometry']
        if not geom: continue
        if geom['type'] == 'Polygon':
            m2 = shoelace_m2(geom['coordinates'][0])
        elif geom['type'] == 'MultiPolygon':
            m2 = sum(shoelace_m2(poly[0]) for poly in geom['coordinates'])
            multi_count += 1
        else:
            continue
        acres   = round(m2 * SQM_TO_ACRES, 1)
        name    = p.get('NAME', '').strip()
        state   = p.get('STATE', '').strip()
        city    = p.get('CITY', '').strip().upper()
        zipcode = str(p.get('ZIP', '') or '').strip()[:5].zfill(5)
        records.append({
            'name': name, 'state': state, 'city': city,
            'zip': zipcode, 'acres': acres, 'norm': norm_name(name),
        })

    print(f'  Loaded {len(records):,} features ({multi_count:,} MultiPolygon)')
    return records


def build_hifld_indexes(records):
    by_norm_state, by_zip, by_city_state = {}, {}, {}
    for rec in records:
        key = (rec['norm'], rec['state'])
        if key not in by_norm_state or rec['acres'] > by_norm_state[key]['acres']:
            by_norm_state[key] = rec
        by_zip.setdefault(rec['zip'], []).append(rec)
        by_city_state.setdefault((rec['city'], rec['state']), []).append(rec)
    return by_norm_state, by_zip, by_city_state


def match_hifld(name, state_full, city, zipcode, indexes):
    by_norm_state, by_zip, by_city_state = indexes
    state_abbr = norm_state(state_full)
    name_n     = norm_name(name)
    city_n     = (city or '').strip().upper()
    zip5       = str(zipcode or '').strip()[:5].zfill(5)

    rec = by_norm_state.get((name_n, state_abbr))
    if rec: return rec, 'exact_norm'

    best_s, best_r = 0.0, None
    for c in by_zip.get(zip5, []):
        s = SequenceMatcher(None, name_n, c['norm']).ratio()
        if s > best_s: best_s, best_r = s, c
    if best_s >= ZIP_FUZZY_MIN: return best_r, 'zip_fuzzy'

    best_s, best_r = 0.0, None
    for c in by_city_state.get((city_n, state_abbr), []):
        s = token_overlap(name_n, c['norm'], city_match=True)
        if s > best_s: best_s, best_r = s, c
    if best_s >= CITY_TOKEN_MIN: return best_r, 'city_token'

    return None, 'no_match'


# =============================================================================
# STEP 4+5 — PRIMARY SELECTION + WEIGHTED AVERAGE
# =============================================================================

def validate_sources(hifld_ac, scraper_ac, scraper_conf, osm_ac, osm_conf, osm_ftype):
    valid = {}
    # HIFLD over-boundary: flag when HIFLD is 1.8x+ above HIGH-confidence scraper
    # Audit showed this pattern means HIFLD grabbed adjacent land beyond campus
    hifld_ob = (hifld_ac is not None and scraper_ac and
                str(scraper_conf).upper() == 'HIGH' and
                hifld_ac > scraper_ac * HIFLD_OVERBOUNDARY_RATIO)
    if hifld_ac is not None and HIFLD_MIN_ACRES <= hifld_ac <= HIFLD_MAX_ACRES:
        valid['hifld'] = hifld_ac
        if hifld_ob:
            valid['_hifld_overboundary'] = True  # sentinel — skipped by _authoritative
    if scraper_ac and str(scraper_conf).upper() in ('HIGH', 'MEDIUM'):
        valid['scraper'] = scraper_ac
    if (osm_ac and str(osm_conf).upper() == 'HIGH'
            and str(osm_ftype).lower() in VALID_OSM_FEATURE_TYPES):
        valid['osm'] = osm_ac
    return valid


def compute_weighted(hifld_ac, scraper_ac, osm_ac):
    """
    Weighted average of sources that agree within WEIGHTED_AGREE_PCT (15%).
    If 2+ sources agree → average them.
    If all 3 disagree → return None (no reliable consensus).
    Priority for which pair to use if only 2 agree: HIFLD+scraper > HIFLD+OSM > scraper+OSM.
    """
    sources = {}
    if hifld_ac: sources['hifld']   = hifld_ac
    if scraper_ac: sources['scraper'] = scraper_ac
    if osm_ac: sources['osm']      = osm_ac

    if len(sources) < 2:
        return None, None

    def agree(a, b):
        if not a or not b: return False
        hi, lo = max(a, b), min(a, b)
        return (hi - lo) / hi <= WEIGHTED_AGREE_PCT

    # Check all pairs in priority order
    pairs = [
        ('hifld', 'scraper'),
        ('hifld', 'osm'),
        ('scraper', 'osm'),
    ]
    # Also check all 3
    vals = list(sources.values())
    if len(vals) == 3:
        hi, lo = max(vals), min(vals)
        if (hi - lo) / hi <= WEIGHTED_AGREE_PCT:
            avg = round(sum(vals) / 3, 1)
            return avg, 'all_3_agree'

    for k1, k2 in pairs:
        v1, v2 = sources.get(k1), sources.get(k2)
        if v1 and v2 and agree(v1, v2):
            avg = round((v1 + v2) / 2, 1)
            return avg, f'{k1}+{k2}'

    return None, None


def resolve_primary(hifld_ac, scraper_ac, scraper_conf, osm_ac, osm_conf, osm_ftype, enrollment):
    """Returns (primary, source, tier, conflict_str, inflation_flag)"""
    scraper_inflation = False
    conflict_str = ''

    valid = validate_sources(hifld_ac, scraper_ac, scraper_conf, osm_ac, osm_conf, osm_ftype)

    # Scraper inflation check
    if 'scraper' in valid and len(valid) >= 2:
        others = [v for k, v in valid.items() if k != 'scraper' and not k.startswith('_')]
        other_med = sorted(others)[len(others) // 2]
        if valid['scraper'] > other_med * SCRAPER_INFLATION_RATIO:
            scraper_inflation = True
            del valid['scraper']

    vals = [v for k, v in valid.items() if not k.startswith('_')]

    if len(vals) == 0:
        return _fallback(hifld_ac, scraper_ac, scraper_conf,
                         osm_ac, osm_conf, osm_ftype, scraper_inflation)

    if len(vals) >= 2:
        hi, lo = max(vals), min(vals)
        ratio = hi / lo if lo > 0 else float('inf')
        if ratio <= CONSENSUS_RATIO:
            source, acres = _authoritative(valid)
            return round(acres, 1), source, 'consensus', '', scraper_inflation
        else:
            parts = [f'{k}={v:.0f}ac' for k, v in sorted(valid.items()) if not k.startswith('_')]
            hifld_ob = valid.get('_hifld_overboundary', False)
            ob_note = ', HIFLD_overboundary' if hifld_ob else ''
            conflict_str = f"CONFLICT({', '.join(parts)}, ratio={ratio:.1f}x{ob_note})"
            source, acres = _authoritative(valid)
            return round(acres, 1), source, 'conflict', conflict_str, scraper_inflation

    source, acres = _authoritative(valid)
    if source == 'HIFLD':
        tier = 'single_high'
    elif source == 'scraper':
        tier = 'single_high' if str(scraper_conf).upper() == 'HIGH' else 'single_medium'
    else:
        tier = 'single_high'
    return round(acres, 1), source, tier, '', scraper_inflation


def _authoritative(valid):
    for k, label in [('hifld','HIFLD'),('scraper','scraper'),('osm','OSM')]:
        if k in valid:
            return label, valid[k]
    return '', 0.0


def _fallback(hifld_ac, scraper_ac, scraper_conf, osm_ac, osm_conf, osm_ftype, inflated):
    # Scraper-first fallback, consistent with new audit-driven priority
    if scraper_ac and not inflated:
        tier = 'single_high' if str(scraper_conf).upper() == 'HIGH' else 'single_medium'
        return round(scraper_ac, 1), 'scraper', tier, '', inflated
    if osm_ac and str(osm_conf).upper() == 'HIGH':
        return round(osm_ac, 1), 'OSM', 'single_high', '', inflated
    if osm_ac:
        return round(osm_ac, 1), 'OSM', 'single_medium', '', inflated
    if hifld_ac and hifld_ac > 0:
        return round(hifld_ac, 1), 'HIFLD', 'single_high', '', inflated
    return None, '', 'none', '', inflated


# =============================================================================
# STEP 6 — LAND FLAGS
# =============================================================================

def recompute_land_flags(row):
    """Recompute flag_land_potential and flag_high_land_potential from acreage_primary."""
    ac  = sf(row.get('acreage_primary'))
    urb = (row.get('urbanization') or '').split(':')[0].strip()

    if ac and ac >= LAND_POTENTIAL_MIN:
        row['flag_land_potential'] = '1'
    else:
        row['flag_land_potential'] = '0'

    if ac and ac >= HIGH_LAND_MIN and urb in HIGH_LAND_URBAN:
        row['flag_high_land_potential'] = '1'
    else:
        row['flag_high_land_potential'] = '0'

    return row


# =============================================================================
# MAIN
# =============================================================================

def main():
    # ── Step 1: Load scraper data ─────────────────────────────────────────────
    print(f'\nStep 1: Loading scraper data...')
    scraper_composite, scraper_name = load_scraper(SCRAPER_FILE)

    # ── Step 2: Load OSM data ─────────────────────────────────────────────────
    # ── Step 2: Load HIFLD (OSM loaded after master so we can pass loc lookup) ─
    print(f'\nStep 2: Loading HIFLD polygon data...')
    hifld_records = load_hifld(HIFLD_FILE)
    hifld_indexes = build_hifld_indexes(hifld_records)

    # ── Load master ───────────────────────────────────────────────────────────
    print(f'\nLoading master: {MASTER_FILE}')
    with open(MASTER_FILE, encoding='utf-8', errors='replace') as f:
        reader     = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        rows       = list(reader)
    print(f'  {len(rows):,} rows, {len(fieldnames)} columns')

    # Ensure all output columns exist
    new_cols = [
        'scraper_acres', 'scraper_confidence', 'scraper_source',
        'scraper_status', 'scraper_notes', 'scraper_detected_type',
        'osm_acres', 'osm_confidence', 'osm_feature_name',
        'osm_feature_type', 'osm_source',
        'hifld_acres', 'hifld_match_method',
        'acreage_primary', 'acreage_primary_source',
        'acreage_conflict_flag', 'acreage_reliability_tier',
        'acreage_weighted', 'acreage_weighted_basis',
        'scraper_inflation_flag',
        'flag_land_potential', 'flag_high_land_potential',
    ]
    for col in new_cols:
        if col not in fieldnames:
            fieldnames.append(col)

    # ── Merge scraper ─────────────────────────────────────────────────────────
    # ── Load OSM acreage (after master so we can pass name→loc lookup) ─────────
    print(f'\nStep 3: Loading OSM acreage data...')
    osm_composite, osm_by_name = load_osm(OSM_FILE)

    print(f'\nMerging scraper data...')
    rows = merge_scraper(rows, scraper_composite, scraper_name)

    # ── Merge OSM ─────────────────────────────────────────────────────────────
    print(f'\nMerging OSM acreage data...')
    rows = merge_osm(rows, osm_composite, osm_by_name)

    # ── HIFLD + resolve + weighted ────────────────────────────────────────────
    print(f'\nStep 4-6: HIFLD matching, primary resolution, weighted average, land flags...')

    stats = {
        'exact_norm': 0, 'zip_fuzzy': 0, 'city_token': 0, 'no_match': 0,
        'footprint': 0, 'consensus': 0, 'single_high': 0, 'single_medium': 0,
        'conflict': 0, 'none': 0, 'scraper_inflation': 0,
        'weighted_computed': 0, 'all_3_agree': 0,
        'source_HIFLD': 0, 'source_scraper': 0, 'source_OSM': 0,
        'source_HIFLD_footprint': 0,
    }
    audit_rows = []

    for row in rows:
        name      = row.get('institution_name', '')
        state     = row.get('state', '')
        city      = row.get('city', '')
        zipcode   = row.get('zip_code', '')
        enr       = sf(row.get('enrollment_2024'))

        scraper_ac   = sf(row.get('scraper_acres'))
        scraper_conf = row.get('scraper_confidence', '')
        osm_ac       = sf(row.get('osm_acres'))
        osm_conf     = row.get('osm_confidence', '')
        osm_ftype    = row.get('osm_feature_type', '')

        # HIFLD match
        hifld_rec, method = match_hifld(name, state, city, zipcode, hifld_indexes)
        stats[method] += 1

        if hifld_rec:
            hifld_ac = hifld_rec['acres']
            row['hifld_acres']        = hifld_ac
            row['hifld_match_method'] = method
            if hifld_ac < HIFLD_MIN_ACRES:
                stats['footprint'] += 1
        else:
            hifld_ac = None
            row['hifld_acres']        = ''
            row['hifld_match_method'] = 'no_match'

        # Primary resolution
        primary, source, tier, conflict, inflated = resolve_primary(
            hifld_ac, scraper_ac, scraper_conf,
            osm_ac, osm_conf, osm_ftype, enr
        )

        row['acreage_primary']          = primary if primary is not None else ''
        row['acreage_primary_source']   = source
        row['acreage_reliability_tier'] = tier
        row['acreage_conflict_flag']    = conflict
        row['scraper_inflation_flag']   = 'True' if inflated else ''

        stats[tier] = stats.get(tier, 0) + 1
        if inflated: stats['scraper_inflation'] += 1
        if source:   stats[f'source_{source}'] = stats.get(f'source_{source}', 0) + 1

        # Weighted average
        weighted, basis = compute_weighted(hifld_ac, scraper_ac, osm_ac)
        row['acreage_weighted']       = weighted if weighted is not None else ''
        row['acreage_weighted_basis'] = basis or ''
        if weighted:
            stats['weighted_computed'] += 1
            if basis == 'all_3_agree':
                stats['all_3_agree'] += 1

        # Land flags
        row = recompute_land_flags(row)

        # Audit
        audit_rows.append({
            'institution_name':     name,
            'state':                state,
            'city':                 city,
            'hifld_acres':          row['hifld_acres'],
            'hifld_match_method':   method,
            'scraper_acres':        scraper_ac or '',
            'scraper_confidence':   scraper_conf,
            'osm_acres':            osm_ac or '',
            'osm_confidence':       osm_conf,
            'osm_feature_type':     osm_ftype,
            'acreage_primary':      row['acreage_primary'],
            'acreage_source':       source,
            'reliability_tier':     tier,
            'conflict_flag':        conflict,
            'acreage_weighted':     row['acreage_weighted'],
            'weighted_basis':       row['acreage_weighted_basis'],
            'scraper_inflation':    'True' if inflated else '',
            'flag_land_potential':  row['flag_land_potential'],
            'flag_high_land':       row['flag_high_land_potential'],
        })

    # ── Write master ──────────────────────────────────────────────────────────
    print(f'\nWriting master: {MASTER_FILE}')
    with open(MASTER_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

    # ── Write audit ───────────────────────────────────────────────────────────
    print(f'Writing audit:  {AUDIT_FILE}')
    with open(AUDIT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(audit_rows[0].keys()))
        writer.writeheader()
        writer.writerows(audit_rows)

    # ── Summary ───────────────────────────────────────────────────────────────
    total = len(rows)
    print(f'\n{"="*58}')
    print('ACREAGE RESOLVE v2 COMPLETE')
    print(f'{"="*58}')
    print(f'  Total rows: {total:,}')
    print(f'\n  HIFLD matching:')
    matched = stats["exact_norm"] + stats["zip_fuzzy"] + stats["city_token"]
    print(f'    exact_norm:    {stats["exact_norm"]:>6,}')
    print(f'    zip_fuzzy:     {stats["zip_fuzzy"]:>6,}')
    print(f'    city_token:    {stats["city_token"]:>6,}')
    print(f'    total matched: {matched:>6,}  ({matched/total*100:.1f}%)')
    print(f'    no_match:      {stats["no_match"]:>6,}')
    print(f'\n  acreage_primary source:')
    print(f'    HIFLD:         {stats.get("source_HIFLD",0):>6,}')
    print(f'    scraper:       {stats.get("source_scraper",0):>6,}')
    print(f'    OSM:           {stats.get("source_OSM",0):>6,}')
    print(f'    footprint:     {stats.get("source_HIFLD_footprint",0):>6,}')
    print(f'\n  reliability_tier:')
    print(f'    consensus:     {stats.get("consensus",0):>6,}')
    print(f'    single_high:   {stats.get("single_high",0):>6,}')
    print(f'    single_medium: {stats.get("single_medium",0):>6,}')
    print(f'    conflict:      {stats.get("conflict",0):>6,}')
    print(f'    none:          {stats.get("none",0):>6,}')
    has_primary = sum(1 for r in rows if r.get('acreage_primary') not in ('', None))
    print(f'    with primary:  {has_primary:>6,}  ({has_primary/total*100:.1f}%)')
    print(f'\n  acreage_weighted:')
    print(f'    computed:      {stats["weighted_computed"]:>6,}  ({stats["weighted_computed"]/total*100:.1f}%)')
    print(f'    all 3 agree:   {stats["all_3_agree"]:>6,}')
    print(f'    scraper inflation flags: {stats["scraper_inflation"]:,}')
    print(f'{"="*58}')


if __name__ == '__main__':
    main()