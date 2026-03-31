"""
================================================================================
Hummingbird Map — Standalone Generator
================================================================================
Reads the master CSV + OSM land results CSV, filters to plotted rows
(IPEDS + High/Critical 990s), trims to only columns the map uses, and embeds
both datasets directly into the HTML as JavaScript variables. Output is a single
self-contained HTML file that works anywhere — no server, no CSV dependency,
just double-click and open.

Usage:
    python master_standalone.py

Output:
    index.html  →  commit and push to repo root → served by GitHub Pages
================================================================================
"""

import pandas as pd
import json
import os
import re
import csv
import sys as _sys
# OSM environment scorer — shared with clusters.py
_SCORER_PATHS = [
    os.path.join(os.path.dirname(__file__), 'osm_scorer.py'),
    os.path.join(os.path.dirname(__file__), '..', 'data', 'osm_scorer.py'),
    'osm_scorer.py',
    'hv_master_data/data/osm_scorer.py',
]
_osm_scorer = None
for _p in _SCORER_PATHS:
    if os.path.exists(_p):
        import importlib.util as _ilu
        _spec = _ilu.spec_from_file_location('osm_scorer', _p)
        _osm_scorer = _ilu.module_from_spec(_spec)
        _spec.loader.exec_module(_osm_scorer)
        break
if _osm_scorer is None:
    print("  Warning: osm_scorer.py not found — environment scores will be skipped")

# =============================================================================
# CONFIGURATION
# =============================================================================

MASTER_FILE      = 'hv_master_data/data/Hummingbird_Master_Combined_v6.csv'
NEWS_CACHE_FILE  = 'news_cache.json'
OSM_FILE     = 'hv_master_data/acreage_scripts/osm_land_results.csv'   # optional — skipped if missing
MAP_TEMPLATE = 'hv_master_data/data/master_map2.html'
OUTPUT_FILE  = 'index.html'  # repo root → GitHub Pages

# Columns the map actually references (extracted from JS code)
KEEP_COLUMNS = [
    # Identity
    'institution_name', 'data_source', 'institution_type', 'unitid', 'ein',
    'address', 'city', 'state', 'zip_code', 'county', 'latitude', 'longitude',
    'urbanization', 'affiliation', 'ntee_code', 'is_land_grant',

    # Scores
    'distress_score', 'distress_category', 'data_completeness_pct',
    'closure_risk_score', 'risk_tier', 'ml_warning_flags', 'ml_warning_count',

    # IPEDS subdomains
    'solvency_score_ipeds', 'liquidity_score_ipeds', 'operating_score_ipeds',
    'enrollment_score_ipeds', 'academic_score_ipeds', 'demand_score_ipeds',
    'trend_score_ipeds', 'data_completeness_ipeds',

    # 990 subdomains
    'solvency_score_990', 'liquidity_score_990', 'operating_score_990',
    'trend_score_990', 'red_flag_score_990', 'data_completeness_990',
    'data_quality_tier', 'n_filing_years', 'score_year_actual',

    # Financials
    'revenue_2024', 'revenue_2023', 'revenue_2022',
    'expenses_2024', 'net_income_2024',
    'assets_2024', 'liabilities_2024', 'net_assets_2024',
    'equity_ratio', 'operating_margin', 'debt_ratio',
    'revenue_2yr_pct', 'tuition_dependency', 'endowment_per_fte',
    'long_term_debt',

    # Enrollment
    'enrollment_2022', 'enrollment_2023', 'enrollment_2024', 'enrollment_yoy_pct',
    'retention_rate', 'graduation_rate',
    'admission_rate', 'yield_rate',
    'tuition_2025', 'student_faculty_ratio',

    # Property & Land
    'plant_property_equipment', 'land_book_value',
    'hifld_acres', 'hifld_match_method',
    'scraper_acres', 'osm_acres', 'acreage_primary', 'acreage_primary_source',
    'acreage_conflict_flag', 'scraper_confidence', 'osm_confidence',
    'scraper_source', 'osm_feature_name',

    # Flags
    'flag_enrollment_decline', 'flag_operating_losses',
    'flag_negative_net_worth', 'flag_low_equity_ratio', 'flag_high_debt',
    'flag_990_operating_loss', 'flag_990_consecutive_losses',
    'flag_990_negative_net_assets', 'flag_990_low_equity', 'flag_990_high_debt',
    'flag_990_revenue_decline_1yr', 'flag_990_revenue_decline_2yr',
    'flag_high_land_potential', 'flag_land_potential',
    'flag_rural_suburban', 'flag_small_enrollment',

    # BMF metadata
    'subsection_code_bmf', 'ruling_date', 'eo_status_bmf',
    'filing_type_primary', 'fte_staff',
]

# OSM feature categories — must match osm_features.py
FEATURES = [
    ("natural",   "coastline",       "Coastline",              "water"),
    ("natural",   "water",           "Lake/Pond",              "water"),
    ("waterway",  "river",           "River",                  "water"),
    ("natural",   "beach",           "Beach",                  "water"),
    ("waterway",  "canal",           "Canal",                  "water"),
    ("waterway",  "stream",          "Stream",                 "water"),
    ("natural",   "wetland",         "Wetland",                "water"),
    ("leisure",   "marina",          "Marina",                 "water"),
    ("natural",   "spring",          "Natural Spring",         "water"),
    ("natural",   "hot_spring",      "Hot Spring",             "water"),
    ("leisure",   "ski_resort",      "Ski Resort",             "winter"),
    ("landuse",   "winter_sports",   "Winter Sports Area",     "winter"),
    ("piste:type","downhill",        "Downhill Ski Run",       "winter"),
    ("piste:type","nordic",          "Nordic Ski Trail",       "winter"),
    ("aerialway", "gondola",         "Gondola",                "winter"),
    ("aerialway", "chair_lift",      "Chair Lift",             "winter"),
    ("aerialway", "drag_lift",       "Drag Lift",              "winter"),
    ("aerialway", "cable_car",       "Cable Car",              "winter"),
    ("sport",     "ice_skating",     "Ice Skating",            "winter"),
    ("sport",     "skiing",          "Skiing Facility",        "winter"),
    ("sport",     "ice_hockey",      "Ice Hockey Rink",        "winter"),
    ("route",     "hiking",          "Hiking Trail",           "outdoor_recreation"),
    ("route",     "mtb",             "Mountain Bike Trail",    "outdoor_recreation"),
    ("route",     "bicycle",         "Cycling Route",          "outdoor_recreation"),
    ("route",     "horse",           "Equestrian Trail",       "outdoor_recreation"),
    ("route",     "canoe",           "Canoe Route",            "outdoor_recreation"),
    ("highway",   "cycleway",        "Dedicated Cycleway",     "outdoor_recreation"),
    ("highway",   "bridleway",       "Bridleway",              "outdoor_recreation"),
    ("sport",     "climbing",        "Rock Climbing",          "outdoor_recreation"),
    ("sport",     "surfing",         "Surfing",                "outdoor_recreation"),
    ("sport",     "kayaking",        "Kayaking",               "outdoor_recreation"),
    ("sport",     "rowing",          "Rowing",                 "outdoor_recreation"),
    ("sport",     "fishing",         "Fishing",                "outdoor_recreation"),
    ("sport",     "hunting",         "Hunting",                "outdoor_recreation"),
    ("sport",     "golf",            "Golf",                   "outdoor_recreation"),
    ("leisure",   "golf_course",     "Golf Course",            "outdoor_recreation"),
    ("leisure",   "sports_centre",   "Sports Centre",          "outdoor_recreation"),
    ("leisure",   "horse_riding",    "Horse Riding",           "outdoor_recreation"),
    ("sport",     "swimming",        "Swimming Facility",      "outdoor_recreation"),
    ("tourism",   "camp_site",       "Campground",             "outdoor_recreation"),
    ("tourism",   "wilderness_hut",  "Wilderness Hut",         "outdoor_recreation"),
    ("boundary",  "national_park",   "National Park",          "protected_land"),
    ("boundary",  "protected_area",  "Protected Area",         "protected_land"),
    ("leisure",   "nature_reserve",  "Nature Reserve",         "protected_land"),
    ("landuse",   "forest",          "Forest",                 "protected_land"),
    ("boundary",  "forest",          "National Forest",        "protected_land"),
    ("landuse",   "conservation",    "Conservation Land",      "protected_land"),
    ("natural",   "wood",            "Woodland",               "natural_features"),
    ("natural",   "peak",            "Mountain Peak",          "natural_features"),
    ("natural",   "volcano",         "Volcano",                "natural_features"),
    ("natural",   "cliff",           "Cliff",                  "natural_features"),
    ("natural",   "cave_entrance",   "Cave",                   "natural_features"),
    ("natural",   "glacier",         "Glacier",                "natural_features"),
    ("natural",   "scrub",           "Scrubland",              "natural_features"),
    ("natural",   "heath",           "Heathland",              "natural_features"),
    ("tourism",   "resort",          "Resort",                 "tourism_leisure"),
    ("tourism",   "theme_park",      "Theme Park",             "tourism_leisure"),
    ("tourism",   "zoo",             "Zoo",                    "tourism_leisure"),
    ("tourism",   "aquarium",        "Aquarium",               "tourism_leisure"),
    ("tourism",   "museum",          "Museum",                 "tourism_leisure"),
    ("tourism",   "attraction",      "Tourist Attraction",     "tourism_leisure"),
    ("tourism",   "viewpoint",       "Scenic Viewpoint",       "tourism_leisure"),
    ("historic",  "monument",        "Historic Monument",      "tourism_leisure"),
    ("leisure",   "park",            "Park",                   "tourism_leisure"),
    ("leisure",   "garden",          "Botanical Garden",       "tourism_leisure"),
    ("amenity",   "casino",          "Casino",                 "tourism_leisure"),
    ("aeroway",   "aerodrome",       "Airport",                "infrastructure"),
    ("railway",   "station",         "Train Station",          "infrastructure"),
    ("amenity",   "ferry_terminal",  "Ferry Terminal",         "infrastructure"),
]

LABEL_TO_CAT  = {f[2]: f[3] for f in FEATURES}
CAT_ORDER     = ['water','winter','outdoor_recreation','protected_land',
                 'natural_features','tourism_leisure','infrastructure']


# =============================================================================
# OSM LAND DATA BUILDER
# =============================================================================

MAX_NAMED_DISPLAY = 5   # max named features shown per label in the baked output

def parse_osm_feature_string(feat_str):
    """
    Parse the pipe-delimited feature string from osm_land_results.csv into a
    by_category dict: { cat: [ {label, display}, ... ], ... }

    Format of each pipe segment:  "display text [Label]"

    Trimming: each label is capped at MAX_NAMED_DISPLAY named items for the
    baked output — keeps the JS small. The full data remains in the CSV.
    """
    by_category = {}
    if not feat_str or feat_str.strip() in ('', 'ERROR'):
        return by_category

    for part in feat_str.split(' | '):
        part = part.strip()
        if not part:
            continue
        label   = ''
        display = part
        if part.endswith(']') and '[' in part:
            bracket = part.rfind('[')
            label   = part[bracket+1:-1].strip()
            display = part[:bracket].strip()

        cat = LABEL_TO_CAT.get(label, 'other')
        if cat == 'other':
            continue   # skip unrecognised labels (old-format data)
        if cat not in by_category:
            by_category[cat] = []

        # Trim named list to MAX_NAMED_DISPLAY, preserving unnamed count
        # Display format: "Name1, Name2 + N unnamed Labels" or "Name1, Name2"
        # We split on " + N unnamed" to separate names from unnamed count
        import re as _re
        unnamed_count = 0
        named_str = display

        unnamed_m = _re.search(r' \+ (\d+) unnamed .+$', display)
        if unnamed_m:
            unnamed_count = int(unnamed_m.group(1))
            named_str = display[:unnamed_m.start()]

        names = [n.strip() for n in named_str.split(',') if n.strip()]

        # Cap at MAX_NAMED_DISPLAY, roll overflow back into unnamed count
        overflow = max(0, len(names) - MAX_NAMED_DISPLAY)
        unnamed_count += overflow
        names = names[:MAX_NAMED_DISPLAY]

        # Rebuild trimmed display string
        if names and unnamed_count > 0:
            trimmed_display = ', '.join(names) + f' +{unnamed_count} more'
        elif names:
            trimmed_display = ', '.join(names)
        else:
            trimmed_display = display  # all unnamed, keep as-is

        by_category[cat].append({'label': label, 'display': trimmed_display})

    return by_category


def build_osm_lookup(osm_path, urb_lookup=None, name_to_loc=None):
    """
    Read osm_land_results.csv and return a dict keyed by name||city||state.
    Uses composite key to avoid collision between same-name institutions.
    urb_lookup: optional dict of {name||city||state: urbanization_string}
    name_to_loc: dict of {institution_name: (city, state)} from master records
    """
    if not os.path.exists(osm_path):
        print(f"  OSM file not found: {osm_path} — skipping land data")
        return {}

    # Some feature strings exceed Python's default CSV field limit (131072 bytes)
    csv.field_size_limit(10 * 1024 * 1024)  # 10MB — safely above any real row

    # name_to_loc maps name → list of (city, state) tuples (could be multiple)
    _name_loc = name_to_loc or {}

    lookup = {}
    with open(osm_path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            name      = row.get('institution_name', '').strip()
            queried   = row.get('osm_env_queried_at', '').strip()
            feat_str  = row.get('osm_env_features', '').strip()
            if not name or not queried or feat_str == 'ERROR':
                continue

            cats_raw = [c.strip() for c in row.get('osm_env_categories', '').split('|') if c.strip()]
            by_cat   = parse_osm_feature_string(feat_str)

            _count  = int(str(row.get('osm_env_count', '0') or '0').strip()) if str(row.get('osm_env_count', '0') or '0').strip().lstrip('-').isdigit() else 0
            _radius = row.get('osm_env_radius_miles', '20')

            # Build composite keys for all (city, state) combos this name maps to
            locations = _name_loc.get(name, [('', '')])
            for (city, state) in locations:
                composite_key = f"{name}||{city}||{state}"
                _urb = (urb_lookup or {}).get(composite_key) or (urb_lookup or {}).get(name)
                _env_score = _osm_scorer.score_osm_environment(
                    by_cat, feature_count=_count, radius_miles=_radius,
                    urbanization=_urb
                ) if _osm_scorer else {'score': None, 'feature_score': None,
                                       'category_score': None, 'density_bonus': None,
                                       'radius_bonus': None, 'context_mult': None,
                                       'breakdown': [], 'top_features': [],
                                       'has_osm_data': bool(by_cat)}
                lookup[composite_key] = {
                    'categories':  cats_raw,
                    'count':       _count,
                    'radius_miles': _radius,
                    'queried_at':  queried,
                    'by_category': by_cat,
                    'env_score':   _env_score,
                }

    return lookup


# =============================================================================
# MAIN
# =============================================================================

def main():
    print('=' * 70)
    print('HUMMINGBIRD MAP — STANDALONE GENERATOR')
    print('=' * 70)

    # ── Load and filter master data ──────────────────────────────────────────
    print(f'\nLoading master: {MASTER_FILE}')
    master = pd.read_csv(MASTER_FILE, low_memory=False)
    print(f'  Total rows: {len(master):,}, columns: {len(master.columns)}')

    # Normalise distress category/score across IPEDS and 990 sources
    mask_empty_cat = master['distress_category'].isna() | (master['distress_category'] == '')
    if 'distress_category_990' in master.columns:
        master.loc[mask_empty_cat, 'distress_category'] = master.loc[mask_empty_cat, 'distress_category_990']

    cat_map = {'High Risk': 'High', 'Severe Distress': 'Critical', 'Low Risk': 'Low', 'Moderate Risk': 'Moderate'}
    master['distress_category'] = master['distress_category'].map(
        lambda x: cat_map.get(x, x) if pd.notna(x) else x)

    if 'distress_score_990' in master.columns:
        mask_empty_score = master['distress_score'].isna()
        master.loc[mask_empty_score, 'distress_score'] = master.loc[mask_empty_score, 'distress_score_990']

    if 'data_completeness_pct' not in master.columns:
        master['data_completeness_pct'] = None
    if 'data_completeness_990' in master.columns:
        mask_empty_comp = master['data_completeness_pct'].isna()
        master.loc[mask_empty_comp, 'data_completeness_pct'] = master.loc[mask_empty_comp, 'data_completeness_990']

    # Filter to plotted rows (has coords, IPEDS or 990)
    master['latitude']  = pd.to_numeric(master['latitude'],  errors='coerce')
    master['longitude'] = pd.to_numeric(master['longitude'], errors='coerce')
    has_coords = master['latitude'].notna() & master['longitude'].notna()
    is_ipeds   = master['data_source'] == 'IPEDS'
    is_990     = master['data_source'] == 'Hummingbird_990'
    plotted    = master[has_coords & (is_ipeds | is_990)].copy()

    print(f'  Plotted rows: {len(plotted):,}')
    print(f'    IPEDS: {(plotted["data_source"]=="IPEDS").sum():,}')
    print(f'    990:   {(plotted["data_source"]=="Hummingbird_990").sum():,}')

    # Trim to needed columns
    available = [c for c in KEEP_COLUMNS if c in plotted.columns]
    missing   = [c for c in KEEP_COLUMNS if c not in plotted.columns]
    plotted   = plotted[available].copy()

    if missing:
        print(f'\n  Note: {len(missing)} columns not found in master (skipped):')
        for m in missing[:10]:
            print(f'    - {m}')
        if len(missing) > 10:
            print(f'    ... and {len(missing)-10} more')

    plotted   = plotted.fillna('')
    records   = plotted.to_dict(orient='records')
    data_json = json.dumps(records, separators=(',', ':'))
    size_mb   = len(data_json) / (1024 * 1024)
    print(f'\n  Data JSON: {size_mb:.1f} MB  ({len(records):,} records, {len(available)} fields)')

    # ── Load OSM land data ───────────────────────────────────────────────────
    print(f'\nLoading OSM land data: {OSM_FILE}')
    # Build name→[(city,state)] lookup for composite key resolution
    from collections import defaultdict
    _name_to_loc = defaultdict(list)
    for r in records:
        n = (r.get('institution_name') or '').strip()
        c = (r.get('city') or '').strip()
        s = (r.get('state') or '').strip()
        if n:
            _name_to_loc[n].append((c, s))
    # Build composite key urb_lookup: name||city||state → urbanization
    urb_lookup = {}
    for r in records:
        n = (r.get('institution_name') or '').strip()
        c = (r.get('city') or '').strip()
        s = (r.get('state') or '').strip()
        if n:
            urb_lookup[f"{n}||{c}||{s}"] = r.get('urbanization', '')
    osm_lookup = build_osm_lookup(OSM_FILE, urb_lookup=urb_lookup, name_to_loc=dict(_name_to_loc))
    osm_json   = json.dumps(osm_lookup, separators=(',', ':'), ensure_ascii=False)
    osm_mb     = len(osm_json) / (1024 * 1024)
    print(f'  {len(osm_lookup):,} institutions with land data  ({osm_mb:.1f} MB)')

    # ── Read map template ────────────────────────────────────────────────────
    print(f'\nReading template: {MAP_TEMPLATE}')
    with open(MAP_TEMPLATE, 'r', encoding='utf-8') as f:
        html = f.read()

    # ── Inject master data (replace loadCSV) ─────────────────────────────────
    pattern = r'function loadCSV\(\)\s*\{.*?\n    \}'
    new_load = """function loadCSV() {
        allData = _embeddedData;
        plotData = allData;

        var nIPEDS  = plotData.filter(function(r){return r.data_source==='IPEDS'}).length;
        var n990    = plotData.filter(function(r){return r.data_source==='Hummingbird_990'}).length;
        var nClosed = plotData.filter(function(r){return isLikelyClosed(r)}).length;
        document.getElementById('headerSubtitle').textContent =
            plotData.length.toLocaleString()+' plotted · '+nIPEDS.toLocaleString()+' IPEDS · '+
            n990.toLocaleString()+' 990s · '+nClosed+' likely closed';

        showMarkers(plotData);
    }"""

    match = re.search(pattern, html, re.DOTALL)
    if match:
        data_declaration = (
            '    // ── Embedded master data (standalone) ──\n'
            '    var _embeddedData = __MASTER_DATA__;\n\n'
            '    // ── Embedded OSM land data (standalone) ──\n'
            '    var OSM_LAND_DATA = __OSM_DATA__;\n\n'
            '    // ── Embedded news cache (standalone) ──\n'
            '    var NEWS_CACHE = __NEWS_CACHE__;\n\n    '
        )
        html = html[:match.start()] + data_declaration + new_load + html[match.end():]
        print('  ✓ Replaced loadCSV with embedded data loader')
    else:
        print('  ERROR: Could not find loadCSV function in template — aborting')
        return

    # Inject the actual data values
    html = html.replace('__MASTER_DATA__', data_json)
    html = html.replace('__OSM_DATA__',    osm_json)

    # ── Embed news cache ──────────────────────────────────────────────────────
    if os.path.exists(NEWS_CACHE_FILE):
        with open(NEWS_CACHE_FILE, encoding='utf-8') as f:
            news_cache_json = f.read()
        html = html.replace('__NEWS_CACHE__', news_cache_json)
        cache_mb = len(news_cache_json) / (1024 * 1024)
        print(f'  News cache embedded: {cache_mb:.1f} MB')
    else:
        html = html.replace('__NEWS_CACHE__', '{}')
        print('  News cache: not found (news_scraper.py not yet run)')

    # Remove PapaParse (no longer needed in standalone mode)
    html = re.sub(
        r'<script src="[^"]*papaparse[^"]*"></script>',
        '<!-- PapaParse removed (standalone mode) -->',
        html
    )

    # Remove osm_land_data.js reference if present in template
    # (data is now baked in directly)
    html = re.sub(
        r'<script src="osm_land_data\.js"[^>]*></script>',
        '<!-- osm_land_data.js removed (standalone mode — data baked in) -->',
        html
    )

    # ── Write output ─────────────────────────────────────────────────────────
    final_mb = len(html) / (1024 * 1024)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f'\n  Output: {OUTPUT_FILE}  ({final_mb:.1f} MB)')
    print(f'  ✓ Self-contained — commit and push to serve on GitHub Pages')

    # ── Run cluster pipeline ──────────────────────────────────────────────────
    clusters_script = os.path.join(os.path.dirname(__file__), 'clusters.py')
    if not os.path.exists(clusters_script):
        # Try repo root
        clusters_script = 'hv_master_data/data/clusters.py'
    if os.path.exists(clusters_script):
        print('\n' + '=' * 70)
        print('  Running cluster pipeline...')
        import subprocess
        result = subprocess.run(
            [sys.executable, clusters_script],
            capture_output=False
        )
        if result.returncode != 0:
            print('  ⚠ Cluster pipeline failed — skipping (clusters.html not updated)')
    else:
        print(f'\n  ℹ clusters.py not found — skipping cluster generation')
        print(f'    Place clusters.py at: {clusters_script}')

    print('=' * 70)


if __name__ == '__main__':
    import sys
    main()