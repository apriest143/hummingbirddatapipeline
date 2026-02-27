"""
================================================================================
Hummingbird Map — Standalone Generator
================================================================================
Reads the master CSV, filters to plotted rows (IPEDS + High/Critical 990s),
trims to only columns the map uses, and embeds the data directly into the HTML
as a JavaScript array. Output is a single self-contained HTML file that works
anywhere — no server, no CSV dependency, just double-click and open.

Usage:
    python generate_standalone_map.py

Output:
    hummingbird_map_standalone.html (~5-15MB depending on data)
================================================================================
"""

import pandas as pd
import json
import os

# =============================================================================
# CONFIGURATION
# =============================================================================

MASTER_FILE = 'hv_master_data/data/Hummingbird_Master_Combined_v6.csv'
MAP_TEMPLATE = 'hv_master_data/data/master_map2.html'
OUTPUT_FILE = 'hv_master_data/data/hummingbird_map_standalone6.html'

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
    'enrollment_2024', 'enrollment_yoy_pct',
    'retention_rate', 'graduation_rate',
    'admission_rate', 'yield_rate',
    'tuition_2025', 'student_faculty_ratio',

    # Property
    'plant_property_equipment', 'land_book_value',
    'acreage_raw', 'verified_acres',

    # Flags
    'flag_enrollment_decline', 'flag_operating_losses',
    'flag_negative_net_worth', 'flag_low_equity_ratio', 'flag_high_debt',
    'flag_990_operating_loss', 'flag_990_consecutive_losses',
    'flag_990_negative_net_assets', 'flag_990_low_equity', 'flag_990_high_debt',
    'flag_990_revenue_decline_1yr', 'flag_990_revenue_decline_2yr',
    'flag_high_land_potential', 'flag_land_potential',

    # BMF metadata
    'subsection_code_bmf', 'ruling_date', 'eo_status_bmf',
    'filing_type_primary', 'fte_staff',
]


def main():
    print("=" * 70)
    print("HUMMINGBIRD MAP — STANDALONE GENERATOR")
    print("=" * 70)

    # --- Load and filter data ---
    print("\nLoading master...")
    master = pd.read_csv(MASTER_FILE, low_memory=False)
    print(f"  Total rows: {len(master):,}, columns: {len(master.columns)}")

    # Normalize: unify distress_category across IPEDS and 990 sources
    mask_empty_cat = master['distress_category'].isna() | (master['distress_category'] == '')
    if 'distress_category_990' in master.columns:
        master.loc[mask_empty_cat, 'distress_category'] = master.loc[mask_empty_cat, 'distress_category_990']
    
    # Map 990 category names to IPEDS convention
    cat_map = {'High Risk': 'High', 'Severe Distress': 'Critical', 'Low Risk': 'Low', 'Moderate Risk': 'Moderate'}
    master['distress_category'] = master['distress_category'].map(lambda x: cat_map.get(x, x) if pd.notna(x) else x)

    if 'distress_score_990' in master.columns:
        mask_empty_score = master['distress_score'].isna()
        master.loc[mask_empty_score, 'distress_score'] = master.loc[mask_empty_score, 'distress_score_990']
    if 'data_completeness_990' in master.columns:
        mask_empty_comp = master['data_completeness_pct'].isna() if 'data_completeness_pct' in master.columns else pd.Series(True, index=master.index)
        if 'data_completeness_pct' not in master.columns:
            master['data_completeness_pct'] = None
        master.loc[mask_empty_comp, 'data_completeness_pct'] = master.loc[mask_empty_comp, 'data_completeness_990']

    # Debug: check 990 status
    df990 = master[master['data_source'] == 'Hummingbird_990']
    print(f"  990 rows total: {len(df990):,}")
    print(f"  990 with distress_category: {df990['distress_category'].notna().sum():,}")
    print(f"  990 High/Critical: {df990['distress_category'].isin(['High','Critical']).sum():,}")
    print(f"  990 with lat/long: {(df990['latitude'].notna() & df990['longitude'].notna()).sum():,}")

    # Filter to plotted rows
    master['latitude'] = pd.to_numeric(master['latitude'], errors='coerce')
    master['longitude'] = pd.to_numeric(master['longitude'], errors='coerce')
    has_coords = master['latitude'].notna() & master['longitude'].notna()

    is_ipeds = master['data_source'] == 'IPEDS'
    is_990 = master['data_source'] == 'Hummingbird_990'

    plotted = master[has_coords & (is_ipeds | is_990)].copy()
    print(f"  Plotted rows: {len(plotted):,}")
    print(f"    IPEDS: {(plotted['data_source']=='IPEDS').sum():,}")
    print(f"    990: {(plotted['data_source']=='Hummingbird_990').sum():,}")

    # --- Trim to only needed columns ---
    available = [c for c in KEEP_COLUMNS if c in plotted.columns]
    missing = [c for c in KEEP_COLUMNS if c not in plotted.columns]
    plotted = plotted[available].copy()

    if missing:
        print(f"\n  Note: {len(missing)} columns not in master (skipped):")
        for m in missing[:10]:
            print(f"    - {m}")
        if len(missing) > 10:
            print(f"    ... and {len(missing)-10} more")

    # --- Clean data for JSON embedding ---
    # Replace NaN with empty string for cleaner JSON
    plotted = plotted.fillna('')

    # Convert to list of dicts
    records = plotted.to_dict(orient='records')

    # Compact JSON — no pretty-print, minimize size
    data_json = json.dumps(records, separators=(',', ':'))
    size_mb = len(data_json) / (1024 * 1024)
    print(f"\n  Data JSON size: {size_mb:.1f} MB")
    print(f"  Records: {len(records):,}, fields per record: {len(available)}")

    # --- Read map template ---
    print(f"\nReading map template: {MAP_TEMPLATE}")
    with open(MAP_TEMPLATE, 'r', encoding='utf-8') as f:
        html = f.read()

    # --- Replace Papa.parse CSV loading with inline data ---
    # Use regex to find and replace the loadCSV function regardless of exact content
    import re
    
    pattern = r'function loadCSV\(\)\s*\{.*?\n    \}'
    
    new_load = """function loadCSV() {
        allData = _embeddedData;
        plotData = allData; // Already filtered during generation

        // Update header
        var nIPEDS = plotData.filter(function(r){return r.data_source==='IPEDS'}).length;
        var n990 = plotData.filter(function(r){return r.data_source==='Hummingbird_990'}).length;
        document.getElementById('headerSubtitle').textContent =
            plotData.length.toLocaleString()+' plotted · '+nIPEDS.toLocaleString()+' IPEDS + '+n990.toLocaleString()+' 990s';

        showMarkers(plotData);
    }"""

    match = re.search(pattern, html, re.DOTALL)
    if match:
        # Insert embedded data variable before the function
        data_declaration = "    // --- EMBEDDED DATA (standalone mode) ---\n    var _embeddedData = __DATA_PLACEHOLDER__;\n\n    "
        html = html[:match.start()] + data_declaration + new_load + html[match.end():]
        print("  ✓ Replaced loadCSV with embedded data loader")
    else:
        print("  ERROR: Could not locate loadCSV function in template!")
        return

    # Inject the actual data
    html = html.replace('__DATA_PLACEHOLDER__', data_json)

    # Remove PapaParse script tag (no longer needed)
    html = html.replace(
        '<script src="https://cdnjs.cloudflare.com/ajax/libs/PapaParse/5.4.1/papaparse.min.js"></script>',
        '<!-- PapaParse removed (standalone mode) -->'
    )

    # --- Write output ---
    final_size = len(html) / (1024 * 1024)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n  Output: {OUTPUT_FILE}")
    print(f"  File size: {final_size:.1f} MB")
    print(f"\n  ✓ Self-contained — just open in a browser!")
    print("=" * 70)


if __name__ == '__main__':
    main()