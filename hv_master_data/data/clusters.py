"""
clusters.py — Hummingbird Ventures Opportunity Cluster Engine
Reads Hummingbird_Master_Combined_v6.csv (+ optional osm_land_results.csv),
fits K-Means models separately for IPEDS and 990 institutions,
and generates clusters.html — a standalone explorer page.

Run from repo root:
    python hv_master_data/data/clusters.py

Or called by master_standalone.py.
"""

import os
import sys
import csv
import json
import math
import argparse
import warnings
from collections import Counter, defaultdict

warnings.filterwarnings('ignore')

# ---------------------------------------------------------------------------
# Paths (all relative to repo root)
# ---------------------------------------------------------------------------
DEFAULT_MASTER   = 'hv_master_data/data/Hummingbird_Master_Combined_v6.csv'
DEFAULT_OSM      = 'hv_master_data/acreage_scripts/osm_land_results.csv'
DEFAULT_OUT_HTML = 'clusters.html'
DEFAULT_OUT_CSV  = 'hv_master_data/data/Hummingbird_Master_Clustered.csv'

IPEDS_K = 6
HB990_K = 5

# ---------------------------------------------------------------------------
# US Region lookup
# ---------------------------------------------------------------------------
STATE_REGION = {
    'Maine':'Northeast','New Hampshire':'Northeast','Vermont':'Northeast',
    'Massachusetts':'Northeast','Rhode Island':'Northeast','Connecticut':'Northeast',
    'New York':'Northeast','New Jersey':'Northeast','Pennsylvania':'Northeast',
    'Maryland':'Northeast','Delaware':'Northeast','District of Columbia':'Northeast',
    'Virginia':'Southeast','West Virginia':'Southeast','North Carolina':'Southeast',
    'South Carolina':'Southeast','Georgia':'Southeast','Florida':'Southeast',
    'Alabama':'Southeast','Mississippi':'Southeast','Tennessee':'Southeast',
    'Kentucky':'Southeast','Arkansas':'Southeast','Louisiana':'Southeast',
    'Ohio':'Midwest','Indiana':'Midwest','Illinois':'Midwest','Michigan':'Midwest',
    'Wisconsin':'Midwest','Minnesota':'Midwest','Iowa':'Midwest','Missouri':'Midwest',
    'North Dakota':'Midwest','South Dakota':'Midwest','Nebraska':'Midwest','Kansas':'Midwest',
    'Texas':'Southwest','Oklahoma':'Southwest','New Mexico':'Southwest','Arizona':'Southwest',
    'Montana':'West','Idaho':'West','Wyoming':'West','Colorado':'West','Utah':'West',
    'Nevada':'West','California':'West','Oregon':'West','Washington':'West',
    'Alaska':'West','Hawaii':'West',
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fv(row, col, default=None):
    """Safe float parse — never returns NaN or Inf."""
    v = row.get(col, '')
    if v in ('', 'nan', 'NaN', 'None', None):
        return default
    try:
        result = float(v)
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (ValueError, TypeError):
        return default

def flag(row, col):
    v = str(row.get(col, '')).strip().lower()
    return v in ('1', 'true', 'yes', '1.0')

def safe_log(x):
    if x is None or x <= 0:
        return None
    return math.log1p(x)

def urbanization_group(urb):
    if not urb:
        return 'Unknown'
    prefix = urb.split(':')[0].strip()
    return prefix if prefix in ('City', 'Suburb', 'Town', 'Rural') else 'Unknown'

def impute_median(values):
    """Replace None with column median."""
    valid = [v for v in values if v is not None]
    if not valid:
        return [0.0] * len(values)
    valid.sort()
    med = valid[len(valid) // 2]
    return [v if v is not None else med for v in values]

def sanitize_for_json(obj):
    """Recursively replace NaN/Inf floats with None for valid JSON."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    return obj

# ---------------------------------------------------------------------------
# IPEDS feature extraction
# ---------------------------------------------------------------------------
def extract_ipeds_features(rows):
    feature_names = [
        'solvency_score', 'liquidity_score', 'operating_score',
        'enrollment_score', 'academic_score', 'demand_score', 'trend_score',
        'operating_margin', 'equity_ratio',
        'log_enrollment', 'enrollment_yoy_pct', 'log_acreage', 'log_revenue',
        'flag_enrollment_decline', 'flag_small_enrollment',
        'flag_high_tuition_dependency', 'flag_low_retention',
        'flag_operating_losses', 'flag_negative_net_worth',
        'flag_land_potential', 'flag_high_land_potential', 'flag_rural_suburban',
        'type_public_4yr', 'type_public_2yr',
        'type_pnp_4yr', 'type_pnp_2yr',
        'type_pfp_4yr', 'type_pfp_2yr',
        'urban_city', 'urban_suburb', 'urban_town', 'urban_rural',
        'region_northeast', 'region_southeast', 'region_midwest',
        'region_southwest', 'region_west',
    ]

    raw = defaultdict(list)
    valid_indices = []

    for i, r in enumerate(rows):
        if r.get('data_source') != 'IPEDS':
            continue

        raw['solvency_score'].append(fv(r, 'solvency_score_ipeds'))
        raw['liquidity_score'].append(fv(r, 'liquidity_score_ipeds'))
        raw['operating_score'].append(fv(r, 'operating_score_ipeds'))
        raw['enrollment_score'].append(fv(r, 'enrollment_score_ipeds'))
        raw['academic_score'].append(fv(r, 'academic_score_ipeds'))
        raw['demand_score'].append(fv(r, 'demand_score_ipeds'))
        raw['trend_score'].append(fv(r, 'trend_score_ipeds'))
        raw['operating_margin'].append(fv(r, 'operating_margin'))
        raw['equity_ratio'].append(fv(r, 'equity_ratio'))

        enr = fv(r, 'enrollment_2024')
        raw['log_enrollment'].append(safe_log(enr))
        raw['enrollment_yoy_pct'].append(fv(r, 'enrollment_yoy_pct'))

        acres = fv(r, 'acreage_primary') or fv(r, 'scraper_acres') or fv(r, 'osm_acres')
        raw['log_acreage'].append(safe_log(acres))
        raw['log_revenue'].append(safe_log(fv(r, 'revenue_2024')))

        raw['flag_enrollment_decline'].append(1.0 if flag(r, 'flag_enrollment_decline') else 0.0)
        raw['flag_small_enrollment'].append(1.0 if flag(r, 'flag_small_enrollment') else 0.0)
        raw['flag_high_tuition_dependency'].append(1.0 if flag(r, 'flag_high_tuition_dependency') else 0.0)
        raw['flag_low_retention'].append(1.0 if flag(r, 'flag_low_retention') else 0.0)
        raw['flag_operating_losses'].append(1.0 if flag(r, 'flag_operating_losses') else 0.0)
        raw['flag_negative_net_worth'].append(1.0 if flag(r, 'flag_negative_net_worth') else 0.0)
        raw['flag_land_potential'].append(1.0 if flag(r, 'flag_land_potential') else 0.0)
        raw['flag_high_land_potential'].append(1.0 if flag(r, 'flag_high_land_potential') else 0.0)
        raw['flag_rural_suburban'].append(1.0 if flag(r, 'flag_rural_suburban') else 0.0)

        it = r.get('institution_type', '')
        raw['type_public_4yr'].append(1.0 if 'Public | 4' in it else 0.0)
        raw['type_public_2yr'].append(1.0 if 'Public | 2' in it else 0.0)
        raw['type_pnp_4yr'].append(1.0 if 'Private Non-Profit | 4' in it else 0.0)
        raw['type_pnp_2yr'].append(1.0 if 'Private Non-Profit | 2' in it else 0.0)
        raw['type_pfp_4yr'].append(1.0 if 'Private For-Profit | 4' in it else 0.0)
        raw['type_pfp_2yr'].append(1.0 if 'Private For-Profit | 2' in it else 0.0)

        ug = urbanization_group(r.get('urbanization', ''))
        raw['urban_city'].append(1.0 if ug == 'City' else 0.0)
        raw['urban_suburb'].append(1.0 if ug == 'Suburb' else 0.0)
        raw['urban_town'].append(1.0 if ug == 'Town' else 0.0)
        raw['urban_rural'].append(1.0 if ug == 'Rural' else 0.0)

        reg = STATE_REGION.get(r.get('state', ''), 'Unknown')
        raw['region_northeast'].append(1.0 if reg == 'Northeast' else 0.0)
        raw['region_southeast'].append(1.0 if reg == 'Southeast' else 0.0)
        raw['region_midwest'].append(1.0 if reg == 'Midwest' else 0.0)
        raw['region_southwest'].append(1.0 if reg == 'Southwest' else 0.0)
        raw['region_west'].append(1.0 if reg == 'West' else 0.0)

        valid_indices.append(i)

    continuous = ['solvency_score','liquidity_score','operating_score','enrollment_score',
                  'academic_score','demand_score','trend_score','operating_margin',
                  'equity_ratio','log_enrollment','enrollment_yoy_pct','log_acreage','log_revenue']
    for col in continuous:
        raw[col] = impute_median(raw[col])

    n = len(valid_indices)
    matrix = [[raw[feat][i] for feat in feature_names] for i in range(n)]
    return matrix, feature_names, valid_indices


# ---------------------------------------------------------------------------
# 990 feature extraction
# ---------------------------------------------------------------------------
def extract_990_features(rows):
    feature_names = [
        'solvency_score', 'liquidity_score', 'operating_score',
        'trend_score', 'red_flag_score',
        'operating_margin', 'equity_ratio',
        'log_revenue', 'log_assets', 'log_ppe', 'log_acreage',
        'flag_operating_loss', 'flag_negative_net_assets',
        'flag_high_debt', 'flag_consecutive_losses', 'flag_revenue_decline',
        'flag_land_potential',
        'type_religious', 'type_housing', 'type_recreation',
        'type_educational', 'type_youth', 'type_environmental',
        'type_human_services', 'type_animal', 'type_agriculture', 'type_other',
        'region_northeast', 'region_southeast', 'region_midwest',
        'region_southwest', 'region_west',
    ]

    raw = defaultdict(list)
    valid_indices = []

    for i, r in enumerate(rows):
        if r.get('data_source') != 'Hummingbird_990':
            continue

        raw['solvency_score'].append(fv(r, 'solvency_score_990'))
        raw['liquidity_score'].append(fv(r, 'liquidity_score_990'))
        raw['operating_score'].append(fv(r, 'operating_score_990'))
        raw['trend_score'].append(fv(r, 'trend_score_990'))
        raw['red_flag_score'].append(fv(r, 'red_flag_score_990'))
        raw['operating_margin'].append(fv(r, 'operating_margin'))
        raw['equity_ratio'].append(fv(r, 'equity_ratio'))
        raw['log_revenue'].append(safe_log(fv(r, 'revenue_2024')))
        raw['log_assets'].append(safe_log(fv(r, 'assets_2024')))
        raw['log_ppe'].append(safe_log(fv(r, 'plant_property_equipment')))

        acres = fv(r, 'acreage_primary') or fv(r, 'scraper_acres') or fv(r, 'osm_acres')
        raw['log_acreage'].append(safe_log(acres))

        raw['flag_operating_loss'].append(1.0 if flag(r, 'flag_990_operating_loss') else 0.0)
        raw['flag_negative_net_assets'].append(1.0 if flag(r, 'flag_990_negative_net_assets') else 0.0)
        raw['flag_high_debt'].append(1.0 if flag(r, 'flag_990_high_debt') else 0.0)
        raw['flag_consecutive_losses'].append(1.0 if flag(r, 'flag_990_consecutive_losses') else 0.0)
        raw['flag_revenue_decline'].append(1.0 if flag(r, 'flag_990_revenue_decline_1yr') else 0.0)
        raw['flag_land_potential'].append(1.0 if flag(r, 'flag_land_potential') else 0.0)

        it = r.get('institution_type', '')
        raw['type_religious'].append(1.0 if it == 'Religious Institution' else 0.0)
        raw['type_housing'].append(1.0 if it == 'Housing/Residential' else 0.0)
        raw['type_recreation'].append(1.0 if it == 'Recreation/Camp' else 0.0)
        raw['type_educational'].append(1.0 if it == 'Educational Institution' else 0.0)
        raw['type_youth'].append(1.0 if it == 'Youth Development' else 0.0)
        raw['type_environmental'].append(1.0 if it == 'Environmental/Conservation' else 0.0)
        raw['type_human_services'].append(1.0 if it == 'Human Services' else 0.0)
        raw['type_animal'].append(1.0 if it == 'Animal/Wildlife' else 0.0)
        raw['type_agriculture'].append(1.0 if it == 'Agriculture/Farm' else 0.0)
        raw['type_other'].append(1.0 if it == 'Other Nonprofit' else 0.0)

        reg = STATE_REGION.get(r.get('state', ''), 'Unknown')
        raw['region_northeast'].append(1.0 if reg == 'Northeast' else 0.0)
        raw['region_southeast'].append(1.0 if reg == 'Southeast' else 0.0)
        raw['region_midwest'].append(1.0 if reg == 'Midwest' else 0.0)
        raw['region_southwest'].append(1.0 if reg == 'Southwest' else 0.0)
        raw['region_west'].append(1.0 if reg == 'West' else 0.0)

        valid_indices.append(i)

    continuous = ['solvency_score','liquidity_score','operating_score','trend_score',
                  'red_flag_score','operating_margin','equity_ratio',
                  'log_revenue','log_assets','log_ppe','log_acreage']
    for col in continuous:
        raw[col] = impute_median(raw[col])

    n = len(valid_indices)
    matrix = [[raw[feat][i] for feat in feature_names] for i in range(n)]
    return matrix, feature_names, valid_indices


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------
def run_kmeans(matrix, k, seed=42):
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    X = StandardScaler().fit_transform(matrix)
    km = KMeans(n_clusters=k, random_state=seed, n_init=20, max_iter=500)
    labels = km.fit_predict(X)
    return labels.tolist(), km.cluster_centers_.tolist(), km.inertia_


# ---------------------------------------------------------------------------
# Cluster naming
# ---------------------------------------------------------------------------
def auto_name_ipeds_cluster(cluster_rows, cluster_id):
    def med(col):
        vals = [fv(r, col) for r in cluster_rows if fv(r, col) is not None]
        if not vals: return 0
        vals.sort(); return vals[len(vals) // 2]

    def pct(col):
        return sum(1 for r in cluster_rows if flag(r, col)) / max(len(cluster_rows), 1) * 100

    def top(col):
        c = Counter(r.get(col, '') for r in cluster_rows if r.get(col, ''))
        return c.most_common(1)[0][0] if c else ''

    ds           = med('distress_score')
    solvency     = med('solvency_score_ipeds')
    enroll_sc    = med('enrollment_score_ipeds')
    operating_sc = med('operating_score_ipeds')
    enr          = med('enrollment_2024')
    enr_yoy      = med('enrollment_yoy_pct')
    acres        = med('acreage_primary') or med('scraper_acres') or 0
    op_margin    = med('operating_margin')
    top_type     = top('institution_type')
    top_urb      = (top('urbanization') or '').split(':')[0].strip()
    top_region   = Counter(STATE_REGION.get(r.get('state', ''), 'Unknown') for r in cluster_rows).most_common(1)[0][0]

    pct_enr_dec = pct('flag_enrollment_decline')
    pct_small   = pct('flag_small_enrollment')
    pct_op_loss = pct('flag_operating_losses')
    pct_neg_nw  = pct('flag_negative_net_worth')
    pct_land    = (pct('flag_land_potential') + pct('flag_high_land_potential')) / 2
    pct_rural   = pct('flag_rural_suburban')

    if solvency > 30 and pct_neg_nw > 8:
        name, icon, color = 'Balance Sheet Distress', '🔴', '#ff4757'
        desc = 'Institutions with severely impaired balance sheets — negative or near-zero net assets. Urgent restructuring candidates.'
    elif enroll_sc > 25 and pct_enr_dec > 20 and pct_small > 70:
        name, icon, color = 'Enrollment Collapse', '📉', '#ff6b35'
        desc = 'Small institutions facing steep enrollment declines. Heavily tuition-dependent with rapidly shrinking student pipelines.'
    elif 'For-Profit' in top_type and pct_small > 60:
        name, icon, color = 'For-Profit Trade Schools', '🏢', '#a29bfe'
        desc = 'Small for-profit vocational and trade programs. Urban-concentrated, typically 2-year, with high tuition dependency.'
    elif 'Public' in top_type and enr > 2000:
        name, icon, color = 'Public Anchor Institutions', '🏛', '#3b82f6'
        desc = 'Public colleges and universities with stable enrollment bases. Community colleges and regional universities serving as local anchors.'
    elif pct_rural > 60 and acres > 30:
        name, icon, color = 'Rural Land Asset', '🌲', '#00b894'
        desc = 'Rural institutions with significant acreage. Facing demographic headwinds but holding substantial underdeveloped land assets.'
    elif op_margin < 5 and pct_op_loss > 20 and ds > 20:
        name, icon, color = 'Operating Stress', '💸', '#ffa502'
        desc = 'Institutions running thin or negative operating margins. Asset bases remain but cash generation is deteriorating.'
    else:
        name, icon, color = 'Private NP Campus', '🎓', '#74b9ff'
        desc = 'Private non-profit colleges with mixed financial health. Varied enrollment and asset profiles across 2-year and 4-year programs.'

    return {
        'id': cluster_id,
        'name': name, 'desc': desc, 'color': color, 'icon': icon,
        'count': len(cluster_rows),
        'stats': {
            'median_distress':         round(ds, 1),
            'median_solvency_score':   round(solvency, 1),
            'median_enrollment_score': round(enroll_sc, 1),
            'median_operating_score':  round(operating_sc, 1),
            'median_enrollment':       int(round(enr)),
            'median_enr_yoy':          round(enr_yoy, 1) if enr_yoy else 0,
            'median_acres':            round(acres, 1),
            'median_op_margin':        round(op_margin, 1) if op_margin else 0,
            'pct_enrollment_decline':  round(pct_enr_dec),
            'pct_operating_losses':    round(pct_op_loss),
            'pct_negative_net_worth':  round(pct_neg_nw),
            'pct_land_potential':      round(pct_land),
            'pct_small_enrollment':    round(pct_small),
            'pct_rural':               round(pct_rural),
            'top_type':   top_type,
            'top_urb':    top_urb,
            'top_region': top_region,
        }
    }


def auto_name_990_cluster(cluster_rows, cluster_id):
    def med(col):
        vals = [fv(r, col) for r in cluster_rows if fv(r, col) is not None]
        if not vals: return 0
        vals.sort(); return vals[len(vals) // 2]

    def pct(col):
        return sum(1 for r in cluster_rows if flag(r, col)) / max(len(cluster_rows), 1) * 100

    def top(col):
        c = Counter(r.get(col, '') for r in cluster_rows if r.get(col, ''))
        return c.most_common(1)[0][0] if c else ''

    ds        = med('distress_score')
    rev       = med('revenue_2024')
    assets    = med('assets_2024')
    ppe       = med('plant_property_equipment')
    acres     = med('acreage_primary') or med('scraper_acres') or 0
    eq_ratio  = med('equity_ratio')
    top_type  = top('institution_type')
    top_region = Counter(STATE_REGION.get(r.get('state', ''), 'Unknown') for r in cluster_rows).most_common(1)[0][0]

    pct_op_loss = pct('flag_990_operating_loss')
    pct_neg_ast = pct('flag_990_negative_net_assets')
    pct_hi_debt = pct('flag_990_high_debt')
    pct_consec  = pct('flag_990_consecutive_losses')
    pct_rev_dec = pct('flag_990_revenue_decline_1yr')
    pct_land    = pct('flag_land_potential')

    if pct_neg_ast > 40 and pct_hi_debt > 40:
        name, icon, color = 'Insolvent Nonprofits', '🔴', '#ff4757'
        desc = 'Nonprofits with negative net assets and crushing debt. Functionally insolvent with urgent distress signals across all financial metrics.'
    elif top_type in ('Recreation/Camp', 'Environmental/Conservation') and ppe > 100000:
        name, icon, color = 'Outdoor Asset Holders', '🌲', '#00b894'
        desc = 'Recreation, camp, and conservation nonprofits with meaningful land and facility assets. Core HV acquisition targets.'
    elif top_type == 'Religious Institution':
        name, icon, color = 'Faith Campuses', '⛪', '#a29bfe'
        desc = 'Distressed faith-based organizations, many holding retreat or campus facilities. Strong adaptive reuse potential.'
    elif top_type == 'Housing/Residential' and ppe > 200000:
        name, icon, color = 'Residential Asset Holders', '🏘', '#74b9ff'
        desc = 'Housing and residential nonprofits with significant physical plant. Potential for campus, hospitality, or senior living conversion.'
    else:
        name, icon, color = 'Community Service Orgs', '🤝', '#ffa502'
        desc = 'Community-serving nonprofits across youth development, human services, and education. Moderate assets with varied financial health.'

    return {
        'id': cluster_id,
        'name': name, 'desc': desc, 'color': color, 'icon': icon,
        'count': len(cluster_rows),
        'stats': {
            'median_distress':         round(ds, 1),
            'median_revenue_k':        int(round(rev / 1000)) if rev else 0,
            'median_assets_k':         int(round(assets / 1000)) if assets else 0,
            'median_ppe_k':            int(round(ppe / 1000)) if ppe else 0,
            'median_acres':            round(acres, 1),
            'median_equity_ratio':     round(eq_ratio, 1) if eq_ratio else 0,
            'pct_operating_loss':      round(pct_op_loss),
            'pct_negative_assets':     round(pct_neg_ast),
            'pct_high_debt':           round(pct_hi_debt),
            'pct_consecutive_losses':  round(pct_consec),
            'pct_revenue_decline':     round(pct_rev_dec),
            'pct_land_potential':      round(pct_land),
            'top_type':   top_type,
            'top_region': top_region,
        }
    }


# ---------------------------------------------------------------------------
# Preview rows
# ---------------------------------------------------------------------------
def build_preview_rows(cluster_rows, source):
    rows_sorted = sorted(cluster_rows, key=lambda r: -(fv(r, 'distress_score') or 0))[:200]
    out = []
    for r in rows_sorted:
        acres = fv(r, 'acreage_primary') or fv(r, 'scraper_acres') or fv(r, 'osm_acres')
        rev   = fv(r, 'revenue_2024')
        enr   = fv(r, 'enrollment_2024')
        out.append({
            'name':        r.get('institution_name', ''),
            'state':       r.get('state', ''),
            'city':        r.get('city', ''),
            'type':        r.get('institution_type', ''),
            'distress':    round(fv(r, 'distress_score') or 0, 1),
            'category':    r.get('distress_category', ''),
            'revenue':     round(rev / 1000, 1) if rev else None,
            'enrollment':  int(round(enr)) if enr else None,
            'acres':       round(acres, 1) if acres else None,
            'risk_tier':   r.get('risk_tier', '') if source == 'IPEDS' else '',
            'urbanization': r.get('urbanization', '') if source == 'IPEDS' else '',
        })
    return out


# ---------------------------------------------------------------------------
# HTML generation — matches main map theme exactly
# ---------------------------------------------------------------------------
def build_html(ipeds_clusters, hb990_clusters):
    ipeds_js = json.dumps(sanitize_for_json([{
        'id': c['id'], 'name': c['name'], 'desc': c['desc'],
        'color': c['color'], 'icon': c['icon'], 'count': c['count'],
        'stats': c['stats'], 'rows': c['rows'],
    } for c in ipeds_clusters]))

    hb990_js = json.dumps(sanitize_for_json([{
        'id': c['id'], 'name': c['name'], 'desc': c['desc'],
        'color': c['color'], 'icon': c['icon'], 'count': c['count'],
        'stats': c['stats'], 'rows': c['rows'],
    } for c in hb990_clusters]))

    total   = sum(c['count'] for c in ipeds_clusters) + sum(c['count'] for c in hb990_clusters)
    n_ipeds = sum(c['count'] for c in ipeds_clusters)
    n_990   = sum(c['count'] for c in hb990_clusters)

    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Hummingbird \u2014 Opportunity Clusters</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root {
  --bg:#0f1117; --surface:#181a20; --surface2:#1e2028; --border:#2a2d37;
  --text:#e2e4e9; --text2:#8b8fa3;
  --accent:#6c5ce7; --accent2:#a29bfe;
  --critical:#ff4757; --high:#ff6b35; --elevated:#ffa502;
  --moderate:#3b82f6; --low:#10b981;
  --ipeds:#6c5ce7; --hb990:#00b894;
  --font:'DM Sans',-apple-system,sans-serif;
  --mono:'JetBrains Mono',monospace;
}
*,*::before,*::after { box-sizing:border-box; margin:0; padding:0; }
html,body { background:var(--bg); color:var(--text); font-family:var(--font); font-size:14px; min-height:100vh; }

nav { position:sticky; top:0; z-index:100; display:flex; align-items:center; gap:14px;
      padding:0 20px; height:52px; background:var(--surface); border-bottom:1px solid var(--border); }
.nav-logo { font-size:16px; font-weight:700; letter-spacing:-.5px;
            background:linear-gradient(135deg,var(--accent2),#74b9ff);
            -webkit-background-clip:text; background-clip:text; -webkit-text-fill-color:transparent; }
.nav-sep  { width:1px; height:18px; background:var(--border); }
.nav-sub  { font-size:11px; color:var(--text2); font-weight:500; }
.nav-spacer { flex:1; }
.nav-back { display:flex; align-items:center; gap:6px; padding:5px 12px; border-radius:6px;
             background:var(--surface2); border:1px solid var(--border); color:var(--text2);
             font-size:11px; font-weight:600; text-decoration:none; transition:all .15s; }
.nav-back:hover { border-color:var(--accent); color:var(--text); }

.page { max-width:1280px; margin:0 auto; padding:24px 20px 64px; }
.page-eyebrow { font-family:var(--mono); font-size:10px; color:var(--accent2);
                letter-spacing:2px; text-transform:uppercase; margin-bottom:8px; }
.page-title { font-size:26px; font-weight:700; letter-spacing:-.5px; margin-bottom:6px; }
.page-sub   { font-size:12px; color:var(--text2); line-height:1.6; max-width:680px; margin-bottom:20px; }

.source-tabs { display:flex; gap:0; margin-bottom:20px; border-bottom:1px solid var(--border); }
.s-tab { padding:8px 18px; border:1px solid transparent; border-bottom:none;
         border-radius:6px 6px 0 0; background:transparent; color:var(--text2);
         font-family:var(--font); font-size:12px; font-weight:600; cursor:pointer;
         transition:all .15s; margin-bottom:-1px; }
.s-tab.active { background:var(--surface); border-color:var(--border);
                border-bottom-color:var(--surface); color:var(--text); }
.s-tab:hover:not(.active) { color:var(--text); }
.tc { display:inline-block; font-family:var(--mono); font-size:9px;
      background:rgba(255,255,255,.06); border-radius:8px; padding:1px 6px; margin-left:5px; }
.s-tab.active .tc { background:rgba(108,92,231,.15); color:var(--accent2); }

.layout { display:grid; grid-template-columns:290px 1fr; gap:14px; align-items:start; }

.cards { display:flex; flex-direction:column; gap:6px; }
.card  { padding:13px 13px 13px 16px; border-radius:8px; background:var(--surface);
         border:1px solid var(--border); cursor:pointer; transition:background .15s,border-color .15s;
         position:relative; overflow:hidden; user-select:none; }
.card-bar { position:absolute; left:0; top:0; bottom:0; width:3px; }
.card:hover  { background:var(--surface2); }
.card.active { background:var(--surface2); border-color:var(--border); }
.card-hdr  { display:flex; align-items:center; gap:8px; margin-bottom:5px; }
.card-icon { font-size:15px; flex-shrink:0; }
.card-name { font-size:12px; font-weight:600; flex:1; line-height:1.2; }
.card-n    { font-family:var(--mono); font-size:9px; color:var(--text2); }
.card-desc { font-size:10px; color:var(--text2); line-height:1.5; margin-bottom:7px; }
.chips { display:flex; flex-wrap:wrap; gap:3px; }
.chip  { padding:2px 6px; border-radius:3px; background:var(--surface2);
         border:1px solid var(--border); font-family:var(--mono); font-size:9px; color:var(--text2); }
.chip b { color:var(--text); font-weight:500; }

.detail { background:var(--surface); border:1px solid var(--border);
          border-radius:8px; overflow:hidden; position:sticky; top:68px; }
.d-empty { padding:60px 20px; text-align:center; color:var(--text2); }
.d-empty-arrow { font-size:24px; margin-bottom:10px; }
.d-empty-title { font-size:13px; font-weight:600; margin-bottom:4px; }
.d-empty-sub   { font-size:11px; }

.d-head { padding:16px 18px; border-bottom:1px solid var(--border); background:var(--surface2);
          display:flex; align-items:flex-start; gap:12px; }
.dh-icon { font-size:22px; flex-shrink:0; margin-top:1px; }
.dh-name { font-size:17px; font-weight:700; letter-spacing:-.3px; line-height:1.2; }
.dh-count { font-family:var(--mono); font-size:10px; color:var(--text2); margin-top:3px; }
.dh-desc  { font-size:11px; color:var(--text2); line-height:1.5; margin-top:5px; }

.stat-strip { display:grid; border-bottom:1px solid var(--border); }
.stat-cell  { padding:11px 14px; border-right:1px solid var(--border); text-align:center; }
.stat-cell:last-child { border-right:none; }
.stat-v { font-family:var(--mono); font-size:15px; font-weight:500; display:block; }
.stat-l { font-size:9px; color:var(--text2); text-transform:uppercase; letter-spacing:.6px; margin-top:2px; }

.flags { padding:12px 16px; border-bottom:1px solid var(--border); }
.flags-title { font-family:var(--mono); font-size:9px; color:var(--text2);
               text-transform:uppercase; letter-spacing:1px; margin-bottom:8px; }
.frow   { display:flex; align-items:center; gap:8px; margin-bottom:5px; }
.flabel { font-size:10px; color:var(--text2); width:145px; flex-shrink:0; }
.fbar-wrap { flex:1; height:4px; background:var(--border); border-radius:2px; overflow:hidden; }
.fbar { height:100%; border-radius:2px; }
.fpct { font-family:var(--mono); font-size:9px; color:var(--text2); width:28px; text-align:right; }

.tbar   { display:flex; align-items:center; gap:8px; padding:9px 13px;
          border-bottom:1px solid var(--border); background:var(--surface2); }
.tsearch { flex:1; padding:5px 8px; background:var(--bg); border:1px solid var(--border);
           border-radius:5px; color:var(--text); font-family:var(--font); font-size:11px; outline:none; }
.tsearch:focus { border-color:var(--accent); }
.tcount { font-family:var(--mono); font-size:9px; color:var(--text2); white-space:nowrap; }
.export-btn { padding:4px 10px; border-radius:5px; background:rgba(108,92,231,.1);
              border:1px solid rgba(108,92,231,.3); color:var(--accent2);
              font-family:var(--font); font-size:10px; font-weight:600;
              cursor:pointer; white-space:nowrap; }
.export-btn:hover { background:rgba(108,92,231,.2); }
.twrap { max-height:400px; overflow-y:auto; }
.twrap::-webkit-scrollbar { width:3px; }
.twrap::-webkit-scrollbar-thumb { background:var(--border); border-radius:2px; }
table { width:100%; border-collapse:collapse; }
th { padding:6px 10px; text-align:left; font-family:var(--mono); font-size:9px; color:var(--text2);
     text-transform:uppercase; letter-spacing:.6px; border-bottom:1px solid var(--border);
     background:var(--surface2); white-space:nowrap; cursor:pointer; user-select:none; }
th:hover { color:var(--text); }
td { padding:6px 10px; font-size:11px; border-bottom:1px solid rgba(42,45,55,.5); vertical-align:middle; }
tr:hover td { background:rgba(255,255,255,.015); }
.td-name { font-weight:600; max-width:180px; }
.td-name small { display:block; font-size:9px; color:var(--text2); font-weight:400; margin-top:1px; }
.dp { display:inline-block; padding:2px 5px; border-radius:3px;
      font-family:var(--mono); font-size:9px; font-weight:600; }
.d-critical { background:rgba(255,71,87,.15);  color:#ff4757; }
.d-high     { background:rgba(255,107,53,.15); color:#ff6b35; }
.d-moderate { background:rgba(255,165,2,.15);  color:#ffa502; }
.d-low      { background:rgba(59,130,246,.15); color:#74b9ff; }
.d-healthy  { background:rgba(16,185,129,.15); color:#10b981; }

@media(max-width:860px) {
  .layout { grid-template-columns:1fr; }
  .detail { position:static; }
}
</style>
</head>
<body>
<nav>
  <div class="nav-logo">Hummingbird</div>
  <div class="nav-sep"></div>
  <div class="nav-sub">Opportunity Clusters</div>
  <div class="nav-spacer"></div>
  <a href="index.html" class="nav-back">\u2190 Map</a>
</nav>

<div class="page">
  <div class="page-eyebrow">Phase II \u00b7 Machine Learning</div>
  <div class="page-title">Opportunity Clusters</div>
  <div class="page-sub">K-Means segmentation of """ + f"{total:,}" + """ institutions into distinct opportunity archetypes.
    IPEDS higher education and 990 nonprofits are modeled separately.</div>

  <div class="source-tabs">
    <div class="s-tab active" id="tabIPEDS">
      IPEDS \u2014 Higher Education <span class="tc">""" + f"{n_ipeds:,}" + """</span>
    </div>
    <div class="s-tab" id="tab990">
      990 \u2014 Nonprofits <span class="tc">""" + f"{n_990:,}" + """</span>
    </div>
  </div>

  <div class="layout">
    <div class="cards" id="cards"></div>
    <div class="detail" id="detail">
      <div class="d-empty">
        <div class="d-empty-arrow">\u2190</div>
        <div class="d-empty-title">Select a cluster</div>
        <div class="d-empty-sub">Click any card to explore its institutions</div>
      </div>
    </div>
  </div>
</div>

<script>
var IPEDS_DATA = """ + ipeds_js + """;
var HB990_DATA = """ + hb990_js + """;

var src       = 'ipeds';
var activeId  = null;
var sortCol   = 'distress';
var sortAsc   = false;
var searchQ   = '';

function getClusters() { return src === 'ipeds' ? IPEDS_DATA : HB990_DATA; }

function setSource(s, tab) {
  src = s; activeId = null; searchQ = '';
  document.querySelectorAll('.s-tab').forEach(function(t) { t.classList.remove('active'); });
  tab.classList.add('active');
  renderCards();
  document.getElementById('detail').innerHTML =
    '<div class="d-empty"><div class="d-empty-arrow">\u2190</div>' +
    '<div class="d-empty-title">Select a cluster</div>' +
    '<div class="d-empty-sub">Click any card to explore its institutions</div></div>';
}

function renderCards() {
  var cl = getClusters();
  var html = '';
  for (var i = 0; i < cl.length; i++) {
    var c = cl[i];
    var active = (activeId === c.id);
    var s = c.stats;
    var chips = '';
    if (src === 'ipeds') {
      chips += '<span class="chip">Distress <b>' + s.median_distress + '</b></span>';
      chips += '<span class="chip">Enroll <b>' + (s.median_enrollment || 0).toLocaleString() + '</b></span>';
    } else {
      chips += '<span class="chip">Distress <b>' + s.median_distress + '</b></span>';
      chips += '<span class="chip">Rev <b>$' + (s.median_revenue_k || 0).toLocaleString() + 'K</b></span>';
    }
    if (s.median_acres > 0) {
      chips += '<span class="chip">Acres <b>' + s.median_acres + '</b></span>';
    }
    html += '<div class="card' + (active ? ' active' : '') + '" data-cid="' + c.id + '">';
    html += '<div class="card-bar" style="background:' + c.color + '"></div>';
    html += '<div class="card-hdr">';
    html += '<div class="card-icon">' + c.icon + '</div>';
    html += '<div class="card-name">' + c.name + '</div>';
    html += '<div class="card-n">' + c.count.toLocaleString() + '</div>';
    html += '</div>';
    html += '<div class="card-desc">' + c.desc + '</div>';
    html += '<div class="chips">' + chips + '</div>';
    html += '</div>';
  }
  document.getElementById('cards').innerHTML = html;
}

function selectCluster(cid) {
  activeId = cid; searchQ = ''; sortCol = 'distress'; sortAsc = false;
  renderCards();
  var cl = getClusters();
  var c = null;
  for (var i = 0; i < cl.length; i++) { if (cl[i].id === cid) { c = cl[i]; break; } }
  if (!c) return;
  renderDetail(c);
}

function renderDetail(c) {
  var s = c.stats;
  var isIPEDS = (src === 'ipeds');

  // Stat strip
  var statCols, statHtml = '';
  if (isIPEDS) {
    statCols = 4;
    statHtml += mkStat(s.median_distress, 'Distress Score');
    statHtml += mkStat((s.median_enrollment || 0).toLocaleString(), 'Median Enroll');
    statHtml += mkStat(s.median_acres > 0 ? s.median_acres + ' ac' : '\u2014', 'Median Acres');
    statHtml += mkStat(s.pct_enrollment_decline + '%', 'Enroll Decline');
  } else {
    statCols = 4;
    statHtml += mkStat(s.median_distress, 'Distress Score');
    statHtml += mkStat('$' + (s.median_revenue_k || 0).toLocaleString() + 'K', 'Median Revenue');
    statHtml += mkStat(s.median_acres > 0 ? s.median_acres + ' ac' : '\u2014', 'Median Acres');
    statHtml += mkStat(s.pct_operating_loss + '%', 'Op. Loss');
  }

  // Flag bars
  var flagDefs = isIPEDS
    ? [['Enrollment Decline', s.pct_enrollment_decline, '#ff6b35'],
       ['Operating Losses',   s.pct_operating_losses,  '#ff4757'],
       ['Neg. Net Worth',     s.pct_negative_net_worth, '#ff4757'],
       ['Land Potential',     s.pct_land_potential,    '#00b894']]
    : [['Operating Loss',     s.pct_operating_loss,     '#ff6b35'],
       ['Neg. Net Assets',    s.pct_negative_assets,    '#ff4757'],
       ['High Debt',          s.pct_high_debt,          '#ffa502'],
       ['Consec. Losses',     s.pct_consecutive_losses, '#ff6b35'],
       ['Land Potential',     s.pct_land_potential,     '#00b894']];

  var flagHtml = '';
  for (var fi = 0; fi < flagDefs.length; fi++) {
    var fd = flagDefs[fi];
    var pv = fd[1] || 0;
    flagHtml += '<div class="frow">'
      + '<div class="flabel">' + fd[0] + '</div>'
      + '<div class="fbar-wrap"><div class="fbar" style="width:' + Math.min(pv, 100) + '%;background:' + fd[2] + '"></div></div>'
      + '<div class="fpct">' + pv + '%</div>'
      + '</div>';
  }

  var html = '<div class="d-head" style="border-left:3px solid ' + c.color + '">'
    + '<div class="dh-icon">' + c.icon + '</div>'
    + '<div>'
    + '<div class="dh-name" style="color:' + c.color + '">' + c.name + '</div>'
    + '<div class="dh-count">' + c.count.toLocaleString() + ' institutions \u00b7 ' + s.top_region + ' most common</div>'
    + '<div class="dh-desc">' + c.desc + '</div>'
    + '</div></div>'
    + '<div class="stat-strip" style="grid-template-columns:repeat(' + statCols + ',1fr)">' + statHtml + '</div>'
    + '<div class="flags"><div class="flags-title">Distress Indicators</div>' + flagHtml + '</div>'
    + buildTable(c);

  document.getElementById('detail').innerHTML = html;
}

function mkStat(val, label) {
  return '<div class="stat-cell"><span class="stat-v">' + val + '</span><div class="stat-l">' + label + '</div></div>';
}

function buildTable(c) {
  var filtered = filterRows(c.rows);
  var sorted   = sortRows(filtered);
  var isIPEDS  = (src === 'ipeds');

  var headers = isIPEDS
    ? [['distress','Distress'],['name','Institution'],['enrollment','Enroll'],['revenue','Revenue'],['acres','Acres'],['urbanization','Urban']]
    : [['distress','Distress'],['name','Institution'],['revenue','Revenue'],['acres','Acres'],['type','Type']];

  var thead = '';
  for (var hi = 0; hi < headers.length; hi++) {
    var h = headers[hi];
    var arrow = (sortCol === h[0]) ? (sortAsc ? ' \u2191' : ' \u2193') : '';
    thead += '<th data-col="' + h[0] + '">' + h[1] + arrow + '</th>';
  }

  var tbody = '';
  var limit = Math.min(sorted.length, 200);
  for (var ri = 0; ri < limit; ri++) {
    var r = sorted[ri];
    var dc = (r.category || '').toLowerCase().replace(/ /g, '');
    var dpill = '<span class="dp d-' + dc + '">' + (r.distress || 0).toFixed(0) + ' ' + (r.category || '') + '</span>';
    var tshort = (r.type || '').replace('Private Non-Profit | ', 'NP ').replace('Private For-Profit | ', 'FP ').replace('Public | ', 'Pub ');
    var urb = (r.urbanization || '').split(':')[0] || '\u2014';
    var nameCell = '<td class="td-name">' + esc(r.name) + '<small>' + esc(r.city) + ', ' + esc(r.state) + '</small></td>';
    var tds = '<td>' + dpill + '</td>' + nameCell;
    if (isIPEDS) {
      tds += '<td>' + (r.enrollment != null ? r.enrollment.toLocaleString() : '\u2014') + '</td>';
      tds += '<td>' + (r.revenue != null ? '$' + r.revenue.toLocaleString() + 'K' : '\u2014') + '</td>';
      tds += '<td>' + (r.acres != null ? r.acres : '\u2014') + '</td>';
      tds += '<td style="font-size:9px;color:var(--text2)">' + esc(urb) + '</td>';
    } else {
      tds += '<td>' + (r.revenue != null ? '$' + r.revenue.toLocaleString() + 'K' : '\u2014') + '</td>';
      tds += '<td>' + (r.acres != null ? r.acres : '\u2014') + '</td>';
      tds += '<td style="font-size:9px;color:var(--text2)">' + esc(tshort) + '</td>';
    }
    tbody += '<tr>' + tds + '</tr>';
  }

  return '<div class="tbar">'
    + '<input class="tsearch" placeholder="Search institutions\u2026" oninput="onSearch(this.value)" value="' + esc(searchQ) + '">'
    + '<span class="tcount" id="tcount">' + filtered.length.toLocaleString() + ' shown</span>'
    + '<button class="export-btn" onclick="doExport()">\u2193 CSV</button>'
    + '</div>'
    + '<div class="twrap"><table><thead><tr>' + thead + '</tr></thead><tbody>' + tbody + '</tbody></table></div>';
}

function filterRows(rows) {
  if (!searchQ) return rows;
  var lq = searchQ.toLowerCase();
  var out = [];
  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];
    if ((r.name||'').toLowerCase().indexOf(lq) >= 0 ||
        (r.state||'').toLowerCase().indexOf(lq) >= 0 ||
        (r.city||'').toLowerCase().indexOf(lq) >= 0) {
      out.push(r);
    }
  }
  return out;
}

function sortRows(rows) {
  var col = sortCol, asc = sortAsc;
  return rows.slice().sort(function(a, b) {
    var av = a[col], bv = b[col];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    if (typeof av === 'string') av = av.toLowerCase();
    if (typeof bv === 'string') bv = bv.toLowerCase();
    var cmp = av < bv ? -1 : av > bv ? 1 : 0;
    return asc ? cmp : -cmp;
  });
}

function setSort(col) {
  if (sortCol === col) { sortAsc = !sortAsc; } else { sortCol = col; sortAsc = false; }
  var cl = getClusters();
  for (var i = 0; i < cl.length; i++) {
    if (cl[i].id === activeId) { renderDetail(cl[i]); break; }
  }
}

function onSearch(val) {
  searchQ = val;
  var cl = getClusters();
  for (var i = 0; i < cl.length; i++) {
    if (cl[i].id === activeId) { renderDetail(cl[i]); break; }
  }
}

function esc(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function doExport() {
  var cl = getClusters();
  var c = null;
  for (var i = 0; i < cl.length; i++) { if (cl[i].id === activeId) { c = cl[i]; break; } }
  if (!c) return;
  var rows = filterRows(c.rows);
  var cols = src === 'ipeds'
    ? ['name','state','city','type','distress','category','enrollment','revenue','acres','risk_tier','urbanization']
    : ['name','state','city','type','distress','category','revenue','acres'];
  var csvStr = cols.join(',') + '\\n' + rows.map(function(r) {
    return cols.map(function(col) { return '"' + String(r[col] != null ? r[col] : '').replace(/"/g, '""') + '"'; }).join(',');
  }).join('\\n');
  var blob = new Blob([csvStr], {type:'text/csv'});
  var a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'hb_cluster_' + c.name.replace(/[^a-z0-9]+/g,'_').toLowerCase() + '.csv';
  a.click();
}

// Init
document.getElementById('tabIPEDS').addEventListener('click', function() { setSource('ipeds', this); });
document.getElementById('tab990').addEventListener('click', function() { setSource('990', this); });
document.getElementById('cards').addEventListener('click', function(e) {
  var card = e.target.closest('[data-cid]');
  if (card) selectCluster(parseInt(card.getAttribute('data-cid'), 10));
});
document.getElementById('detail').addEventListener('click', function(e) {
  var th = e.target.closest('th[data-col]');
  if (th) setSort(th.getAttribute('data-col'));
});
renderCards();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description='Hummingbird Cluster Engine')
    parser.add_argument('--master',   default=DEFAULT_MASTER)
    parser.add_argument('--osm',      default=DEFAULT_OSM)
    parser.add_argument('--out-html', default=DEFAULT_OUT_HTML)
    parser.add_argument('--out-csv',  default=DEFAULT_OUT_CSV)
    parser.add_argument('--ipeds-k',  type=int, default=IPEDS_K)
    parser.add_argument('--hb990-k',  type=int, default=HB990_K)
    args = parser.parse_args()

    try:
        import sklearn
    except ImportError:
        print("ERROR: scikit-learn not installed. Run: pip install scikit-learn pandas numpy")
        sys.exit(1)

    print(f"Loading {args.master}...")
    csv.field_size_limit(100 * 1024 * 1024)
    rows = []
    with open(args.master, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            rows.append(row)
    print(f"  {len(rows):,} rows loaded")

    if os.path.exists(args.osm):
        print(f"Loading OSM data from {args.osm}...")
        osm_cats = {}
        csv.field_size_limit(100 * 1024 * 1024)
        with open(args.osm, newline='', encoding='utf-8') as f:
            for r in csv.DictReader(f):
                name = r.get('institution_name', '')
                cats = r.get('osm_env_categories', '')
                if name and cats and cats != 'ERROR':
                    osm_cats[name] = [c.strip() for c in cats.split(',') if c.strip()]
        print(f"  {len(osm_cats):,} institutions with OSM data")
        for row in rows:
            row['_osm_categories'] = osm_cats.get(row.get('institution_name', ''), [])
    else:
        print(f"  OSM file not found — skipping")
        for row in rows:
            row['_osm_categories'] = []

    # IPEDS
    print(f"\nBuilding IPEDS feature matrix...")
    ipeds_matrix, _, ipeds_idx = extract_ipeds_features(rows)
    print(f"  {len(ipeds_matrix):,} IPEDS institutions x {len(ipeds_matrix[0])} features")
    print(f"  Running K-Means (k={args.ipeds_k})...")
    ipeds_labels, _, _ = run_kmeans(ipeds_matrix, args.ipeds_k)

    for pos, row_idx in enumerate(ipeds_idx):
        rows[row_idx]['cluster_ipeds'] = str(ipeds_labels[pos])

    ipeds_cluster_rows = defaultdict(list)
    for pos, row_idx in enumerate(ipeds_idx):
        ipeds_cluster_rows[ipeds_labels[pos]].append(rows[row_idx])

    ipeds_clusters = []
    for cid in sorted(ipeds_cluster_rows.keys()):
        crows = ipeds_cluster_rows[cid]
        info = auto_name_ipeds_cluster(crows, cid)
        info['rows'] = build_preview_rows(crows, 'IPEDS')
        ipeds_clusters.append(info)
        print(f"  Cluster {cid}: {info['icon']} {info['name']} — {info['count']:,} institutions")

    ipeds_clusters.sort(key=lambda c: -c['stats']['median_distress'])

    # 990
    print(f"\nBuilding 990 feature matrix...")
    hb990_matrix, _, hb990_idx = extract_990_features(rows)
    print(f"  {len(hb990_matrix):,} 990 institutions x {len(hb990_matrix[0])} features")
    print(f"  Running K-Means (k={args.hb990_k})...")
    hb990_labels, _, _ = run_kmeans(hb990_matrix, args.hb990_k)

    for pos, row_idx in enumerate(hb990_idx):
        rows[row_idx]['cluster_990'] = str(hb990_labels[pos])

    hb990_cluster_rows = defaultdict(list)
    for pos, row_idx in enumerate(hb990_idx):
        hb990_cluster_rows[hb990_labels[pos]].append(rows[row_idx])

    hb990_clusters = []
    for cid in sorted(hb990_cluster_rows.keys()):
        crows = hb990_cluster_rows[cid]
        info = auto_name_990_cluster(crows, cid)
        info['rows'] = build_preview_rows(crows, '990')
        hb990_clusters.append(info)
        print(f"  Cluster {cid}: {info['icon']} {info['name']} — {info['count']:,} institutions")

    hb990_clusters.sort(key=lambda c: -c['stats']['median_distress'])

    # Write CSV
    print(f"\nWriting enriched CSV to {args.out_csv}...")
    fieldnames = list(rows[0].keys())
    for col in ['cluster_ipeds', 'cluster_990']:
        if col not in fieldnames:
            fieldnames.append(col)
    fieldnames = [f for f in fieldnames if f != '_osm_categories']
    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    with open(args.out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Done.")

    # Write HTML
    print(f"\nGenerating {args.out_html}...")
    html = build_html(ipeds_clusters, hb990_clusters)
    with open(args.out_html, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  Done. ({len(html):,} chars)")

    print(f"\n✓ Cluster pipeline complete.")
    print(f"  IPEDS: {len(ipeds_clusters)} clusters across {len(ipeds_matrix):,} institutions")
    print(f"  990:   {len(hb990_clusters)} clusters across {len(hb990_matrix):,} institutions")
    print(f"  HTML:  {args.out_html}")
    print(f"  CSV:   {args.out_csv}")

    return ipeds_clusters, hb990_clusters


if __name__ == '__main__':
    main()