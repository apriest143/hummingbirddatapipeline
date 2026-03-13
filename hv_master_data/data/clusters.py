"""
clusters.py — Hummingbird Ventures Opportunity Cluster Engine v2
================================================================
Reads Hummingbird_Master_Combined_v6.csv (+ optional osm_land_results.csv),
fits K-Means models and generates clusters.html.

METHODOLOGY OVERVIEW
--------------------
IPEDS institutions are split by ownership type before clustering:
  - Public institutions       → K=3
  - Private Non-Profit        → K=3
  - Private For-Profit        → K=3
  Total: up to 9 named IPEDS clusters

990 nonprofits are clustered together (K=5) since they share a
common reporting structure regardless of sub-type.

Features used:
  IPEDS: distress subdomain scores, financial ratios, enrollment
         metrics, closure risk, acreage, binary flags
  990:   distress subdomain scores, financial ratios, PPE,
         acreage, binary flags, institution type one-hot
  OSM:   binary one-hot for each environment category
         (water, winter, outdoor_recreation, etc.) where scraped

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
# Paths
# ---------------------------------------------------------------------------
DEFAULT_MASTER   = 'hv_master_data/data/Hummingbird_Master_Combined_v6.csv'
DEFAULT_OSM      = 'hv_master_data/acreage_scripts/osm_land_results.csv'
DEFAULT_OUT_HTML = 'clusters.html'
DEFAULT_OUT_CSV  = 'hv_master_data/data/Hummingbird_Master_Clustered.csv'

# K values per segment
K_PUBLIC  = 3
K_PNP     = 3
K_PFP     = 3
K_HB990   = 5

# OSM category pipe-separated values we one-hot encode
OSM_CATS = ['water', 'winter', 'outdoor_recreation', 'protected_land',
            'natural_features', 'tourism_leisure', 'infrastructure']

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
    v = row.get(col, '')
    if v in ('', 'nan', 'NaN', 'None', None): return default
    try:
        r = float(v)
        return default if (math.isnan(r) or math.isinf(r)) else r
    except: return default

def flag(row, col):
    return str(row.get(col, '')).strip().lower() in ('1', 'true', 'yes', '1.0')

def safe_log(x):
    if x is None or x <= 0: return None
    return math.log1p(x)

def impute_median(values):
    valid = [v for v in values if v is not None]
    if not valid: return [0.0] * len(values)
    valid.sort()
    med = valid[len(valid) // 2]
    return [v if v is not None else med for v in values]

def sanitize_for_json(obj):
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    return obj

def med_of(rows, col):
    vals = [fv(r, col) for r in rows if fv(r, col) is not None]
    if not vals: return 0
    vals.sort(); return vals[len(vals) // 2]

def pct_of(rows, col):
    return round(sum(1 for r in rows if flag(r, col)) / max(len(rows), 1) * 100)

def top_of(rows, col):
    c = Counter(r.get(col, '') for r in rows if r.get(col, ''))
    return c.most_common(1)[0][0] if c else ''

def osm_cats_for(row):
    raw = row.get('_osm_categories', [])
    if isinstance(raw, str):
        return [c.strip().lower() for c in raw.split(',') if c.strip()]
    return [c.lower() for c in raw]

# ---------------------------------------------------------------------------
# Feature extraction — shared IPEDS base
# ---------------------------------------------------------------------------
def ipeds_feature_names():
    return [
        # Distress subdomain scores (0-100, higher = worse)
        'distress_score',
        'solvency_score', 'liquidity_score', 'operating_score',
        'enrollment_score', 'academic_score', 'trend_score',
        # Closure risk (ML model output, 0-1)
        'closure_risk_score',
        # Financial ratios
        'operating_margin', 'tuition_dependency',
        # Enrollment
        'log_enrollment', 'enrollment_yoy_pct', 'enrollment_2yr_pct',
        'retention_rate', 'graduation_rate',
        # Physical
        'log_acreage', 'log_revenue',
        # Binary flags
        'flag_enrollment_decline', 'flag_small_enrollment',
        'flag_high_tuition_dependency', 'flag_low_retention',
        'flag_operating_losses', 'flag_negative_net_worth',
        'flag_land_potential', 'flag_high_land_potential', 'flag_rural_suburban',
        # Urbanization one-hot
        'urban_city', 'urban_suburb', 'urban_town', 'urban_rural',
        # Region one-hot
        'region_ne', 'region_se', 'region_mw', 'region_sw', 'region_w',
        # OSM environment one-hot
    ] + [f'osm_{c}' for c in OSM_CATS]


def extract_ipeds_row(r):
    """Extract feature vector for a single IPEDS row."""
    ug = (r.get('urbanization', '') or '').split(':')[0].strip()
    reg = STATE_REGION.get(r.get('state', ''), 'Unknown')
    acres = fv(r, 'acreage_primary') or fv(r, 'scraper_acres') or fv(r, 'osm_acres')
    cats = osm_cats_for(r)

    vec = [
        fv(r, 'distress_score'),
        fv(r, 'solvency_score_ipeds'),
        fv(r, 'liquidity_score_ipeds'),
        fv(r, 'operating_score_ipeds'),
        fv(r, 'enrollment_score_ipeds'),
        fv(r, 'academic_score_ipeds'),
        fv(r, 'trend_score_ipeds'),
        fv(r, 'closure_risk_score'),
        fv(r, 'operating_margin'),
        fv(r, 'tuition_dependency'),
        safe_log(fv(r, 'enrollment_2024')),
        fv(r, 'enrollment_yoy_pct'),
        fv(r, 'enrollment_2yr_pct'),
        fv(r, 'retention_rate'),
        fv(r, 'graduation_rate'),
        safe_log(acres),
        safe_log(fv(r, 'revenue_2024')),
        1.0 if flag(r, 'flag_enrollment_decline') else 0.0,
        1.0 if flag(r, 'flag_small_enrollment') else 0.0,
        1.0 if flag(r, 'flag_high_tuition_dependency') else 0.0,
        1.0 if flag(r, 'flag_low_retention') else 0.0,
        1.0 if flag(r, 'flag_operating_losses') else 0.0,
        1.0 if flag(r, 'flag_negative_net_worth') else 0.0,
        1.0 if flag(r, 'flag_land_potential') else 0.0,
        1.0 if flag(r, 'flag_high_land_potential') else 0.0,
        1.0 if flag(r, 'flag_rural_suburban') else 0.0,
        1.0 if ug == 'City' else 0.0,
        1.0 if ug == 'Suburb' else 0.0,
        1.0 if ug == 'Town' else 0.0,
        1.0 if ug == 'Rural' else 0.0,
        1.0 if reg == 'Northeast' else 0.0,
        1.0 if reg == 'Southeast' else 0.0,
        1.0 if reg == 'Midwest' else 0.0,
        1.0 if reg == 'Southwest' else 0.0,
        1.0 if reg == 'West' else 0.0,
    ] + [1.0 if cat in cats else 0.0 for cat in OSM_CATS]
    return vec


def build_ipeds_segment(rows, type_filter):
    """Extract feature matrix for one institution-type segment."""
    feat_names = ipeds_feature_names()
    raw = defaultdict(list)
    valid_indices = []

    for i, r in enumerate(rows):
        if r.get('data_source') != 'IPEDS': continue
        it = r.get('institution_type', '')
        if type_filter == 'public' and 'Public' not in it: continue
        if type_filter == 'pnp' and 'Non-Profit' not in it: continue
        if type_filter == 'pfp' and 'For-Profit' not in it: continue

        vec = extract_ipeds_row(r)
        for j, name in enumerate(feat_names):
            raw[name].append(vec[j])
        valid_indices.append(i)

    # Impute continuous columns
    continuous = ['distress_score','solvency_score','liquidity_score','operating_score',
                  'enrollment_score','academic_score','trend_score','closure_risk_score',
                  'operating_margin','tuition_dependency','log_enrollment',
                  'enrollment_yoy_pct','enrollment_2yr_pct','retention_rate',
                  'graduation_rate','log_acreage','log_revenue']
    for col in continuous:
        raw[col] = impute_median(raw[col])

    n = len(valid_indices)
    matrix = [[raw[feat][i] for feat in feat_names] for i in range(n)]
    return matrix, feat_names, valid_indices


# ---------------------------------------------------------------------------
# 990 feature extraction
# ---------------------------------------------------------------------------
def extract_990_features(rows):
    feature_names = [
        'distress_score',
        'solvency_score', 'liquidity_score', 'operating_score',
        'trend_score', 'red_flag_score',
        'operating_margin', 'equity_ratio',
        'log_revenue', 'log_assets', 'log_ppe', 'log_acreage',
        'flag_operating_loss', 'flag_negative_net_assets',
        'flag_high_debt', 'flag_consecutive_losses',
        'flag_revenue_decline_1yr', 'flag_low_equity',
        # Type one-hot
        'type_religious', 'type_housing', 'type_recreation',
        'type_educational', 'type_youth', 'type_environmental',
        'type_human_services', 'type_animal', 'type_agriculture', 'type_other',
        # Region
        'region_ne', 'region_se', 'region_mw', 'region_sw', 'region_w',
        # OSM
    ] + [f'osm_{c}' for c in OSM_CATS]

    raw = defaultdict(list)
    valid_indices = []

    for i, r in enumerate(rows):
        if r.get('data_source') != 'Hummingbird_990': continue

        it = r.get('institution_type', '')
        acres = fv(r, 'scraper_acres') or fv(r, 'osm_acres')
        cats = osm_cats_for(r)
        reg = STATE_REGION.get(r.get('state', ''), 'Unknown')

        vals = [
            fv(r, 'distress_score'),
            fv(r, 'solvency_score_990'),
            fv(r, 'liquidity_score_990'),
            fv(r, 'operating_score_990'),
            fv(r, 'trend_score_990'),
            fv(r, 'red_flag_score_990'),
            fv(r, 'operating_margin'),
            fv(r, 'equity_ratio'),
            safe_log(fv(r, 'revenue_2024')),
            safe_log(fv(r, 'assets_2024')),
            safe_log(fv(r, 'plant_property_equipment')),
            safe_log(acres),
            1.0 if flag(r, 'flag_990_operating_loss') else 0.0,
            1.0 if flag(r, 'flag_990_negative_net_assets') else 0.0,
            1.0 if flag(r, 'flag_990_high_debt') else 0.0,
            1.0 if flag(r, 'flag_990_consecutive_losses') else 0.0,
            1.0 if flag(r, 'flag_990_revenue_decline_1yr') else 0.0,
            1.0 if flag(r, 'flag_990_low_equity') else 0.0,
            1.0 if it == 'Religious Institution' else 0.0,
            1.0 if it == 'Housing/Residential' else 0.0,
            1.0 if it == 'Recreation/Camp' else 0.0,
            1.0 if it == 'Educational Institution' else 0.0,
            1.0 if it == 'Youth Development' else 0.0,
            1.0 if it == 'Environmental/Conservation' else 0.0,
            1.0 if it == 'Human Services' else 0.0,
            1.0 if it == 'Animal/Wildlife' else 0.0,
            1.0 if it == 'Agriculture/Farm' else 0.0,
            1.0 if it == 'Other Nonprofit' else 0.0,
            1.0 if reg == 'Northeast' else 0.0,
            1.0 if reg == 'Southeast' else 0.0,
            1.0 if reg == 'Midwest' else 0.0,
            1.0 if reg == 'Southwest' else 0.0,
            1.0 if reg == 'West' else 0.0,
        ] + [1.0 if cat in cats else 0.0 for cat in OSM_CATS]

        for j, name in enumerate(feature_names):
            raw[name].append(vals[j])
        valid_indices.append(i)

    continuous = ['distress_score','solvency_score','liquidity_score','operating_score',
                  'trend_score','red_flag_score','operating_margin','equity_ratio',
                  'log_revenue','log_assets','log_ppe','log_acreage']
    for col in continuous:
        raw[col] = impute_median(raw[col])

    n = len(valid_indices)
    matrix = [[raw[feat][i] for feat in feature_names] for i in range(n)]
    return matrix, feature_names, valid_indices


# ---------------------------------------------------------------------------
# K-Means
# ---------------------------------------------------------------------------
def run_kmeans(matrix, k, seed=42):
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    X = StandardScaler().fit_transform(matrix)
    km = KMeans(n_clusters=k, random_state=seed, n_init=20, max_iter=500)
    labels = km.fit_predict(X)
    return labels.tolist(), km.inertia_


# ---------------------------------------------------------------------------
# Cluster naming — IPEDS by segment
# ---------------------------------------------------------------------------
def name_public_cluster(rows, cid):
    ds   = med_of(rows, 'distress_score')
    enr  = med_of(rows, 'enrollment_2024')
    rev  = med_of(rows, 'revenue_2024')
    acres = med_of(rows, 'acreage_primary') or med_of(rows, 'scraper_acres') or 0
    cr   = med_of(rows, 'closure_risk_score') or 0
    pct_enr_dec = pct_of(rows, 'flag_enrollment_decline')
    pct_op_loss = pct_of(rows, 'flag_operating_losses')
    top_it = top_of(rows, 'institution_type')

    is_4yr = '4-year' in top_it
    is_2yr = '2-year' in top_it

    if is_4yr and enr > 5000:
        name, icon, color = 'Public Universities', '🏛', '#3b82f6'
        desc = ('Large public 4-year universities and state colleges. '
                'Substantial land holdings, stable enrollment, and strong revenue bases. '
                'Low acquisition probability but useful as market comparables.')
        hv_angle = 'Low direct acquisition target — useful as regional comparables and partnership candidates.'
    elif is_2yr and enr > 1000:
        name, icon, color = 'Public Community Colleges', '🎓', '#6c5ce7'
        desc = ('Public 2-year community colleges serving regional populations. '
                'Financially stable with meaningful acreage, often in suburban or rural settings. '
                'Underleveraged land holdings relative to their footprint.')
        hv_angle = 'Monitor for campus consolidations and satellite facility dispositions. Acreage often underleveraged.'
    else:
        name, icon, color = 'Distressed Public Institutions', '⚠️', '#ffa502'
        desc = ('Smaller or struggling public institutions — branch campuses, admin units, '
                'and underfunded community colleges. Enrollment pressure and budget constraints '
                'creating long-term viability questions.')
        hv_angle = 'Watch list — public institutions occasionally divest satellite campuses and auxiliary facilities.'

    return _make_cluster(cid, name, icon, color, desc, hv_angle, rows, 'IPEDS')


def name_pnp_cluster(rows, cid):
    ds      = med_of(rows, 'distress_score')
    enr     = med_of(rows, 'enrollment_2024')
    acres   = med_of(rows, 'acreage_primary') or med_of(rows, 'scraper_acres') or 0
    cr      = med_of(rows, 'closure_risk_score') or 0
    pct_op_loss = pct_of(rows, 'flag_operating_losses')
    pct_rural   = pct_of(rows, 'flag_rural_suburban')
    top_urb     = (top_of(rows, 'urbanization') or '').split(':')[0].strip()

    if cr > 0.35 and ds > 30:
        name, icon, color = 'High-Risk Private Colleges', '🔴', '#ff4757'
        desc = ('Small private non-profit colleges with elevated closure risk scores and '
                'high distress — median enrollment under 300, operating losses common, '
                'and many showing negative net worth trends. Imminent restructuring candidates.')
        hv_angle = 'PRIORITY PIPELINE. Highest closure probability in the NP segment. Many will transact within 3-5 years.'
    elif top_urb == 'City':
        name, icon, color = 'Urban Private NP Colleges', '🏙', '#ff6b35'
        desc = ('Urban private non-profit colleges concentrated in city markets. '
                'Stable enrollment but operating margins thin. Asset value driven by '
                'urban real estate rather than campus acreage.')
        hv_angle = 'Urban land value play. Central locations and building assets can exceed functional campus value.'
    else:
        name, icon, color = 'Rural & Suburban Private Colleges', '🌲', '#00b894'
        desc = ('Private non-profit colleges in suburban and rural settings with meaningful '
                'acreage (median ~100 acres). Financially moderate with lower closure risk '
                'but carrying operating loss exposure. The classic Hummingbird land asset profile.')
        hv_angle = 'PRIMARY LAND TARGET. 100ac median, suburban/rural locations, moderate distress. Build relationships now.'

    return _make_cluster(cid, name, icon, color, desc, hv_angle, rows, 'IPEDS')


def name_pfp_cluster(rows, cid):
    ds   = med_of(rows, 'distress_score')
    enr  = med_of(rows, 'enrollment_2024')
    rev  = med_of(rows, 'revenue_2024') or 0
    pct_small = pct_of(rows, 'flag_small_enrollment')
    pct_enr_dec = pct_of(rows, 'flag_enrollment_decline')
    pct_op_loss = pct_of(rows, 'flag_operating_losses')
    acres = med_of(rows, 'acreage_primary') or med_of(rows, 'scraper_acres') or 0

    if enr < 150 and pct_small > 85:
        name, icon, color = 'Micro Vocational Schools', '✂️', '#a29bfe'
        desc = ('Tiny for-profit trade and vocational programs — cosmetology, barbering, '
                'massage therapy, and similar. Median enrollment under 150, urban-concentrated, '
                'minimal physical footprint. High distress, low asset value.')
        hv_angle = 'Low direct value — no meaningful land assets. Monitor for license/accreditation collapses.'
    elif pct_enr_dec > 25 and ds > 30:
        name, icon, color = 'Declining For-Profit Colleges', '📉', '#ff4757'
        desc = ('Mid-size for-profit colleges with accelerating enrollment decline and '
                'mounting financial pressure. Often facing regulatory scrutiny. '
                'Some have real campus assets worth recovering post-closure.')
        hv_angle = 'Opportunistic — target post-closure or receivership scenarios where campus assets are available.'
    else:
        name, icon, color = 'For-Profit Trade Schools', '🔧', '#fd79a8'
        desc = ('Small-to-mid for-profit trade and career schools with moderate distress. '
                'Primarily 2-year programs in urban markets. Limited land value but '
                'potential lease or facility reuse scenarios.')
        hv_angle = 'Limited direct value. Facility lease-back or sublease plays possible in strong urban markets.'

    return _make_cluster(cid, name, icon, color, desc, hv_angle, rows, 'IPEDS')


def name_990_cluster(rows, cid):
    ds   = med_of(rows, 'distress_score')
    rev  = med_of(rows, 'revenue_2024') or 0
    ppe  = med_of(rows, 'plant_property_equipment') or 0
    acres = med_of(rows, 'scraper_acres') or med_of(rows, 'osm_acres') or 0
    eq   = med_of(rows, 'equity_ratio') or 0

    pct_op_loss  = pct_of(rows, 'flag_990_operating_loss')
    pct_neg_ast  = pct_of(rows, 'flag_990_negative_net_assets')
    pct_hi_debt  = pct_of(rows, 'flag_990_high_debt')
    pct_consec   = pct_of(rows, 'flag_990_consecutive_losses')
    pct_rev_dec  = pct_of(rows, 'flag_990_revenue_decline_1yr')

    top_type = top_of(rows, 'institution_type')
    types = Counter(r.get('institution_type', '') for r in rows)

    # Detect dormant/near-closed: near-zero revenue, high revenue decline, but not insolvent
    is_dormant = rev < 75000 and pct_rev_dec > 70 and pct_neg_ast < 10

    if is_dormant:
        name, icon, color = 'Dormant / Near-Closed', '💤', '#636e72'
        desc = ('Organizations with near-zero revenue and steep revenue decline suggesting '
                'they are dormant, winding down, or effectively closed. '
                'Balance sheets may appear clean but operational activity has essentially ceased.')
        hv_angle = 'Low value as operating entities. Title search and asset recovery plays only.'
    elif top_type == 'Housing/Residential' and pct_neg_ast > 80:
        name, icon, color = 'Insolvent Housing Nonprofits', '🏘', '#ff4757'
        desc = ('Housing and residential nonprofits with deeply negative net assets and '
                'significant physical plant. Structurally insolvent but operationally active. '
                'Real estate assets often exceed remaining organizational value.')
        hv_angle = 'Real estate recovery play. PPE median significant — buildings may be viable for adaptive reuse.'
    elif ds > 70 and pct_neg_ast > 70 and pct_hi_debt > 80:
        name, icon, color = 'Acutely Distressed Nonprofits', '🔴', '#ff4757'
        desc = ('The most severely distressed cluster — highest distress scores, deeply negative '
                'equity, and near-universal operating losses. Religious, recreation, and youth '
                'organizations dominate. Many are functionally at end-of-life.')
        hv_angle = 'Late-stage distress. Fastest path to transaction — sort by acreage and PPE for land recovery value.'
    elif pct_neg_ast > 75 and pct_hi_debt > 85 and pct_op_loss > 85:
        name, icon, color = 'Insolvent Educational & Faith Orgs', '🏫', '#ff6b35'
        desc = ('Educational institutions and faith organizations that are operationally active '
                'but deeply insolvent — 80%+ negative net assets and near-universal operating '
                'losses. Often running camps, retreat centers, or community education programs.')
        hv_angle = 'Facility acquisition targets. Screen by PPE and acreage — many hold real physical plant worth recovering.'
    elif pct_op_loss < 5 and pct_consec < 5 and pct_hi_debt > 50:
        name, icon, color = 'Debt-Burdened but Operational', '⚡', '#ffa502'
        desc = ('Nonprofits that are currently cash-flow positive and not running losses, '
                'but carry heavy debt loads and in many cases negative net assets. '
                '"Zombie" organizations — functioning today but structurally fragile.')
        hv_angle = 'Watch list — these orgs are one bad year from crisis. Build relationships early.'
    elif top_type == 'Agriculture/Farm':
        name, icon, color = 'Distressed Ag & Farm Orgs', '🌾', '#00b894'
        desc = ('Agricultural and farm nonprofits under financial stress. '
                'Typically very small revenue but often holding land assets. '
                'Conservation easements and agricultural land plays relevant here.')
        hv_angle = 'Agricultural land potential. Conservation easement or farm-to-table programming conversion.'
    else:
        name, icon, color = 'Distressed Community Orgs', '🤝', '#74b9ff'
        desc = ('Community-serving nonprofits — youth development, recreation, religious, '
                'and human services — showing financial stress. Varied asset profiles. '
                'Many hold camp, retreat, or community facility real estate.')
        hv_angle = 'Camp and retreat facility plays. Screen by PPE and acreage for real estate potential.'

    return _make_cluster(cid, name, icon, color, desc, hv_angle, rows, '990')


def _make_cluster(cid, name, icon, color, desc, hv_angle, rows, source):
    ds  = med_of(rows, 'distress_score')
    cr  = med_of(rows, 'closure_risk_score') if source == 'IPEDS' else None
    enr = med_of(rows, 'enrollment_2024') if source == 'IPEDS' else None
    rev = med_of(rows, 'revenue_2024') or 0
    acres_val = med_of(rows, 'acreage_primary') or med_of(rows, 'scraper_acres') or med_of(rows, 'osm_acres') or 0
    ppe = med_of(rows, 'plant_property_equipment') if source == '990' else None
    top_region = Counter(STATE_REGION.get(r.get('state', ''), 'Unknown') for r in rows).most_common(1)[0][0]

    stats = {
        'median_distress': round(ds, 1) if ds else 0,
        'median_acres': round(acres_val, 1),
        'top_region': top_region,
        'count': len(rows),
    }

    if source == 'IPEDS':
        stats.update({
            'median_enrollment': int(round(enr)) if enr else 0,
            'median_revenue_m': round(rev / 1e6, 1) if rev else 0,
            'median_closure_risk': round(cr, 3) if cr else 0,
            'pct_enrollment_decline': pct_of(rows, 'flag_enrollment_decline'),
            'pct_operating_losses':   pct_of(rows, 'flag_operating_losses'),
            'pct_negative_net_worth': pct_of(rows, 'flag_negative_net_worth'),
            'pct_land_potential':     pct_of(rows, 'flag_land_potential'),
            'pct_rural':              pct_of(rows, 'flag_rural_suburban'),
            'top_type': top_of(rows, 'institution_type'),
            'top_urb':  (top_of(rows, 'urbanization') or '').split(':')[0].strip(),
        })
    else:
        stats.update({
            'median_revenue_k': int(round(rev / 1000)) if rev else 0,
            'median_ppe_k':     int(round(ppe / 1000)) if ppe else 0,
            'pct_operating_loss':    pct_of(rows, 'flag_990_operating_loss'),
            'pct_negative_assets':   pct_of(rows, 'flag_990_negative_net_assets'),
            'pct_high_debt':         pct_of(rows, 'flag_990_high_debt'),
            'pct_consecutive_losses':pct_of(rows, 'flag_990_consecutive_losses'),
            'pct_revenue_decline':   pct_of(rows, 'flag_990_revenue_decline_1yr'),
            'top_type': top_of(rows, 'institution_type'),
        })

    return {
        'id': cid, 'name': name, 'icon': icon, 'color': color,
        'desc': desc, 'hv_angle': hv_angle,
        'count': len(rows), 'source': source, 'stats': stats,
        'rows': build_preview_rows(rows, source),
    }


# ---------------------------------------------------------------------------
# Preview rows
# ---------------------------------------------------------------------------
def build_preview_rows(cluster_rows, source):
    sorted_rows = sorted(cluster_rows, key=lambda r: -(fv(r, 'distress_score') or 0))[:200]
    out = []
    for r in sorted_rows:
        acres = fv(r, 'acreage_primary') or fv(r, 'scraper_acres') or fv(r, 'osm_acres')
        rev   = fv(r, 'revenue_2024')
        enr   = fv(r, 'enrollment_2024')
        cr    = fv(r, 'closure_risk_score')
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
            'closure_risk': round(cr, 3) if cr else None,
            'risk_tier':   r.get('risk_tier', ''),
            'urbanization': r.get('urbanization', ''),
        })
    return out


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------
def build_html(ipeds_clusters, hb990_clusters, meta):
    ipeds_js = json.dumps(sanitize_for_json([{
        'id': c['id'], 'name': c['name'], 'icon': c['icon'], 'color': c['color'],
        'desc': c['desc'], 'hv_angle': c['hv_angle'],
        'count': c['count'], 'source': c['source'],
        'stats': c['stats'], 'rows': c['rows'],
    } for c in ipeds_clusters]), ensure_ascii=False)

    hb990_js = json.dumps(sanitize_for_json([{
        'id': c['id'], 'name': c['name'], 'icon': c['icon'], 'color': c['color'],
        'desc': c['desc'], 'hv_angle': c['hv_angle'],
        'count': c['count'], 'source': c['source'],
        'stats': c['stats'], 'rows': c['rows'],
    } for c in hb990_clusters]), ensure_ascii=False)

    total   = sum(c['count'] for c in ipeds_clusters) + sum(c['count'] for c in hb990_clusters)
    n_ipeds = sum(c['count'] for c in ipeds_clusters)
    n_990   = sum(c['count'] for c in hb990_clusters)
    osm_pct = meta.get('osm_pct', 0)
    run_date = meta.get('run_date', '')
    osm_scraped = meta.get('osm_scraped', 0)

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
  --text:#e2e4e9; --text2:#8b8fa3; --text3:#3d4f6e;
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

.page { max-width:1280px; margin:0 auto; padding:24px 20px 80px; }

/* Methodology section */
.methodology {
  background:var(--surface); border:1px solid var(--border); border-radius:10px;
  margin-bottom:28px; overflow:hidden;
}
.meth-toggle {
  display:flex; align-items:center; gap:10px; padding:14px 18px;
  cursor:pointer; user-select:none; transition:background .15s;
}
.meth-toggle:hover { background:var(--surface2); }
.meth-toggle-label {
  font-family:var(--mono); font-size:10px; color:var(--accent2);
  text-transform:uppercase; letter-spacing:1.5px; flex:1;
}
.meth-toggle-sub { font-size:11px; color:var(--text2); }
.meth-arrow { font-size:11px; color:var(--text2); transition:transform .2s; }
.meth-arrow.open { transform:rotate(180deg); }
.meth-body { display:none; padding:0 18px 18px; }
.meth-body.open { display:block; }
.meth-grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:16px; }
.meth-card {
  background:var(--surface2); border:1px solid var(--border); border-radius:8px;
  padding:14px; border-left:3px solid var(--accent);
}
.meth-card.hb990 { border-left-color:var(--hb990); }
.meth-card-title {
  font-family:var(--mono); font-size:10px; text-transform:uppercase;
  letter-spacing:1px; margin-bottom:8px;
}
.meth-card.hb990 .meth-card-title { color:var(--hb990); }
.meth-card:not(.hb990) .meth-card-title { color:var(--accent2); }
.meth-card p { font-size:11px; color:var(--text2); line-height:1.7; margin-bottom:8px; }
.meth-card p:last-child { margin-bottom:0; }
.meth-card b { color:var(--text); font-weight:600; }
.meth-feats {
  display:flex; flex-wrap:wrap; gap:4px; margin-top:8px;
}
.meth-feat {
  padding:2px 7px; border-radius:3px; font-family:var(--mono); font-size:9px;
  background:rgba(255,255,255,.04); border:1px solid var(--border); color:var(--text2);
}
.meth-caveat {
  background:rgba(255,165,2,.06); border:1px solid rgba(255,165,2,.2);
  border-radius:6px; padding:10px 14px; font-size:11px; color:#c49a00; line-height:1.6;
}
.meth-caveat b { color:#ffa502; }
.meth-meta {
  display:flex; gap:16px; margin-top:12px; padding-top:12px;
  border-top:1px solid var(--border); flex-wrap:wrap;
}
.meth-meta-item { font-family:var(--mono); font-size:9px; color:var(--text2); }
.meth-meta-item b { color:var(--text); }

/* Hero */
.page-eyebrow { font-family:var(--mono); font-size:10px; color:var(--accent2);
                letter-spacing:2px; text-transform:uppercase; margin-bottom:8px; }
.page-title { font-size:26px; font-weight:700; letter-spacing:-.5px; margin-bottom:6px; }
.page-sub   { font-size:12px; color:var(--text2); line-height:1.6; max-width:680px; margin-bottom:20px; }

/* Source tabs */
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

/* Segment filter (IPEDS only) */
.seg-bar { display:flex; gap:6px; margin-bottom:14px; }
.seg-btn { padding:5px 12px; border-radius:20px; font-size:11px; font-weight:600;
           border:1px solid var(--border); background:var(--surface2);
           color:var(--text2); cursor:pointer; transition:all .15s; }
.seg-btn.active { background:rgba(108,92,231,.15); border-color:rgba(108,92,231,.4); color:var(--accent2); }
.seg-btn:hover:not(.active) { color:var(--text); border-color:var(--text3); }

/* Layout */
.layout { display:grid; grid-template-columns:290px 1fr; gap:14px; align-items:start; }
.cards  { display:flex; flex-direction:column; gap:6px; }

/* Cards */
.card { padding:13px 13px 13px 16px; border-radius:8px; background:var(--surface);
        border:1px solid var(--border); cursor:pointer; transition:background .15s,border-color .15s;
        position:relative; overflow:hidden; user-select:none; }
.card-bar { position:absolute; left:0; top:0; bottom:0; width:3px; }
.card:hover  { background:var(--surface2); }
.card.active { background:var(--surface2); }
.card-hdr { display:flex; align-items:center; gap:8px; margin-bottom:5px; }
.card-icon { font-size:15px; flex-shrink:0; }
.card-name { font-size:12px; font-weight:600; flex:1; line-height:1.2; }
.card-n { font-family:var(--mono); font-size:9px; color:var(--text2); }
.card-desc { font-size:10px; color:var(--text2); line-height:1.5; margin-bottom:7px; }
.chips { display:flex; flex-wrap:wrap; gap:3px; }
.chip { padding:2px 6px; border-radius:3px; background:var(--surface2);
        border:1px solid var(--border); font-family:var(--mono); font-size:9px; color:var(--text2); }
.chip b { color:var(--text); font-weight:500; }

/* Detail */
.detail { background:var(--surface); border:1px solid var(--border);
          border-radius:8px; overflow:hidden; position:sticky; top:68px; }
.d-empty { padding:60px 20px; text-align:center; color:var(--text2); }
.d-empty-arrow { font-size:24px; margin-bottom:10px; }
.d-empty-title { font-size:13px; font-weight:600; margin-bottom:4px; }
.d-empty-sub   { font-size:11px; }
.d-head { padding:16px 18px; border-bottom:1px solid var(--border); background:var(--surface2);
          display:flex; align-items:flex-start; gap:12px; }
.dh-icon  { font-size:22px; flex-shrink:0; margin-top:1px; }
.dh-name  { font-size:17px; font-weight:700; letter-spacing:-.3px; line-height:1.2; }
.dh-count { font-family:var(--mono); font-size:10px; color:var(--text2); margin-top:3px; }
.dh-desc  { font-size:11px; color:var(--text2); line-height:1.5; margin-top:5px; }

/* HV angle callout */
.hv-angle {
  margin:0; padding:10px 16px; border-bottom:1px solid var(--border);
  background:rgba(0,184,148,.06); border-left:3px solid var(--hb990);
  display:flex; align-items:flex-start; gap:8px;
}
.hv-angle-label { font-family:var(--mono); font-size:9px; color:var(--hb990);
                  text-transform:uppercase; letter-spacing:1px; white-space:nowrap; margin-top:1px; }
.hv-angle-text  { font-size:11px; color:#a8e6da; line-height:1.5; }

/* Stats */
.stat-strip { display:grid; border-bottom:1px solid var(--border); }
.stat-cell  { padding:11px 14px; border-right:1px solid var(--border); text-align:center; }
.stat-cell:last-child { border-right:none; }
.stat-v { font-family:var(--mono); font-size:15px; font-weight:500; display:block; }
.stat-l { font-size:9px; color:var(--text2); text-transform:uppercase; letter-spacing:.6px; margin-top:2px; }

/* Flags */
.flags { padding:12px 16px; border-bottom:1px solid var(--border); }
.flags-title { font-family:var(--mono); font-size:9px; color:var(--text2);
               text-transform:uppercase; letter-spacing:1px; margin-bottom:8px; }
.frow   { display:flex; align-items:center; gap:8px; margin-bottom:5px; }
.flabel { font-size:10px; color:var(--text2); width:155px; flex-shrink:0; }
.fbar-wrap { flex:1; height:4px; background:var(--border); border-radius:2px; overflow:hidden; }
.fbar { height:100%; border-radius:2px; }
.fpct { font-family:var(--mono); font-size:9px; color:var(--text2); width:28px; text-align:right; }

/* Table */
.tbar { display:flex; align-items:center; gap:8px; padding:9px 13px;
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
.twrap { max-height:380px; overflow-y:auto; }
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
  .meth-grid { grid-template-columns:1fr; }
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

  <!-- Methodology section -->
  <div class="methodology">
    <div class="meth-toggle" id="methToggle">
      <div class="meth-toggle-label">Methodology &amp; Data Notes</div>
      <div class="meth-toggle-sub">How these clusters were built</div>
      <div class="meth-arrow" id="methArrow">\u25be</div>
    </div>
    <div class="meth-body" id="methBody">
      <div class="meth-grid">
        <div class="meth-card">
          <div class="meth-card-title">IPEDS Clustering \u2014 Higher Education</div>
          <p>IPEDS institutions are <b>split by ownership type before clustering</b>: Public, Private Non-Profit, and Private For-Profit each get their own K-Means model (K=3 each, 9 total clusters). This prevents ownership type from dominating the distance metric and reveals meaningful within-type variation.</p>
          <p><b>Feature set</b> includes distress subdomain scores (solvency, liquidity, operating, enrollment, academic, trend), closure risk score from the ML model, financial ratios (operating margin, tuition dependency), enrollment metrics (YoY and 2-year change, retention, graduation rate), acreage, revenue size, and 13 binary distress flags. Urbanization and region are one-hot encoded.</p>
          <p><b>OSM environment categories</b> (water, outdoor recreation, protected land, etc.) are one-hot encoded for institutions that have been scraped. Currently """ + str(meta.get('osm_scraped',0)) + """ of 12,911 institutions have OSM data (\u2248""" + str(meta.get('osm_pct',0)) + """%).</p>
          <div class="meth-feats">
            <span class="meth-feat">distress_score</span>
            <span class="meth-feat">solvency_score_ipeds</span>
            <span class="meth-feat">operating_score_ipeds</span>
            <span class="meth-feat">enrollment_score_ipeds</span>
            <span class="meth-feat">closure_risk_score</span>
            <span class="meth-feat">operating_margin</span>
            <span class="meth-feat">tuition_dependency</span>
            <span class="meth-feat">enrollment_2024 (log)</span>
            <span class="meth-feat">enrollment_yoy_pct</span>
            <span class="meth-feat">enrollment_2yr_pct</span>
            <span class="meth-feat">retention_rate</span>
            <span class="meth-feat">graduation_rate</span>
            <span class="meth-feat">acreage (log)</span>
            <span class="meth-feat">revenue (log)</span>
            <span class="meth-feat">9 distress flags</span>
            <span class="meth-feat">urbanization (4 one-hot)</span>
            <span class="meth-feat">region (5 one-hot)</span>
            <span class="meth-feat">OSM cats (7 one-hot)</span>
          </div>
        </div>
        <div class="meth-card hb990">
          <div class="meth-card-title">990 Clustering \u2014 Nonprofits</div>
          <p>990 nonprofits are clustered together (K=5) since they share a common IRS reporting structure regardless of sub-type. Institution type is one-hot encoded as a feature so the model can still discover type-correlated patterns.</p>
          <p><b>Feature set</b> uses 990-specific distress subdomain scores (solvency, liquidity, operating, trend, red flag), financial ratios, plant/property/equipment as a proxy for physical assets, acreage where available, and 6 binary distress flags. All 10 institution type categories are one-hot encoded.</p>
          <p><b>Note:</b> <code>flag_land_potential</code> is not computed for 990 institutions in the current pipeline and was excluded. Acreage coverage for 990s is approximately 23% (scraper + OSM combined), so acreage features are imputed at the median for most institutions.</p>
          <div class="meth-feats">
            <span class="meth-feat">distress_score</span>
            <span class="meth-feat">solvency_score_990</span>
            <span class="meth-feat">operating_score_990</span>
            <span class="meth-feat">trend_score_990</span>
            <span class="meth-feat">red_flag_score_990</span>
            <span class="meth-feat">operating_margin</span>
            <span class="meth-feat">equity_ratio</span>
            <span class="meth-feat">revenue (log)</span>
            <span class="meth-feat">assets (log)</span>
            <span class="meth-feat">plant_property_equipment (log)</span>
            <span class="meth-feat">acreage (log)</span>
            <span class="meth-feat">6 distress flags</span>
            <span class="meth-feat">10 type one-hots</span>
            <span class="meth-feat">region (5 one-hot)</span>
            <span class="meth-feat">OSM cats (7 one-hot)</span>
          </div>
        </div>
      </div>
      <div class="meth-caveat">
        <b>Data caveats:</b> All features are standardized (zero mean, unit variance) before clustering so no single feature dominates by scale. Continuous features with missing values are imputed at the column median within each segment \u2014 this means acreage-heavy features are somewhat diluted for institutions without land data. <b>OSM coverage is ~24% of institutions</b>; environment category features will become more discriminative as the scrape completes. Cluster assignments will shift modestly with each re-run as OSM coverage grows. Cluster names are human-assigned heuristics based on centroid inspection \u2014 they represent the dominant archetype in each group, not a guarantee about every member institution.
      </div>
      <div class="meth-meta">
        <div class="meth-meta-item">Last run: <b>""" + run_date + """</b></div>
        <div class="meth-meta-item">IPEDS institutions: <b>""" + f"{n_ipeds:,}" + """</b></div>
        <div class="meth-meta-item">990 institutions: <b>""" + f"{n_990:,}" + """</b></div>
        <div class="meth-meta-item">OSM scraped: <b>""" + f"{osm_scraped:,}" + """ / 12,911</b></div>
        <div class="meth-meta-item">K (Public/NP/FP/990): <b>3 / 3 / 3 / 5</b></div>
        <div class="meth-meta-item">Algorithm: <b>K-Means, n_init=20, StandardScaler</b></div>
      </div>
    </div>
  </div>

  <div class="page-eyebrow">Phase II \u00b7 Machine Learning</div>
  <div class="page-title">Opportunity Clusters</div>
  <div class="page-sub">K-Means segmentation of """ + f"{total:,}" + """ institutions into distinct opportunity archetypes. IPEDS higher education institutions are clustered separately by ownership type (Public, Private NP, Private FP). 990 nonprofits are clustered together.</div>

  <div class="source-tabs">
    <div class="s-tab active" id="tabIPEDS">
      IPEDS \u2014 Higher Education <span class="tc">""" + f"{n_ipeds:,}" + """</span>
    </div>
    <div class="s-tab" id="tab990">
      990 \u2014 Nonprofits <span class="tc">""" + f"{n_990:,}" + """</span>
    </div>
  </div>

  <div class="seg-bar" id="segBar">
    <div class="seg-btn active" data-seg="all">All Types</div>
    <div class="seg-btn" data-seg="public">Public</div>
    <div class="seg-btn" data-seg="pnp">Private NP</div>
    <div class="seg-btn" data-seg="pfp">Private FP</div>
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

var src      = 'ipeds';
var seg      = 'all';
var activeId = null;
var sortCol  = 'distress';
var sortAsc  = false;
var searchQ  = '';

function getClusters() {
  var cl = src === 'ipeds' ? IPEDS_DATA : HB990_DATA;
  if (src !== 'ipeds' || seg === 'all') return cl;
  return cl.filter(function(c) {
    var s = c.stats.top_type || '';
    if (seg === 'public') return s.indexOf('Public') >= 0;
    if (seg === 'pnp')    return s.indexOf('Non-Profit') >= 0;
    if (seg === 'pfp')    return s.indexOf('For-Profit') >= 0;
    return true;
  });
}

function renderCards() {
  var cl = getClusters();
  var html = '';
  for (var i = 0; i < cl.length; i++) {
    var c = cl[i];
    var active = (activeId === c.id);
    var s = c.stats;
    var chips = '';
    chips += '<span class="chip">Distress <b>' + s.median_distress + '</b></span>';
    if (src === 'ipeds') {
      chips += '<span class="chip">Enroll <b>' + (s.median_enrollment || 0).toLocaleString() + '</b></span>';
    } else {
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
    html += '<div class="card-desc">' + c.desc.substring(0, 120) + (c.desc.length > 120 ? '\u2026' : '') + '</div>';
    html += '<div class="chips">' + chips + '</div>';
    html += '</div>';
  }
  if (html === '') html = '<div style="padding:20px;font-size:11px;color:var(--text2)">No clusters in this segment.</div>';
  document.getElementById('cards').innerHTML = html;
}

function selectCluster(cid) {
  activeId = cid; searchQ = ''; sortCol = 'distress'; sortAsc = false;
  renderCards();
  var cl = src === 'ipeds' ? IPEDS_DATA : HB990_DATA;
  var c = null;
  for (var i = 0; i < cl.length; i++) { if (cl[i].id === cid) { c = cl[i]; break; } }
  if (!c) return;
  renderDetail(c);
}

function renderDetail(c) {
  var s = c.stats;
  var isIPEDS = (src === 'ipeds');

  var statCols, statHtml = '';
  if (isIPEDS) {
    statCols = 4;
    statHtml += mkStat(s.median_distress, 'Distress');
    statHtml += mkStat((s.median_enrollment || 0).toLocaleString(), 'Enroll');
    statHtml += mkStat(s.median_acres > 0 ? s.median_acres + ' ac' : '\u2014', 'Acres');
    statHtml += mkStat(s.median_closure_risk ? (s.median_closure_risk * 100).toFixed(1) + '%' : '\u2014', 'Closure Risk');
  } else {
    statCols = 4;
    statHtml += mkStat(s.median_distress, 'Distress');
    statHtml += mkStat('$' + (s.median_revenue_k || 0).toLocaleString() + 'K', 'Revenue');
    statHtml += mkStat(s.median_ppe_k ? '$' + s.median_ppe_k.toLocaleString() + 'K' : '\u2014', 'PPE');
    statHtml += mkStat(s.median_acres > 0 ? s.median_acres + ' ac' : '\u2014', 'Acres');
  }

  var flagDefs = isIPEDS
    ? [['Enrollment Decline', s.pct_enrollment_decline, '#ff6b35'],
       ['Operating Losses',   s.pct_operating_losses,  '#ff4757'],
       ['Neg. Net Worth',     s.pct_negative_net_worth,'#ff4757'],
       ['Land Potential',     s.pct_land_potential,    '#00b894']]
    : [['Operating Loss',     s.pct_operating_loss,    '#ff6b35'],
       ['Neg. Net Assets',    s.pct_negative_assets,   '#ff4757'],
       ['High Debt',          s.pct_high_debt,         '#ffa502'],
       ['Consec. Losses',     s.pct_consecutive_losses,'#ff6b35'],
       ['Revenue Decline',    s.pct_revenue_decline,   '#ff4757']];

  var flagHtml = '';
  for (var fi = 0; fi < flagDefs.length; fi++) {
    var fd = flagDefs[fi];
    var pv = fd[1] || 0;
    flagHtml += '<div class="frow">'
      + '<div class="flabel">' + fd[0] + '</div>'
      + '<div class="fbar-wrap"><div class="fbar" style="width:' + Math.min(pv,100) + '%;background:' + fd[2] + '"></div></div>'
      + '<div class="fpct">' + pv + '%</div></div>';
  }

  var html = '<div class="d-head" style="border-left:3px solid ' + c.color + '">'
    + '<div class="dh-icon">' + c.icon + '</div>'
    + '<div>'
    + '<div class="dh-name" style="color:' + c.color + '">' + c.name + '</div>'
    + '<div class="dh-count">' + c.count.toLocaleString() + ' institutions \u00b7 ' + s.top_region + ' most common</div>'
    + '<div class="dh-desc">' + c.desc + '</div>'
    + '</div></div>'
    + '<div class="hv-angle"><div class="hv-angle-label">HV Angle</div><div class="hv-angle-text">' + c.hv_angle + '</div></div>'
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
    ? [['distress','Distress'],['name','Institution'],['enrollment','Enroll'],
       ['revenue','Rev ($K)'],['acres','Acres'],['closure_risk','Closure Risk']]
    : [['distress','Distress'],['name','Institution'],['revenue','Rev ($K)'],
       ['acres','Acres'],['type','Type']];

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
    var tshort = (r.type || '').replace('Private Non-Profit | ','NP ').replace('Private For-Profit | ','FP ').replace('Public | ','Pub ');
    var nameCell = '<td class="td-name">' + esc(r.name) + '<small>' + esc(r.city) + ', ' + esc(r.state) + '</small></td>';
    var tds = '<td>' + dpill + '</td>' + nameCell;
    if (isIPEDS) {
      tds += '<td>' + (r.enrollment != null ? r.enrollment.toLocaleString() : '\u2014') + '</td>';
      tds += '<td>' + (r.revenue != null ? '$' + r.revenue.toLocaleString() + 'K' : '\u2014') + '</td>';
      tds += '<td>' + (r.acres != null ? r.acres : '\u2014') + '</td>';
      tds += '<td>' + (r.closure_risk != null ? (r.closure_risk * 100).toFixed(1) + '%' : '\u2014') + '</td>';
    } else {
      tds += '<td>' + (r.revenue != null ? '$' + r.revenue.toLocaleString() + 'K' : '\u2014') + '</td>';
      tds += '<td>' + (r.acres != null ? r.acres : '\u2014') + '</td>';
      tds += '<td style="font-size:9px;color:var(--text2)">' + esc(tshort) + '</td>';
    }
    tbody += '<tr>' + tds + '</tr>';
  }

  return '<div class="tbar">'
    + '<input class="tsearch" placeholder="Search\u2026" oninput="onSearch(this.value)" value="' + esc(searchQ) + '">'
    + '<span class="tcount">' + filtered.length.toLocaleString() + ' shown</span>'
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
        (r.city||'').toLowerCase().indexOf(lq) >= 0) out.push(r);
  }
  return out;
}

function sortRows(rows) {
  var col = sortCol, asc = sortAsc;
  return rows.slice().sort(function(a,b) {
    var av = a[col], bv = b[col];
    if (av == null && bv == null) return 0;
    if (av == null) return 1; if (bv == null) return -1;
    if (typeof av === 'string') av = av.toLowerCase();
    if (typeof bv === 'string') bv = bv.toLowerCase();
    var cmp = av < bv ? -1 : av > bv ? 1 : 0;
    return asc ? cmp : -cmp;
  });
}

function setSort(col) {
  if (sortCol === col) sortAsc = !sortAsc; else { sortCol = col; sortAsc = false; }
  var cl = src === 'ipeds' ? IPEDS_DATA : HB990_DATA;
  for (var i = 0; i < cl.length; i++) {
    if (cl[i].id === activeId) { renderDetail(cl[i]); break; }
  }
}

function onSearch(val) {
  searchQ = val;
  var cl = src === 'ipeds' ? IPEDS_DATA : HB990_DATA;
  for (var i = 0; i < cl.length; i++) {
    if (cl[i].id === activeId) { renderDetail(cl[i]); break; }
  }
}

function esc(s) {
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function doExport() {
  var cl = src === 'ipeds' ? IPEDS_DATA : HB990_DATA;
  var c = null;
  for (var i = 0; i < cl.length; i++) { if (cl[i].id === activeId) { c = cl[i]; break; } }
  if (!c) return;
  var rows = filterRows(c.rows);
  var cols = src === 'ipeds'
    ? ['name','state','city','type','distress','category','enrollment','revenue','acres','closure_risk','risk_tier','urbanization']
    : ['name','state','city','type','distress','category','revenue','acres'];
  var csvStr = cols.join(',') + '\n' + rows.map(function(r) {
    return cols.map(function(col) {
      return '"' + String(r[col] != null ? r[col] : '').replace(/"/g,'""') + '"';
    }).join(',');
  }).join('\n');
  var blob = new Blob([csvStr], {type:'text/csv'});
  var a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'hb_cluster_' + c.name.replace(/[^a-z0-9]+/g,'_').toLowerCase() + '.csv';
  a.click();
}

// ── Event wiring ──────────────────────────────────────────────────────────
document.getElementById('tabIPEDS').addEventListener('click', function() {
  src = 'ipeds'; activeId = null; searchQ = '';
  document.querySelectorAll('.s-tab').forEach(function(t) { t.classList.remove('active'); });
  this.classList.add('active');
  document.getElementById('segBar').style.display = 'flex';
  renderCards();
  document.getElementById('detail').innerHTML = '<div class="d-empty"><div class="d-empty-arrow">\u2190</div><div class="d-empty-title">Select a cluster</div><div class="d-empty-sub">Click any card to explore its institutions</div></div>';
});

document.getElementById('tab990').addEventListener('click', function() {
  src = '990'; activeId = null; searchQ = '';
  document.querySelectorAll('.s-tab').forEach(function(t) { t.classList.remove('active'); });
  this.classList.add('active');
  document.getElementById('segBar').style.display = 'none';
  renderCards();
  document.getElementById('detail').innerHTML = '<div class="d-empty"><div class="d-empty-arrow">\u2190</div><div class="d-empty-title">Select a cluster</div><div class="d-empty-sub">Click any card to explore its institutions</div></div>';
});

document.getElementById('segBar').addEventListener('click', function(e) {
  var btn = e.target.closest('[data-seg]');
  if (!btn) return;
  seg = btn.getAttribute('data-seg');
  activeId = null;
  document.querySelectorAll('.seg-btn').forEach(function(b) { b.classList.remove('active'); });
  btn.classList.add('active');
  renderCards();
  document.getElementById('detail').innerHTML = '<div class="d-empty"><div class="d-empty-arrow">\u2190</div><div class="d-empty-title">Select a cluster</div><div class="d-empty-sub">Click any card to explore its institutions</div></div>';
});

document.getElementById('cards').addEventListener('click', function(e) {
  var card = e.target.closest('[data-cid]');
  if (card) selectCluster(parseInt(card.getAttribute('data-cid'), 10));
});

document.getElementById('detail').addEventListener('click', function(e) {
  var th = e.target.closest('th[data-col]');
  if (th) setSort(th.getAttribute('data-col'));
});

document.getElementById('methToggle').addEventListener('click', function() {
  var body  = document.getElementById('methBody');
  var arrow = document.getElementById('methArrow');
  body.classList.toggle('open');
  arrow.classList.toggle('open');
});

renderCards();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description='Hummingbird Cluster Engine v2')
    parser.add_argument('--master',    default=DEFAULT_MASTER)
    parser.add_argument('--osm',       default=DEFAULT_OSM)
    parser.add_argument('--out-html',  default=DEFAULT_OUT_HTML)
    parser.add_argument('--out-csv',   default=DEFAULT_OUT_CSV)
    parser.add_argument('--k-public',  type=int, default=K_PUBLIC)
    parser.add_argument('--k-pnp',     type=int, default=K_PNP)
    parser.add_argument('--k-pfp',     type=int, default=K_PFP)
    parser.add_argument('--k-990',     type=int, default=K_HB990)
    args = parser.parse_args()

    try:
        import sklearn
    except ImportError:
        print("ERROR: scikit-learn not installed. Run: pip install scikit-learn")
        sys.exit(1)

    # Load master
    print(f"Loading {args.master}...")
    csv.field_size_limit(100 * 1024 * 1024)
    rows = []
    with open(args.master, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            rows.append(row)
    print(f"  {len(rows):,} rows loaded")

    # Load OSM
    osm_scraped = 0
    if os.path.exists(args.osm):
        print(f"Loading OSM from {args.osm}...")
        csv.field_size_limit(100 * 1024 * 1024)
        osm_lookup = {}
        with open(args.osm, newline='', encoding='utf-8', errors='replace') as f:
            for r in csv.DictReader(f):
                name = r.get('institution_name', '')
                raw_cats = r.get('osm_env_categories', '').strip()
                if name and raw_cats and raw_cats.upper() != 'ERROR':
                    # Parse pipe-separated category groups into individual category names
                    cats = []
                    for part in raw_cats.split('|'):
                        part = part.strip().lower()
                        for known in OSM_CATS:
                            if known in part:
                                cats.append(known)
                    osm_lookup[name] = list(set(cats))
                    osm_scraped += 1
        print(f"  {osm_scraped:,} institutions with OSM data")
        for row in rows:
            row['_osm_categories'] = osm_lookup.get(row.get('institution_name', ''), [])
    else:
        print(f"  OSM file not found at {args.osm} — skipping")
        for row in rows:
            row['_osm_categories'] = []

    osm_pct = round(osm_scraped / len(rows) * 100)

    import datetime
    run_date = datetime.datetime.now().strftime('%Y-%m-%d')

    all_ipeds_clusters = []
    cluster_id_counter = [0]

    def next_id():
        cid = cluster_id_counter[0]
        cluster_id_counter[0] += 1
        return cid

    # ── IPEDS by segment ──────────────────────────────────────────────────────
    for seg_name, type_filter, k, namer in [
        ('Public',          'public', args.k_public,  name_public_cluster),
        ('Private NP',      'pnp',    args.k_pnp,     name_pnp_cluster),
        ('Private FP',      'pfp',    args.k_pfp,     name_pfp_cluster),
    ]:
        print(f"\nIPEDS {seg_name} (k={k})...")
        matrix, feat_names, valid_idx = build_ipeds_segment(rows, type_filter)
        print(f"  {len(matrix):,} institutions x {len(feat_names)} features")
        if len(matrix) < k:
            print(f"  Skipping — fewer institutions than k")
            continue

        labels, _ = run_kmeans(matrix, k)

        for pos, row_idx in enumerate(valid_idx):
            rows[row_idx]['cluster_ipeds'] = f"{type_filter}_{labels[pos]}"

        groups = defaultdict(list)
        for pos, row_idx in enumerate(valid_idx):
            groups[labels[pos]].append(rows[row_idx])

        for cid_local in sorted(groups.keys()):
            crows = groups[cid_local]
            cid = next_id()
            info = namer(crows, cid)
            all_ipeds_clusters.append(info)
            print(f"  [{cid}] {info['icon']} {info['name']} — {len(crows):,}")

    all_ipeds_clusters.sort(key=lambda c: -c['stats']['median_distress'])

    # ── 990 ───────────────────────────────────────────────────────────────────
    print(f"\n990 (k={args.k_990})...")
    hb990_matrix, _, hb990_idx = extract_990_features(rows)
    print(f"  {len(hb990_matrix):,} institutions x {len(hb990_matrix[0])} features")
    hb990_labels, _ = run_kmeans(hb990_matrix, args.k_990)

    for pos, row_idx in enumerate(hb990_idx):
        rows[row_idx]['cluster_990'] = str(hb990_labels[pos])

    hb990_groups = defaultdict(list)
    for pos, row_idx in enumerate(hb990_idx):
        hb990_groups[hb990_labels[pos]].append(rows[row_idx])

    all_990_clusters = []
    for cid_local in sorted(hb990_groups.keys()):
        crows = hb990_groups[cid_local]
        cid = next_id()
        info = name_990_cluster(crows, cid)
        all_990_clusters.append(info)
        print(f"  [{cid}] {info['icon']} {info['name']} — {len(crows):,}")

    all_990_clusters.sort(key=lambda c: -c['stats']['median_distress'])

    # ── Write CSV ─────────────────────────────────────────────────────────────
    print(f"\nWriting CSV to {args.out_csv}...")
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
    print("  Done.")

    # ── Write HTML ────────────────────────────────────────────────────────────
    print(f"\nGenerating {args.out_html}...")
    meta = {
        'run_date': run_date,
        'osm_scraped': osm_scraped,
        'osm_pct': osm_pct,
    }
    html = build_html(all_ipeds_clusters, all_990_clusters, meta)
    with open(args.out_html, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  Done. ({len(html):,} chars)")

    print(f"\n\u2713 Complete.")
    print(f"  IPEDS: {len(all_ipeds_clusters)} clusters across {len([r for r in rows if r.get('data_source')=='IPEDS']):,} institutions")
    print(f"  990:   {len(all_990_clusters)} clusters across {len([r for r in rows if r.get('data_source')=='Hummingbird_990']):,} institutions")
    print(f"  HTML:  {args.out_html}")
    print(f"  CSV:   {args.out_csv}")

    return all_ipeds_clusters, all_990_clusters


if __name__ == '__main__':
    main()