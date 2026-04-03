"""
clusters.py v4 — Hummingbird Ventures Opportunity Cluster Engine
=================================================================
3-axis clustering using pre-computed scores:
  1. URGENCY    — how close is this institution to transacting?
  2. LAND       — how much developable physical asset is here?
  3. PR FIT     — partnership readiness (state policy + funding + urgency + asset value)

IPEDS: K=8 clusters, 3-axis k-means on (urgency, land, PR)
990:   K=5 clusters, urgency-focused (most have no land data)

Cluster labels are ACTION-ORIENTED:
  "Immediate Targets" / "Sweet Spot" / "Long-Term Watch" / etc.
"""

import os, sys, csv, json, math, argparse, warnings, datetime
from collections import Counter, defaultdict
warnings.filterwarnings('ignore')

DEFAULT_MASTER   = 'hv_master_data/data/Hummingbird_Master_Combined_v6.csv'
DEFAULT_OSM      = 'hv_master_data/acreage_scripts/osm_land_results.csv'
DEFAULT_OUT_HTML = 'clusters.html'
DEFAULT_OUT_CSV  = 'hv_master_data/data/Hummingbird_Master_Clustered.csv'

K_IPEDS = 8
K_HB990 = 5

OSM_CATS = ['water','winter','outdoor_recreation','protected_land',
            'natural_features','tourism_leisure','infrastructure']

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
# Partnership Readiness Score — inline tables (source: partnership_readiness.py)
# Used as the PR Fit axis in clustering, replacing raw OSM score
# ---------------------------------------------------------------------------
_BEA_OUTDOOR = {
    'Hawaii':6.1,'Alaska':5.3,'Montana':4.9,'Vermont':4.5,'Wyoming':4.3,
    'Maine':4.1,'Idaho':3.9,'New Mexico':3.7,'Oregon':3.5,'Colorado':3.4,
    'South Dakota':3.3,'North Dakota':3.2,'Utah':3.1,'West Virginia':3.0,
    'Arkansas':2.9,'New Hampshire':2.8,'Mississippi':2.8,'Kentucky':2.7,
    'Tennessee':2.7,'South Carolina':2.6,'Alabama':2.6,'Nevada':2.6,
    'Wisconsin':2.5,'Minnesota':2.5,'Washington':2.5,'Nebraska':2.5,
    'Kansas':2.4,'Iowa':2.4,'Missouri':2.4,'Indiana':2.4,
    'North Carolina':2.3,'Arizona':2.3,'Oklahoma':2.3,'Louisiana':2.3,
    'Georgia':2.2,'Virginia':2.2,'Michigan':2.2,'Ohio':2.1,
    'Pennsylvania':2.1,'Texas':2.1,'California':2.0,'Florida':2.0,
    'Illinois':1.9,'Maryland':1.9,'Massachusetts':1.8,'New Jersey':1.7,
    'Connecticut':1.6,'New York':1.6,'Delaware':1.6,'Rhode Island':1.5,
    'District of Columbia':1.0,
}
_WICHE_CLIFF = {
    'Hawaii':-33,'California':-29,'New York':-27,'West Virginia':-26,
    'Wyoming':-23,'New Mexico':-21,'Oregon':-19,'Pennsylvania':-17,
    'Mississippi':-16,'Rhode Island':-15,'Vermont':-15,'Alaska':-14,
    'Maine':-10,'New Hampshire':-10,'Colorado':-12,'Michigan':-11,
    'Ohio':-11,'Illinois':-10,'Connecticut':-9,'Massachusetts':-9,
    'Maryland':-8,'Missouri':-8,'Wisconsin':-8,'Indiana':-7,
    'Minnesota':-7,'Kansas':-7,'Nebraska':-6,'Iowa':-6,
    'Washington':-5,'Virginia':-5,'Nevada':-4,'Delaware':-4,
    'New Jersey':-1,'Arizona':2,'Florida':3,'Georgia':4,
    'North Carolina':5,'Tennessee':6,'South Carolina':7,'Texas':8,
    'Kentucky':-3,'Alabama':-2,'Louisiana':-3,'Arkansas':-2,
    'Oklahoma':1,'Utah':3,'Idaho':6,'Montana':4,
    'North Dakota':5,'South Dakota':4,'District of Columbia':-5,
}
_STATE_CLOSURES = {
    'Pennsylvania':12,'New York':10,'California':8,'Massachusetts':7,
    'Illinois':6,'Ohio':6,'Michigan':5,'Virginia':5,'Iowa':5,
    'Indiana':4,'Wisconsin':4,'Minnesota':4,'Missouri':4,'Connecticut':4,
    'New Jersey':3,'Texas':3,'Florida':3,'Georgia':3,'North Carolina':3,
    'Vermont':3,'Maine':2,'Oregon':2,'Washington':2,'Kansas':2,
    'Nebraska':2,'Montana':2,'Tennessee':2,'Alabama':2,'Kentucky':2,
    'Oklahoma':2,'Colorado':1,'Arizona':1,'New Hampshire':1,
    'Rhode Island':1,'West Virginia':1,'Maryland':1,'South Carolina':1,
    'Mississippi':1,'Arkansas':1,'Louisiana':1,'Nevada':1,'Idaho':1,
    'New Mexico':1,'Hawaii':1,
}
_STATE_FUNDING = {
    'Washington':3,'Colorado':3,'Oregon':3,'California':3,'Minnesota':3,
    'Maryland':3,'Massachusetts':3,'Connecticut':3,'New York':3,'Utah':3,
    'Idaho':3,'Montana':3,'North Dakota':3,'South Dakota':3,
    'Texas':2,'Florida':2,'Georgia':2,'North Carolina':2,'Virginia':2,
    'Tennessee':2,'Indiana':2,'Ohio':2,'Wisconsin':2,'Michigan':2,
    'Iowa':2,'Nebraska':2,'Kansas':2,'Missouri':2,'Oklahoma':2,
    'Arkansas':2,'South Carolina':2,'New Jersey':2,'Delaware':2,
    'Nevada':2,'Arizona':2,'Hawaii':2,'Alaska':2,'New Mexico':2,'Wyoming':2,
    'Illinois':1,'Pennsylvania':1,'New Hampshire':1,'Vermont':1,
    'Maine':1,'Rhode Island':1,'Kentucky':1,'West Virginia':1,
    'Mississippi':1,'Louisiana':1,'Alabama':1,'District of Columbia':1,
}

def compute_pr_score(r):
    """Read Partnership Readiness Score from master CSV (pre-computed by partnership_readiness.py v3).
    Falls back to 46 (median) if not present."""
    v = fv(r, 'partnership_readiness_score')
    return round(v) if v is not None else 46


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fv(row, col, default=None):
    v = row.get(col, '')
    if v in ('','nan','NaN','None',None): return default
    try:
        r = float(v)
        return default if (math.isnan(r) or math.isinf(r)) else r
    except: return default

def flag(row, col):
    return str(row.get(col,'')).strip().lower() in ('1','true','yes','1.0')

def safe_log(x):
    if x is None or x <= 0: return None
    return math.log1p(x)

def impute_median(values):
    valid = [v for v in values if v is not None]
    if not valid: return [0.0]*len(values)
    valid.sort()
    med = valid[len(valid)//2]
    return [v if v is not None else med for v in values]

def sanitize_for_json(obj):
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict): return {k: sanitize_for_json(v) for k,v in obj.items()}
    if isinstance(obj, list): return [sanitize_for_json(v) for v in obj]
    return obj

def med_of(rows, col):
    vals = [fv(r,col) for r in rows if fv(r,col) is not None]
    if not vals: return 0
    vals.sort(); return vals[len(vals)//2]

def pct_of(rows, col):
    return round(sum(1 for r in rows if flag(r,col)) / max(len(rows),1) * 100)

def top_of(rows, col):
    c = Counter(r.get(col,'') for r in rows if r.get(col,''))
    return c.most_common(1)[0][0] if c else ''

# ---------------------------------------------------------------------------
# Load scorer
# ---------------------------------------------------------------------------
def load_scorer():
    paths = [
        os.path.join(os.path.dirname(__file__), 'osm_scorer.py'),
        'hv_master_data/data/osm_scorer.py',
        'osm_scorer.py',
    ]
    for p in paths:
        if os.path.exists(p):
            import importlib.util
            spec = importlib.util.spec_from_file_location('osm_scorer', p)
            mod  = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
    return None

# ---------------------------------------------------------------------------
# Parse OSM features (mirrors master_standalone logic)
# ---------------------------------------------------------------------------
FEATURES = [
    ("natural","coastline","Coastline","water"),
    ("natural","water","Lake/Pond","water"),
    ("waterway","river","River","water"),
    ("natural","beach","Beach","water"),
    ("waterway","canal","Canal","water"),
    ("waterway","stream","Stream","water"),
    ("natural","wetland","Wetland","water"),
    ("leisure","marina","Marina","water"),
    ("natural","spring","Natural Spring","water"),
    ("natural","hot_spring","Hot Spring","water"),
    ("leisure","ski_resort","Ski Resort","winter"),
    ("landuse","winter_sports","Winter Sports Area","winter"),
    ("piste:type","downhill","Downhill Ski Run","winter"),
    ("piste:type","nordic","Nordic Ski Trail","winter"),
    ("aerialway","gondola","Gondola","winter"),
    ("aerialway","chair_lift","Chair Lift","winter"),
    ("aerialway","drag_lift","Drag Lift","winter"),
    ("aerialway","cable_car","Cable Car","winter"),
    ("sport","ice_skating","Ice Skating","winter"),
    ("sport","skiing","Skiing Facility","winter"),
    ("sport","ice_hockey","Ice Hockey Rink","winter"),
    ("route","hiking","Hiking Trail","outdoor_recreation"),
    ("route","mtb","Mountain Bike Trail","outdoor_recreation"),
    ("route","bicycle","Cycling Route","outdoor_recreation"),
    ("route","horse","Equestrian Trail","outdoor_recreation"),
    ("route","canoe","Canoe Route","outdoor_recreation"),
    ("highway","cycleway","Dedicated Cycleway","outdoor_recreation"),
    ("highway","bridleway","Bridleway","outdoor_recreation"),
    ("sport","climbing","Rock Climbing","outdoor_recreation"),
    ("sport","surfing","Surfing","outdoor_recreation"),
    ("sport","kayaking","Kayaking","outdoor_recreation"),
    ("sport","rowing","Rowing","outdoor_recreation"),
    ("sport","fishing","Fishing","outdoor_recreation"),
    ("sport","hunting","Hunting","outdoor_recreation"),
    ("sport","golf","Golf","outdoor_recreation"),
    ("leisure","golf_course","Golf Course","outdoor_recreation"),
    ("leisure","sports_centre","Sports Centre","outdoor_recreation"),
    ("leisure","horse_riding","Horse Riding","outdoor_recreation"),
    ("sport","swimming","Swimming Facility","outdoor_recreation"),
    ("tourism","camp_site","Campground","outdoor_recreation"),
    ("tourism","wilderness_hut","Wilderness Hut","outdoor_recreation"),
    ("boundary","national_park","National Park","protected_land"),
    ("boundary","protected_area","Protected Area","protected_land"),
    ("leisure","nature_reserve","Nature Reserve","protected_land"),
    ("landuse","forest","Forest","protected_land"),
    ("boundary","forest","National Forest","protected_land"),
    ("landuse","conservation","Conservation Land","protected_land"),
    ("natural","wood","Woodland","natural_features"),
    ("natural","peak","Mountain Peak","natural_features"),
    ("natural","volcano","Volcano","natural_features"),
    ("natural","cliff","Cliff","natural_features"),
    ("natural","cave_entrance","Cave","natural_features"),
    ("natural","glacier","Glacier","natural_features"),
    ("natural","scrub","Scrubland","natural_features"),
    ("natural","heath","Heathland","natural_features"),
    ("tourism","resort","Resort","tourism_leisure"),
    ("tourism","theme_park","Theme Park","tourism_leisure"),
    ("tourism","zoo","Zoo","tourism_leisure"),
    ("tourism","aquarium","Aquarium","tourism_leisure"),
    ("tourism","museum","Museum","tourism_leisure"),
    ("tourism","attraction","Tourist Attraction","tourism_leisure"),
    ("tourism","viewpoint","Scenic Viewpoint","tourism_leisure"),
    ("historic","monument","Historic Monument","tourism_leisure"),
    ("leisure","park","Park","tourism_leisure"),
    ("leisure","garden","Botanical Garden","tourism_leisure"),
    ("amenity","casino","Casino","tourism_leisure"),
    ("aeroway","aerodrome","Airport","infrastructure"),
    ("railway","station","Train Station","infrastructure"),
    ("amenity","ferry_terminal","Ferry Terminal","infrastructure"),
]
LABEL_TO_CAT = {f[2]: f[3] for f in FEATURES}

import re as _re
def parse_osm_feature_string(feat_str):
    by_category = {}
    if not feat_str or feat_str.strip() in ('','ERROR'): return by_category
    for part in feat_str.split(' | '):
        part = part.strip()
        if not part: continue
        label = ''; display = part
        if part.endswith(']') and '[' in part:
            bracket = part.rfind('[')
            label   = part[bracket+1:-1].strip()
            display = part[:bracket].strip()
        cat = LABEL_TO_CAT.get(label, 'other')
        if cat == 'other': continue
        if cat not in by_category: by_category[cat] = []
        unnamed_count = 0; named_str = display
        m = _re.search(r' \+ (\d+) unnamed .+$', display)
        if m:
            unnamed_count = int(m.group(1))
            named_str = display[:m.start()]
        names = [n.strip() for n in named_str.split(',') if n.strip()]
        trimmed = ', '.join(names[:5])
        if unnamed_count > 0: trimmed += f' +{unnamed_count} more'
        elif not names: trimmed = display
        by_category[cat].append({'label': label, 'display': trimmed})
    return by_category

# ---------------------------------------------------------------------------
# IPEDS feature extraction — 3-axis: urgency, land, PR
# ---------------------------------------------------------------------------
def extract_ipeds_features(rows, osm_scores):
    feat_names = ['urgency', 'land', 'pr']
    raw = defaultdict(list)
    valid_indices = []

    for i, r in enumerate(rows):
        if r.get('data_source') != 'IPEDS': continue
        vals = [
            inst_urgency(r, 'IPEDS'),
            inst_land(r),
            float(compute_pr_score(r)),
        ]
        for j, name in enumerate(feat_names):
            raw[name].append(vals[j])
        valid_indices.append(i)

    n = len(valid_indices)
    matrix = [[raw[feat][i] for feat in feat_names] for i in range(n)]
    return matrix, feat_names, valid_indices

# ---------------------------------------------------------------------------
# 990 feature extraction — urgency + physical assets + HV fit
# ---------------------------------------------------------------------------
def extract_990_features(rows, osm_scores):
    feat_names = [
        # ── URGENCY ───────────────────────────────────────────────────
        'distress_score',
        'solvency_score_990',
        'operating_score_990',
        'trend_score_990',
        'red_flag_score_990',
        'operating_margin',
        'equity_ratio',
        'flag_990_operating_loss',
        'flag_990_negative_net_assets',
        'flag_990_high_debt',
        'flag_990_consecutive_losses',
        'flag_990_revenue_decline_1yr',
        # ── PHYSICAL ASSETS ──────────────────────────────────────────
        'log_revenue',
        'log_assets',
        'log_ppe',
        'log_acreage',
        # ── PR FIT ───────────────────────────────────────────────────
        'pr_score',
        # ── TYPE one-hots ────────────────────────────────────────────
        'type_religious','type_housing','type_recreation',
        'type_educational','type_youth','type_environmental',
        'type_human_services','type_animal','type_agriculture','type_other',
        # ── REGION ───────────────────────────────────────────────────
        'region_ne','region_se','region_mw','region_sw','region_w',
    ]

    raw = defaultdict(list)
    valid_indices = []

    for i, r in enumerate(rows):
        if r.get('data_source') != 'Hummingbird_990': continue
        it   = r.get('institution_type','')
        reg  = STATE_REGION.get(r.get('state',''), 'Unknown')
        acres = fv(r,'acreage_primary') or fv(r,'scraper_acres') or fv(r,'osm_acres')
        osm_s = osm_scores.get(r.get('institution_name',''), {}).get('score')

        vals = [
            fv(r,'distress_score'),
            fv(r,'solvency_score_990'),
            fv(r,'operating_score_990'),
            fv(r,'trend_score_990'),
            fv(r,'red_flag_score_990'),
            fv(r,'operating_margin'),
            fv(r,'equity_ratio'),
            1.0 if flag(r,'flag_990_operating_loss') else 0.0,
            1.0 if flag(r,'flag_990_negative_net_assets') else 0.0,
            1.0 if flag(r,'flag_990_high_debt') else 0.0,
            1.0 if flag(r,'flag_990_consecutive_losses') else 0.0,
            1.0 if flag(r,'flag_990_revenue_decline_1yr') else 0.0,
            safe_log(fv(r,'revenue_2024')),
            safe_log(fv(r,'assets_2024')),
            safe_log(fv(r,'plant_property_equipment')),
            safe_log(acres),
            compute_pr_score(r),
            1.0 if it=='Religious Institution' else 0.0,
            1.0 if it=='Housing/Residential' else 0.0,
            1.0 if it=='Recreation/Camp' else 0.0,
            1.0 if it=='Educational Institution' else 0.0,
            1.0 if it=='Youth Development' else 0.0,
            1.0 if it=='Environmental/Conservation' else 0.0,
            1.0 if it=='Human Services' else 0.0,
            1.0 if it=='Animal/Wildlife' else 0.0,
            1.0 if it=='Agriculture/Farm' else 0.0,
            1.0 if it=='Other Nonprofit' else 0.0,
            1.0 if reg=='Northeast' else 0.0,
            1.0 if reg=='Southeast' else 0.0,
            1.0 if reg=='Midwest' else 0.0,
            1.0 if reg=='Southwest' else 0.0,
            1.0 if reg=='West' else 0.0,
        ]
        for j, name in enumerate(feat_names):
            raw[name].append(vals[j])
        valid_indices.append(i)

    continuous = ['distress_score','solvency_score_990','operating_score_990',
                  'trend_score_990','red_flag_score_990','operating_margin',
                  'equity_ratio','log_revenue','log_assets','log_ppe',
                  'log_acreage','pr_score']
    for col in continuous:
        raw[col] = impute_median(raw[col])

    n = len(valid_indices)
    matrix = [[raw[feat][i] for feat in feat_names] for i in range(n)]
    return matrix, feat_names, valid_indices

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
# Cluster characterization helpers
# ---------------------------------------------------------------------------
def inst_urgency(r, source='IPEDS'):
    """Per-institution urgency score (0-100) with good spread."""
    if source == 'IPEDS':
        ds  = fv(r,'distress_score') or 0
        cr  = (fv(r,'closure_risk_score') or 0) * 100
        es  = fv(r,'enrollment_score_ipeds') or 0
        return round(min(100, ds*0.45 + cr*0.35 + es*0.20), 1)
    else:
        ds  = fv(r,'distress_score') or 0
        sol = fv(r,'solvency_score_990') or 0
        op  = fv(r,'operating_score_990') or 0
        return round(min(100, ds*0.4 + sol*0.35 + op*0.25), 1)

def inst_land(r):
    """Per-institution land score (0-100)."""
    acres = fv(r,'acreage_primary') or fv(r,'scraper_acres') or fv(r,'osm_acres') or 0
    rural = 1.0 if flag(r,'flag_rural_suburban') else 0.0
    land_p = 1.0 if flag(r,'flag_land_potential') else 0.0
    hi_land = 1.0 if flag(r,'flag_high_land_potential') else 0.0
    log_a = math.log1p(acres) / math.log1p(500) * 100 if acres > 0 else 0
    return round(min(100, log_a*0.55 + rural*25 + land_p*12 + hi_land*8), 1)

def axis_scores(rows, osm_scores, source='IPEDS'):
    """Compute urgency / land / fit axis scores for a cluster (medians)."""
    urgency_vals = [inst_urgency(r, source) for r in rows]
    land_vals    = [inst_land(r) for r in rows]
    urgency_vals.sort(); land_vals.sort()
    urgency = round(urgency_vals[len(urgency_vals)//2], 1) if urgency_vals else 0
    land    = round(land_vals[len(land_vals)//2], 1) if land_vals else 0

    pr_vals = [compute_pr_score(r) for r in rows]
    fit = round(sum(pr_vals)/len(pr_vals), 1) if pr_vals else None

    return urgency, land, fit

# ---------------------------------------------------------------------------
# IPEDS cluster naming — action-oriented, based on 3-axis position
# ---------------------------------------------------------------------------
def name_ipeds_cluster(rows, cid, osm_scores):
    urgency, land, fit = axis_scores(rows, osm_scores, 'IPEDS')

    avg_acres = sum((fv(r,'acreage_primary') or fv(r,'scraper_acres') or fv(r,'osm_acres') or 0) for r in rows) / max(len(rows),1)
    pct_rural = pct_of(rows, 'flag_rural_suburban')
    pct_land  = pct_of(rows, 'flag_land_potential')
    top_urb   = (top_of(rows, 'urbanization') or '').split(':')[0].strip()

    # ── Action-oriented naming based on axis positions ──────────────────
    u_hi = urgency > 30
    u_med = urgency > 15
    l_hi = land > 50
    l_med = land > 20
    p_hi = fit > 52 if fit else False
    p_med = fit > 42 if fit else True

    if u_hi and l_med:
        # Sub-differentiate: higher urgency vs more land
        if urgency > 45:
            name = 'Immediate Targets — Acute Distress'
            desc = (f'The most urgently distressed institutions in the dataset that also hold developable land. '
                    f'Avg {avg_acres:.0f} acres, {pct_rural}% rural/suburban. Financial crisis is active — '
                    f'these institutions are at or near the point of closure or forced asset disposition.')
            hv_angle = 'Highest priority. Active financial crisis + tangible land = near-term acquisition opportunity. Move fast.'
        else:
            name = 'Immediate Targets — Distressed w/ Land'
            desc = (f'Institutions with significant financial distress AND developable land assets. '
                    f'Avg {avg_acres:.0f} acres, {pct_rural}% rural/suburban. Distress is real and building — '
                    f'these are high-probability targets within 1-2 years.')
            hv_angle = 'Act now. Financial pressure is mounting and land assets are meaningful. Sort by acreage to prioritize outreach.'
        icon, color = '🔴', '#ff4757'
        priority = 1

    elif u_hi and not l_med:
        name = 'Distressed — Partnership Only'
        icon, color = '🟠', '#ff6b35'
        desc = (f'High financial distress but minimal land holdings. Predominantly urban for-profit '
                f'and small private institutions. The urgency is real but the land thesis doesn\'t apply — '
                f'value is in relationships, IP, or facility conversion.')
        hv_angle = 'Partnership plays only. No meaningful land — focus on program licensing, facility leases, or referral agreements.'
        priority = 2

    elif u_med and l_hi and p_hi:
        name = 'Sweet Spot — Land-Rich, High PR Fit'
        icon, color = '🎯', '#00b894'
        desc = (f'The best combination in the dataset: meaningful land ({avg_acres:.0f} avg acres), '
                f'moderate financial pressure creating willingness to partner, and strong state policy alignment. '
                f'{pct_rural}% rural/suburban. These institutions are primed for the outdoor/wellness thesis.')
        hv_angle = 'PRIMARY PIPELINE. Build relationships now — these become acquisition targets as demographic pressure intensifies over 2-5 years.'
        priority = 1

    elif u_med and l_hi:
        name = 'Pipeline — Land-Rich, Moderate Pressure'
        icon, color = '🟡', '#ffa502'
        desc = (f'Large land holdings ({avg_acres:.0f} avg acres) with moderate but real financial pressure. '
                f'Not yet in crisis, but enrollment trends and operating losses are building. '
                f'Predominantly {top_urb.lower()} locations.')
        hv_angle = 'Build relationships early. These are 2-4 year targets — the land is excellent and pressure is mounting.'
        priority = 3

    elif u_med and not l_hi:
        name = 'Watch List — Moderate Pressure, Low Land'
        icon, color = '⚠️', '#a29bfe'
        desc = (f'Moderate financial distress but limited land assets. Mix of suburban and urban '
                f'institutions feeling demographic and financial headwinds. Worth monitoring for '
                f'campus consolidation opportunities.')
        hv_angle = 'Secondary pipeline. Monitor for campus closures or consolidation announcements that create land opportunities.'
        priority = 4

    elif not u_med and l_hi and p_hi:
        name = 'Long-Term — Land-Rich, Strong PR, Stable'
        icon, color = '🌲', '#10b981'
        desc = (f'Financially healthy institutions with the best land profiles in the dataset '
                f'({avg_acres:.0f} avg acres) and strong policy fit. {pct_rural}% rural/suburban. '
                f'No urgency today, but these are the premier long-term targets as demographics shift.')
        hv_angle = 'Relationship-building phase. These institutions will face pressure in 5-10 years. Be the known partner when they do.'
        priority = 5

    elif not u_med and l_hi:
        # Sub-differentiate: stronger PR fit vs weaker
        if fit and fit > 48:
            name = 'Stable — Land-Rich, Good PR Fit'
            icon, color = '🏫', '#74b9ff'
            desc = (f'Healthy institutions with meaningful land ({avg_acres:.0f} avg acres) and decent '
                    f'state policy alignment. {pct_rural}% rural/suburban. No urgency today but the '
                    f'land + policy combination makes these strong long-term targets.')
            hv_angle = 'Long-term relationship building. Good land, reasonable policy fit — these become targets as demographics shift.'
        else:
            name = 'Stable — Land-Rich, Urban Leaning'
            icon, color = '🏫', '#636e72'
            desc = (f'Healthy institutions with meaningful land ({avg_acres:.0f} avg acres) but weaker '
                    f'state policy alignment or more urban/suburban locations. The land is real but the '
                    f'outdoor thesis fit and partnership path is longer.')
            hv_angle = 'Long-term watch. Strong land assets but location or policy environment reduces near-term fit.'
        priority = 6

    elif not u_med and not l_hi and not p_med:
        name = 'Low Fit — Urban, No Land, Low PR'
        icon, color = '⚪', '#636e72'
        desc = (f'Urban institutions with minimal land, low distress, and weak policy fit. '
                f'Predominantly city-based for-profit programs. Low alignment with the outdoor/wellness thesis.')
        hv_angle = 'Not actionable for land thesis. Deprioritize unless a specific campus closure creates an opportunity.'
        priority = 7

    else:
        name = 'Mixed Profile — Monitor'
        icon, color = '📋', '#8b8fa3'
        desc = (f'Institutions with mixed signals across urgency, land, and partnership readiness. '
                f'No single axis stands out. Worth periodic review as conditions evolve.')
        hv_angle = 'Periodic review. Filter by state or type to find individual opportunities within this group.'
        priority = 5

    return _make_cluster(cid, name, icon, color, desc, hv_angle, priority,
                         rows, 'IPEDS', osm_scores, urgency, land, fit)

# ---------------------------------------------------------------------------
# 990 cluster naming
# ---------------------------------------------------------------------------
def name_990_cluster(rows, cid, osm_scores):
    urgency, land, fit = axis_scores(rows, osm_scores, '990')

    ds   = med_of(rows,'distress_score')
    rev  = med_of(rows,'revenue_2024') or 0
    ppe  = med_of(rows,'plant_property_equipment') or 0
    acres = med_of(rows,'acreage_primary') or med_of(rows,'scraper_acres') or med_of(rows,'osm_acres') or 0
    eq   = med_of(rows,'equity_ratio') or 0

    pct_op_loss  = pct_of(rows,'flag_990_operating_loss')
    pct_neg_ast  = pct_of(rows,'flag_990_negative_net_assets')
    pct_hi_debt  = pct_of(rows,'flag_990_high_debt')
    pct_consec   = pct_of(rows,'flag_990_consecutive_losses')
    pct_rev_dec  = pct_of(rows,'flag_990_revenue_decline_1yr')

    top_type   = top_of(rows,'institution_type')
    top_region = Counter(STATE_REGION.get(r.get('state',''),'Unknown') for r in rows).most_common(1)[0][0]
    types      = Counter(r.get('institution_type','') for r in rows)

    is_dormant = rev < 75000 and pct_rev_dec > 70 and pct_neg_ast < 10

    if is_dormant:
        name, icon, color = 'Dormant / Near-Closed', '💤', '#636e72'
        desc = ('Organizations with near-zero revenue and steep decline — effectively dormant '
                'or winding down. Balance sheets may appear clean but operational activity '
                'has essentially ceased.')
        hv_angle = 'Low operational value. Title search and asset recovery only — sort by acreage for land plays.'
        priority = 5

    elif top_type == 'Housing/Residential' and pct_neg_ast > 75 and ppe > 500000:
        name, icon, color = 'Insolvent Housing Nonprofits', '🏘', '#ff4757'
        desc = ('Housing and residential nonprofits that are structurally insolvent but '
                'operationally active. Significant physical plant — buildings are real. '
                'Real estate asset value typically exceeds remaining organizational value.')
        hv_angle = 'Real estate recovery play. PPE median meaningful — buildings viable for hospitality or residential conversion.'
        priority = 2

    elif ds > 70 and pct_neg_ast > 75 and pct_hi_debt > 85:
        name, icon, color = 'Acutely Distressed — High Urgency', '🔴', '#ff4757'
        desc = ('The most severely distressed cluster across all metrics — highest distress '
                'scores, deeply negative equity, near-universal losses. Religious, recreation '
                'and youth organizations dominate. Many are at end-of-life.')
        hv_angle = 'Fastest transaction path. Sort by acreage and PPE — the ones with land are direct acquisition targets.'
        priority = 1

    elif pct_op_loss < 5 and pct_consec < 5 and pct_hi_debt > 50:
        name, icon, color = 'Debt-Burdened but Operational', '⚡', '#ffa502'
        desc = ('Currently cash-flow positive but carrying heavy debt and in many cases '
                'negative net assets. Zombie organizations — functioning today but '
                'structurally fragile. One bad year triggers crisis.')
        hv_angle = 'Build relationships early. Low urgency today but high conversion probability in 2-4 years.'
        priority = 4

    elif types.get('Recreation/Camp',0) + types.get('Environmental/Conservation',0) > len(rows)*0.25:
        name, icon, color = 'Outdoor & Conservation Orgs', '🌲', '#00b894'
        desc = ('Recreation, camp, and conservation nonprofits with meaningful land and facility '
                'assets. Direct programming fit with HV outdoor thesis. Financial distress '
                'is real but asset base is strong.')
        hv_angle = 'PRIMARY 990 TARGET. Recreation/camp facilities often have exactly the land and infrastructure HV needs.'
        priority = 1

    else:
        name, icon, color = 'Insolvent Educational & Faith', '🏫', '#ff6b35'
        desc = ('Educational institutions and faith organizations that are operationally active '
                'but deeply insolvent. Often running camps, retreat centers, or community '
                'education programs with real physical facilities.')
        hv_angle = 'Facility acquisition play. Screen for retreat centers, camps, and conference facilities by PPE and acreage.'
        priority = 3

    return _make_cluster(cid, name, icon, color, desc, hv_angle, priority,
                         rows, '990', osm_scores, urgency, land, fit)

# ---------------------------------------------------------------------------
# Cluster object builder
# ---------------------------------------------------------------------------
def _make_cluster(cid, name, icon, color, desc, hv_angle, priority,
                  rows, source, osm_scores, urgency, land, fit):
    ds   = med_of(rows,'distress_score')
    cr   = med_of(rows,'closure_risk_score') if source=='IPEDS' else None
    enr  = med_of(rows,'enrollment_2024') if source=='IPEDS' else None
    rev  = med_of(rows,'revenue_2024') or 0
    acres = med_of(rows,'acreage_primary') or med_of(rows,'scraper_acres') or med_of(rows,'osm_acres') or 0
    ppe  = med_of(rows,'plant_property_equipment') if source=='990' else None
    top_region = Counter(STATE_REGION.get(r.get('state',''),'Unknown') for r in rows).most_common(1)[0][0]

    stats = {
        'median_distress': round(ds,1) if ds else 0,
        'median_acres':    round(acres,1),
        'top_region':      top_region,
        'urgency_score':   urgency,
        'land_score':      land,
        'fit_score':       fit,
        'osm_scraped_pct': round(sum(1 for r in rows if osm_scores.get(r.get('institution_name',''),{}).get('score') is not None) / max(len(rows),1) * 100),
    }

    if source == 'IPEDS':
        stats.update({
            'median_enrollment':  int(round(enr)) if enr else 0,
            'median_revenue_m':   round(rev/1e6,1) if rev else 0,
            'median_closure_risk':round(cr,3) if cr else 0,
            'pct_enrollment_decline': pct_of(rows,'flag_enrollment_decline'),
            'pct_operating_losses':   pct_of(rows,'flag_operating_losses'),
            'pct_negative_net_worth': pct_of(rows,'flag_negative_net_worth'),
            'pct_land_potential':     pct_of(rows,'flag_land_potential'),
            'pct_rural':              pct_of(rows,'flag_rural_suburban'),
        })
    else:
        stats.update({
            'median_revenue_k':      int(round(rev/1000)) if rev else 0,
            'median_ppe_k':          int(round(ppe/1000)) if ppe else 0,
            'pct_operating_loss':    pct_of(rows,'flag_990_operating_loss'),
            'pct_negative_assets':   pct_of(rows,'flag_990_negative_net_assets'),
            'pct_high_debt':         pct_of(rows,'flag_990_high_debt'),
            'pct_consecutive_losses':pct_of(rows,'flag_990_consecutive_losses'),
            'pct_revenue_decline':   pct_of(rows,'flag_990_revenue_decline_1yr'),
        })

    # Build 3D scatter points (per-institution axis coords, max 300 per cluster)
    # Sorted by distress descending so the most interesting ones are always included
    sorted_rows = sorted(rows, key=lambda r: -(fv(r,'distress_score') or 0))[:300]
    points = []
    for r in sorted_rows:
        u = inst_urgency(r, source)
        l = inst_land(r)
        f = compute_pr_score(r)
        points.append({
            'n': r.get('institution_name',''),        # name
            'st': r.get('state',''),                  # state
            'u': u,                                   # urgency
            'l': l,                                   # land
            'f': round(f, 1) if f is not None else None,  # fit
            'd': round(fv(r,'distress_score') or 0, 1),   # distress
            'cr': round(fv(r,'closure_risk_score') or 0, 3), # closure risk
            'a': round(fv(r,'acreage_primary') or fv(r,'scraper_acres') or 0, 0), # acres
        })

    return {
        'id': cid, 'name': name, 'icon': icon, 'color': color,
        'desc': desc, 'hv_angle': hv_angle, 'priority': priority,
        'count': len(rows), 'source': source, 'stats': stats,
        'rows': build_preview_rows(rows, source, osm_scores),
        'points': points,
    }

# ---------------------------------------------------------------------------
# Preview rows
# ---------------------------------------------------------------------------
def build_preview_rows(cluster_rows, source, osm_scores):
    sorted_rows = sorted(cluster_rows, key=lambda r: -(fv(r,'distress_score') or 0))[:200]
    out = []
    for r in sorted_rows:
        acres = fv(r,'acreage_primary') or fv(r,'scraper_acres') or fv(r,'osm_acres')
        rev   = fv(r,'revenue_2024')
        enr   = fv(r,'enrollment_2024')
        cr    = fv(r,'closure_risk_score')
        osm_s = osm_scores.get(r.get('institution_name',''),{}).get('score')
        out.append({
            'name':        r.get('institution_name',''),
            'state':       r.get('state',''),
            'city':        r.get('city',''),
            'type':        r.get('institution_type',''),
            'distress':    round(fv(r,'distress_score') or 0,1),
            'category':    r.get('distress_category',''),
            'revenue':     round(rev/1000,1) if rev else None,
            'enrollment':  int(round(enr)) if enr else None,
            'acres':       round(acres,1) if acres else None,
            'closure_risk':round(cr,3) if cr else None,
            'env_score':   round(osm_s,1) if osm_s is not None else None,
            'risk_tier':   r.get('risk_tier',''),
            'urbanization':r.get('urbanization',''),
        })
    return out

# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------
def build_html(ipeds_clusters, hb990_clusters, meta):
    ipeds_js = json.dumps(sanitize_for_json([{
        'id':c['id'],'name':c['name'],'icon':c['icon'],'color':c['color'],
        'desc':c['desc'],'hv_angle':c['hv_angle'],'priority':c['priority'],
        'count':c['count'],'source':c['source'],'stats':c['stats'],'rows':c['rows'],'points':c['points'],
    } for c in ipeds_clusters]), ensure_ascii=False)

    hb990_js = json.dumps(sanitize_for_json([{
        'id':c['id'],'name':c['name'],'icon':c['icon'],'color':c['color'],
        'desc':c['desc'],'hv_angle':c['hv_angle'],'priority':c['priority'],
        'count':c['count'],'source':c['source'],'stats':c['stats'],'rows':c['rows'],'points':c['points'],
    } for c in hb990_clusters]), ensure_ascii=False)

    total   = sum(c['count'] for c in ipeds_clusters) + sum(c['count'] for c in hb990_clusters)
    n_ipeds = sum(c['count'] for c in ipeds_clusters)
    n_990   = sum(c['count'] for c in hb990_clusters)
    run_date     = meta.get('run_date','')
    osm_scraped  = meta.get('osm_scraped',0)
    osm_pct      = meta.get('osm_pct',0)

    # Build HTML — stored in parts to avoid Python string escape issues
    parts = []
    parts.append("""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Hummingbird \u2014 Opportunity Clusters</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{--bg:#0f1117;--surface:#181a20;--surface2:#1e2028;--border:#2a2d37;
--text:#e2e4e9;--text2:#8b8fa3;--accent:#6c5ce7;--accent2:#a29bfe;
--hb990:#00b894;--font:'DM Sans',-apple-system,sans-serif;--mono:'JetBrains Mono',monospace;}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
html,body{background:var(--bg);color:var(--text);font-family:var(--font);font-size:14px;min-height:100vh;}
nav{position:sticky;top:0;z-index:100;display:flex;align-items:center;gap:14px;padding:0 20px;height:52px;background:var(--surface);border-bottom:1px solid var(--border);}
.nav-logo{font-size:16px;font-weight:700;letter-spacing:-.5px;background:linear-gradient(135deg,var(--accent2),#74b9ff);-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;}
.nav-sep{width:1px;height:18px;background:var(--border);}
.nav-sub{font-size:11px;color:var(--text2);font-weight:500;}
.nav-spacer{flex:1;}
.nav-back{display:flex;align-items:center;gap:6px;padding:5px 12px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text2);font-size:11px;font-weight:600;text-decoration:none;transition:all .15s;}
.nav-back:hover{border-color:var(--accent);color:var(--text);}
.page{max-width:1280px;margin:0 auto;padding:24px 20px 80px;}
.methodology{background:var(--surface);border:1px solid var(--border);border-radius:10px;margin-bottom:28px;overflow:hidden;}
.meth-toggle{display:flex;align-items:center;gap:10px;padding:14px 18px;cursor:pointer;user-select:none;}
.meth-toggle:hover{background:var(--surface2);}
.meth-label{font-family:var(--mono);font-size:10px;color:var(--accent2);text-transform:uppercase;letter-spacing:1.5px;flex:1;}
.meth-sub{font-size:11px;color:var(--text2);}
.meth-arrow{font-size:11px;color:var(--text2);transition:transform .2s;}
.meth-arrow.open{transform:rotate(180deg);}
.meth-body{display:none;padding:0 18px 18px;}
.meth-body.open{display:block;}
.meth-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;}
.meth-card{background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:14px;border-left:3px solid var(--accent);}
.meth-card.c990{border-left-color:var(--hb990);}
.meth-ct{font-family:var(--mono);font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--accent2);margin-bottom:8px;}
.meth-card.c990 .meth-ct{color:var(--hb990);}
.meth-card p{font-size:11px;color:var(--text2);line-height:1.7;margin-bottom:7px;}
.meth-card p b{color:var(--text);}
.meth-feats{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px;}
.meth-feat{padding:2px 7px;border-radius:3px;font-family:var(--mono);font-size:9px;background:rgba(255,255,255,.04);border:1px solid var(--border);color:var(--text2);}
.meth-axes{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:14px;}
.meth-axis{padding:12px;border-radius:8px;background:var(--surface2);border:1px solid var(--border);}
.meth-axis-title{font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;}
.meth-axis-title.urgency{color:#ff6b35;}
.meth-axis-title.land{color:#00b894;}
.meth-axis-title.fit{color:var(--accent2);}
.meth-axis p{font-size:10px;color:var(--text2);line-height:1.6;}
.meth-caveat{background:rgba(255,165,2,.06);border:1px solid rgba(255,165,2,.2);border-radius:6px;padding:10px 14px;font-size:11px;color:#c49a00;line-height:1.6;}
.meth-caveat b{color:#ffa502;}
.meth-meta{display:flex;gap:16px;margin-top:12px;padding-top:12px;border-top:1px solid var(--border);flex-wrap:wrap;}
.meth-mi{font-family:var(--mono);font-size:9px;color:var(--text2);}
.meth-mi b{color:var(--text);}
.page-eyebrow{font-family:var(--mono);font-size:10px;color:var(--accent2);letter-spacing:2px;text-transform:uppercase;margin-bottom:8px;}
.page-title{font-size:26px;font-weight:700;letter-spacing:-.5px;margin-bottom:6px;}
.page-sub{font-size:12px;color:var(--text2);line-height:1.6;max-width:680px;margin-bottom:20px;}
.source-tabs{display:flex;gap:0;margin-bottom:20px;border-bottom:1px solid var(--border);}
.s-tab{padding:8px 18px;border:1px solid transparent;border-bottom:none;border-radius:6px 6px 0 0;background:transparent;color:var(--text2);font-family:var(--font);font-size:12px;font-weight:600;cursor:pointer;transition:all .15s;margin-bottom:-1px;}
.s-tab.active{background:var(--surface);border-color:var(--border);border-bottom-color:var(--surface);color:var(--text);}
.s-tab:hover:not(.active){color:var(--text);}
.tc{display:inline-block;font-family:var(--mono);font-size:9px;background:rgba(255,255,255,.06);border-radius:8px;padding:1px 6px;margin-left:5px;}
.s-tab.active .tc{background:rgba(108,92,231,.15);color:var(--accent2);}
.layout{display:grid;grid-template-columns:290px 1fr;gap:14px;align-items:start;}
.cards{display:flex;flex-direction:column;gap:6px;}
.card{padding:13px 13px 13px 16px;border-radius:8px;background:var(--surface);border:1px solid var(--border);cursor:pointer;transition:background .15s,border-color .15s;position:relative;overflow:hidden;user-select:none;}
.card-bar{position:absolute;left:0;top:0;bottom:0;width:3px;}
.card:hover{background:var(--surface2);}
.card.active{background:var(--surface2);}
.card-hdr{display:flex;align-items:center;gap:8px;margin-bottom:5px;}
.card-icon{font-size:15px;flex-shrink:0;}
.card-name{font-size:12px;font-weight:600;flex:1;line-height:1.2;}
.card-n{font-family:var(--mono);font-size:9px;color:var(--text2);}
.card-desc{font-size:10px;color:var(--text2);line-height:1.5;margin-bottom:7px;}
.chips{display:flex;flex-wrap:wrap;gap:3px;}
.chip{padding:2px 6px;border-radius:3px;background:var(--surface2);border:1px solid var(--border);font-family:var(--mono);font-size:9px;color:var(--text2);}
.chip b{color:var(--text);font-weight:500;}
.axis-bars{display:flex;gap:6px;margin-top:6px;}
.axis-bar-wrap{flex:1;}
.axis-bar-label{font-family:var(--mono);font-size:8px;color:var(--text2);margin-bottom:2px;}
.axis-bar-track{height:3px;background:var(--border);border-radius:2px;overflow:hidden;}
.axis-bar-fill{height:100%;border-radius:2px;}
.detail{background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden;position:sticky;top:68px;}
.d-empty{padding:60px 20px;text-align:center;color:var(--text2);}
.d-empty-arrow{font-size:24px;margin-bottom:10px;}
.d-empty-title{font-size:13px;font-weight:600;margin-bottom:4px;}
.d-empty-sub{font-size:11px;}
.d-head{padding:16px 18px;border-bottom:1px solid var(--border);background:var(--surface2);display:flex;align-items:flex-start;gap:12px;}
.dh-icon{font-size:22px;flex-shrink:0;margin-top:1px;}
.dh-name{font-size:17px;font-weight:700;letter-spacing:-.3px;line-height:1.2;}
.dh-count{font-family:var(--mono);font-size:10px;color:var(--text2);margin-top:3px;}
.dh-desc{font-size:11px;color:var(--text2);line-height:1.5;margin-top:5px;}
.hv-angle{margin:0;padding:10px 16px;border-bottom:1px solid var(--border);background:rgba(0,184,148,.06);border-left:3px solid var(--hb990);display:flex;align-items:flex-start;gap:8px;}
.hv-angle-label{font-family:var(--mono);font-size:9px;color:var(--hb990);text-transform:uppercase;letter-spacing:1px;white-space:nowrap;margin-top:1px;}
.hv-angle-text{font-size:11px;color:#a8e6da;line-height:1.5;}
.axis-strip{display:grid;grid-template-columns:repeat(3,1fr);border-bottom:1px solid var(--border);}
.axis-cell{padding:12px 14px;border-right:1px solid var(--border);text-align:center;}
.axis-cell:last-child{border-right:none;}
.axis-val{font-family:var(--mono);font-size:20px;font-weight:600;display:block;}
.axis-lbl{font-size:9px;color:var(--text2);text-transform:uppercase;letter-spacing:.6px;margin-top:2px;}
.axis-sub{font-size:9px;color:var(--text2);margin-top:1px;}
.stat-strip{display:grid;border-bottom:1px solid var(--border);}
.stat-cell{padding:11px 14px;border-right:1px solid var(--border);text-align:center;}
.stat-cell:last-child{border-right:none;}
.stat-v{font-family:var(--mono);font-size:15px;font-weight:500;display:block;}
.stat-l{font-size:9px;color:var(--text2);text-transform:uppercase;letter-spacing:.6px;margin-top:2px;}
.flags{padding:12px 16px;border-bottom:1px solid var(--border);}
.flags-title{font-family:var(--mono);font-size:9px;color:var(--text2);text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;}
.frow{display:flex;align-items:center;gap:8px;margin-bottom:5px;}
.flabel{font-size:10px;color:var(--text2);width:155px;flex-shrink:0;}
.fbar-wrap{flex:1;height:4px;background:var(--border);border-radius:2px;overflow:hidden;}
.fbar{height:100%;border-radius:2px;}
.fpct{font-family:var(--mono);font-size:9px;color:var(--text2);width:28px;text-align:right;}
.tbar{display:flex;align-items:center;gap:8px;padding:9px 13px;border-bottom:1px solid var(--border);background:var(--surface2);}
.tsearch{flex:1;padding:5px 8px;background:var(--bg);border:1px solid var(--border);border-radius:5px;color:var(--text);font-family:var(--font);font-size:11px;outline:none;}
.tsearch:focus{border-color:var(--accent);}
.tcount{font-family:var(--mono);font-size:9px;color:var(--text2);white-space:nowrap;}
.export-btn{padding:4px 10px;border-radius:5px;background:rgba(108,92,231,.1);border:1px solid rgba(108,92,231,.3);color:var(--accent2);font-family:var(--font);font-size:10px;font-weight:600;cursor:pointer;white-space:nowrap;}
.export-btn:hover{background:rgba(108,92,231,.2);}
.twrap{max-height:380px;overflow-y:auto;}
.twrap::-webkit-scrollbar{width:3px;}
.twrap::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px;}
table{width:100%;border-collapse:collapse;}
th{padding:6px 10px;text-align:left;font-family:var(--mono);font-size:9px;color:var(--text2);text-transform:uppercase;letter-spacing:.6px;border-bottom:1px solid var(--border);background:var(--surface2);white-space:nowrap;cursor:pointer;user-select:none;}
th:hover{color:var(--text);}
td{padding:6px 10px;font-size:11px;border-bottom:1px solid rgba(42,45,55,.5);vertical-align:middle;}
tr:hover td{background:rgba(255,255,255,.015);}
.td-name{font-weight:600;max-width:180px;}
.td-name small{display:block;font-size:9px;color:var(--text2);font-weight:400;margin-top:1px;}
.dp{display:inline-block;padding:2px 5px;border-radius:3px;font-family:var(--mono);font-size:9px;font-weight:600;}
.d-critical{background:rgba(255,71,87,.15);color:#ff4757;}
.d-high{background:rgba(255,107,53,.15);color:#ff6b35;}
.d-moderate{background:rgba(255,165,2,.15);color:#ffa502;}
.d-low{background:rgba(59,130,246,.15);color:#74b9ff;}
.d-healthy{background:rgba(16,185,129,.15);color:#10b981;}
.env-pill{display:inline-block;padding:2px 6px;border-radius:3px;font-family:var(--mono);font-size:9px;font-weight:600;}
.filter-bar{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:14px;align-items:center;}
.filter-label{font-family:var(--mono);font-size:9px;color:var(--text2);text-transform:uppercase;letter-spacing:.8px;margin-right:2px;}
.filter-select{padding:4px 8px;border-radius:5px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-family:var(--font);font-size:11px;outline:none;cursor:pointer;}
.filter-select:focus{border-color:var(--accent);}
.filter-btn{padding:4px 10px;border-radius:5px;background:var(--surface2);border:1px solid var(--border);color:var(--text2);font-family:var(--font);font-size:10px;font-weight:600;cursor:pointer;transition:all .15s;}
.filter-btn.active{background:rgba(108,92,231,.15);border-color:rgba(108,92,231,.4);color:var(--accent2);}
.filter-btn:hover:not(.active){color:var(--text);border-color:var(--text2);}
.filter-count{font-family:var(--mono);font-size:10px;color:var(--text2);margin-left:4px;}
@media(max-width:860px){.layout{grid-template-columns:1fr;}.detail{position:static;}.meth-grid,.meth-axes{grid-template-columns:1fr;}}
#clustersView{display:block;}
#scatterView{display:none;}
.view-tabs{display:flex;gap:6px;margin-bottom:16px;}
.view-tab{padding:6px 16px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text2);font-family:var(--font);font-size:11px;font-weight:600;cursor:pointer;transition:all .15s;}
.view-tab.active{background:rgba(108,92,231,.15);border-color:rgba(108,92,231,.4);color:var(--accent2);}
.view-tab:hover:not(.active){color:var(--text);}
.scatter-wrap{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden;}
.scatter-head{padding:14px 18px;border-bottom:1px solid var(--border);background:var(--surface2);display:flex;align-items:center;gap:12px;flex-wrap:wrap;}
.scatter-title{font-family:var(--mono);font-size:11px;color:var(--text2);font-weight:500;}
.scatter-legend{display:flex;flex-wrap:wrap;gap:8px;flex:1;}
.scatter-leg-item{display:flex;align-items:center;gap:5px;font-size:10px;color:var(--text2);cursor:pointer;}
.scatter-leg-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;}
.scatter-canvas{width:100%;height:600px;display:block;cursor:grab;background:#0f1117;}
.scatter-canvas:active{cursor:grabbing;}
.scatter-tooltip{position:absolute;background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:8px 10px;font-size:11px;pointer-events:none;display:none;z-index:100;max-width:240px;box-shadow:0 4px 20px rgba(0,0,0,0.4);}
.scatter-tooltip .tt-name{font-weight:600;margin-bottom:4px;}
.scatter-tooltip .tt-row{font-family:var(--mono);font-size:9px;color:var(--text2);margin-top:2px;}
.scatter-foot{padding:8px 18px;border-top:1px solid var(--border);display:flex;gap:16px;align-items:center;flex-wrap:wrap;}
.scatter-hint{font-size:10px;color:var(--text2);}
.scatter-controls{display:flex;gap:6px;margin-left:auto;}
.scatter-ctrl-btn{padding:3px 10px;border-radius:4px;background:var(--surface2);border:1px solid var(--border);color:var(--text2);font-family:var(--font);font-size:10px;cursor:pointer;}
.scatter-ctrl-btn:hover{color:var(--text);}
</style>
</head>
<body>
<nav>
  <div class="nav-logo">Hummingbird</div>
  <div class="nav-sep"></div>
  <div class="nav-sub">Opportunity Clusters</div>
  <div class="nav-spacer"></div>
  <a href="index.html" class="nav-back">\u2190 Map</a>\n  <a href="scatter3d.html" class="nav-back">3D Scatter \u2197</a>
</nav>
<div class="page">
  <div class="methodology">
    <div class="meth-toggle" id="methToggle">
      <div class="meth-label">Methodology &amp; Data Notes</div>
      <div class="meth-sub">v3 \u2014 thesis-driven clustering across urgency, land, and HV fit axes</div>
      <div class="meth-arrow" id="methArrow">\u25be</div>
    </div>
    <div class="meth-body" id="methBody">
      <div class="meth-axes">
        <div class="meth-axis">
          <div class="meth-axis-title urgency">Urgency Axis</div>
          <p>How close is this institution to transacting? Combines distress score, closure risk (ML model), operating loss flags, enrollment trajectory, and solvency indicators. High urgency = near-term transaction probability.</p>
        </div>
        <div class="meth-axis">
          <div class="meth-axis-title land">Land Axis</div>
          <p>How much underleveraged physical asset is here? Combines log-acreage, land potential flags, rurality, and urbanization. High land score = meaningful real estate opportunity relative to institutional size.</p>
        </div>
        <div class="meth-axis">
          <div class="meth-axis-title fit">PR Fit Axis</div>
          <p>Does the surrounding environment match HV's outdoor/wellness programming thesis? Driven by the OSM Environment Score (0\u2013100) \u2014 a composite of named natural feature tiers, category diversity, feature density, and proximity.</p>
        </div>
      </div>
      <div class="meth-grid">
        <div class="meth-card">
          <div class="meth-ct">IPEDS \u2014 Higher Education (K=6)</div>
          <p>All 5,912 IPEDS institutions are clustered together. <b>Institution type is not a feature</b> \u2014 it is used only for post-hoc labeling. This allows the model to discover cross-type patterns driven by urgency, land, and fit rather than simply rediscovering ownership categories.</p>
          <p>Continuous features are standardized before clustering (zero mean, unit variance). Missing values are imputed at the column median within the full IPEDS segment.</p>
          <div class="meth-feats">
            <span class="meth-feat">distress_score</span><span class="meth-feat">closure_risk_score</span>
            <span class="meth-feat">enrollment_score_ipeds</span><span class="meth-feat">solvency_score_ipeds</span>
            <span class="meth-feat">enrollment_2yr_pct</span><span class="meth-feat">revenue_2yr_pct</span>
            <span class="meth-feat">log_acreage</span><span class="meth-feat">flag_rural_suburban</span>
            <span class="meth-feat">flag_land_potential</span><span class="meth-feat">osm_environment_score</span>
            <span class="meth-feat">log_enrollment</span><span class="meth-feat">log_revenue</span>
            <span class="meth-feat">5 distress flags</span><span class="meth-feat">4 urbanization one-hots</span>
            <span class="meth-feat">5 region one-hots</span>
          </div>
        </div>
        <div class="meth-card c990">
          <div class="meth-ct">990 \u2014 Nonprofits (K=5)</div>
          <p>All 6,999 nonprofits clustered together with institution type as a one-hot feature (not a split criterion). 990 financials use IRS Form 990 reporting: plant/property/equipment as physical asset proxy, equity ratio, and 990-specific distress subdomain scores.</p>
          <p><b>Partnership Readiness Score</b> is included as the HV Fit axis, and imputed at median otherwise. Currently """ + str(osm_scraped) + """ institutions scraped (\u2248""" + str(osm_pct) + """% of total).</p>
          <div class="meth-feats">
            <span class="meth-feat">distress_score</span><span class="meth-feat">solvency/operating/trend scores</span>
            <span class="meth-feat">red_flag_score_990</span><span class="meth-feat">equity_ratio</span>
            <span class="meth-feat">log_ppe</span><span class="meth-feat">log_acreage</span>
            <span class="meth-feat">osm_environment_score</span><span class="meth-feat">6 distress flags</span>
            <span class="meth-feat">10 type one-hots</span><span class="meth-feat">5 region one-hots</span>
          </div>
        </div>
      </div>
      <div class="meth-card" style="margin-bottom:14px;border-left:3px solid var(--accent2);">
        <div class="meth-ct" style="color:var(--accent2)">OSM Environment Score (0\u2013100)</div>
        <p>Computed per institution from the OSM land scrape. <b>Feature Quality (55% weight)</b>: named natural features scored by tier \u2014 Ski Resort/National Park/Coastline (30 pts named, 11 pts unnamed), Lake/Beach/Mountain/River (22/8 pts), Hiking Trail/Wetland/Campground (14/5 pts), Golf/Marina/Park (8/3 pts). Named features score 2.9\u00d7 vs unnamed (a named lake is not Crater Lake). <b>Category Mix (30%)</b>: winter +25, water +20, outdoor recreation +20, protected land +15, natural features +10, tourism +8. <b>Feature Density (10%)</b>: log-scaled count of features found. <b>Proximity (5%)</b>: closer search radius = better density.</p>
      </div>
      <div class="meth-caveat">
        <b>Caveats:</b> OSM environment scores are imputed at the segment median for unscraped institutions \u2014 as coverage grows from """ + str(osm_scraped) + """ toward 12,911, cluster boundaries will shift. Cluster names are human-assigned archetypes based on centroid inspection; every member institution is not guaranteed to match the archetype exactly. K was chosen by elbow analysis and domain judgment.
      </div>
      <div class="meth-meta">
        <div class="meth-mi">Run: <b>""" + run_date + """</b></div>
        <div class="meth-mi">IPEDS: <b>""" + f"{n_ipeds:,}" + """</b></div>
        <div class="meth-mi">990: <b>""" + f"{n_990:,}" + """</b></div>
        <div class="meth-mi">OSM scraped: <b>""" + f"{osm_scraped:,}" + """ / 12,911</b></div>
        <div class="meth-mi">K (IPEDS/990): <b>6 / 5</b></div>
        <div class="meth-mi">Algorithm: <b>K-Means, n_init=20, StandardScaler</b></div>
      </div>
    </div>
  </div>

  <div class="view-tabs">
    <div class="view-tab active" id="viewTabClusters">Clusters</div>
    <div class="view-tab" id="viewTab3D">3D Opportunity Space</div>
  </div>

  <div id="clustersView">
  <div class="page-eyebrow">Phase II \u00b7 Machine Learning</div>
  <div class="page-title">Opportunity Clusters</div>
  <div class="page-sub">""" + f"{total:,}" + """ institutions segmented across three axes: <b>urgency</b> (distress trajectory), <b>land</b> (physical asset potential), and <b>HV fit</b> (environment quality score). Institution type is a label, not a clustering feature.</div>

  <div class="source-tabs">
    <div class="s-tab active" id="tabIPEDS">IPEDS \u2014 Higher Education <span class="tc">""" + f"{n_ipeds:,}" + """</span></div>
    <div class="s-tab" id="tab990">990 \u2014 Nonprofits <span class="tc">""" + f"{n_990:,}" + """</span></div>
  </div>

  <div class="filter-bar" id="filterBar">
    <span class="filter-label">Filter:</span>
    <select class="filter-select" id="filterType">
      <option value="">All Institution Types</option>
      <option value="Public">Public</option>
      <option value="Non-Profit">Private Non-Profit</option>
      <option value="For-Profit">Private For-Profit</option>
    </select>
    <select class="filter-select" id="filterAcreage">
      <option value="">All Acreage</option>
      <option value="any">Has Acreage Data</option>
      <option value="10">10+ acres</option>
      <option value="50">50+ acres</option>
      <option value="100">100+ acres</option>
      <option value="250">250+ acres</option>
      <option value="500">500+ acres</option>
    </select>
    <span class="filter-count" id="filterCount"></span>
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
  </div><!-- /clustersView -->

  <div id="scatterView" style="display:none">
    <div class="scatter-wrap" style="position:relative">
      <div class="scatter-head">
        <div class="scatter-title">URGENCY × LAND × PR FIT — each dot is one institution</div>
        <div class="scatter-legend" id="scatterLegend"></div>
      </div>
      <canvas id="scatterCanvas" class="scatter-canvas" style="width:100%;height:600px;display:block;"></canvas>
      <div class="scatter-tooltip" id="scatterTooltip"></div>
      <div class="scatter-foot">
        <div class="scatter-hint">Drag to rotate · Scroll to zoom · Hover for details · Grey = OSM not yet scraped</div>
        <div class="scatter-controls">
          <button class="scatter-ctrl-btn" id="scatterReset">Reset View</button>
          <button class="scatter-ctrl-btn" id="btnIPEDS3D">IPEDS</button>
          <button class="scatter-ctrl-btn" id="btn9903D">990</button>
        </div>
      </div>
    </div>
  </div>
</div>\n</div>\n<script>\nvar IPEDS_DATA=""")
    parts.append(ipeds_js)
    parts.append(";var HB990_DATA=")
    parts.append(hb990_js)
    parts.append(""";
var src='ipeds',activeId=null,sortCol='distress',sortAsc=false,searchQ='';
var filterType='',filterAcreage='';
function getClusters(){return src==='ipeds'?IPEDS_DATA:HB990_DATA;}
function envColor(s){if(s===null||s===undefined)return'#3d4f6e';if(s>=85)return'#00b894';if(s>=70)return'#10b981';if(s>=55)return'#ffa502';if(s>=35)return'#ff6b35';return'#8b8fa3';}
function renderCards(){
  var cl=getClusters(),html='';
  for(var i=0;i<cl.length;i++){
    var c=cl[i],s=c.stats,active=(activeId===c.id);
    var urgColor=s.urgency_score>60?'#ff4757':s.urgency_score>40?'#ff6b35':'#ffa502';
    var landColor=s.land_score>60?'#00b894':s.land_score>30?'#10b981':'#8b8fa3';
    var fitColor=s.fit_score?envColor(s.fit_score):'#3d4f6e';
    var chips='<span class="chip">Distress <b>'+s.median_distress+'</b></span>';
    if(src==='ipeds')chips+='<span class="chip">Enroll <b>'+(s.median_enrollment||0).toLocaleString()+'</b></span>';
    else chips+='<span class="chip">Rev <b>$'+(s.median_revenue_k||0).toLocaleString()+'K</b></span>';
    if(s.median_acres>0)chips+='<span class="chip">Acres <b>'+s.median_acres+'</b></span>';
    if(s.fit_score)chips+='<span class="chip">Env <b>'+s.fit_score+'</b></span>';
    html+='<div class="card'+(active?' active':'')+'" data-cid="'+c.id+'">';
    html+='<div class="card-bar" style="background:'+c.color+'"></div>';
    html+='<div class="card-hdr"><div class="card-icon">'+c.icon+'</div>';
    html+='<div class="card-name">'+c.name+'</div>';
    html+='<div class="card-n">'+c.count.toLocaleString()+'</div></div>';
    html+='<div class="card-desc">'+c.desc.substring(0,110)+(c.desc.length>110?'\u2026':'')+'</div>';
    html+='<div class="chips">'+chips+'</div>';
    html+='<div class="axis-bars">';
    html+='<div class="axis-bar-wrap"><div class="axis-bar-label">Urgency</div><div class="axis-bar-track"><div class="axis-bar-fill" style="width:'+Math.min(s.urgency_score||0,100)+'%;background:'+urgColor+'"></div></div></div>';
    html+='<div class="axis-bar-wrap"><div class="axis-bar-label">Land</div><div class="axis-bar-track"><div class="axis-bar-fill" style="width:'+Math.min(s.land_score||0,100)+'%;background:'+landColor+'"></div></div></div>';
    html+='<div class="axis-bar-wrap"><div class="axis-bar-label">PR Fit</div><div class="axis-bar-track"><div class="axis-bar-fill" style="width:'+Math.min(s.fit_score||0,100)+'%;background:'+fitColor+'"></div></div></div>';
    html+='</div></div>';
  }
  if(!html)html='<div style="padding:20px;font-size:11px;color:var(--text2)">No clusters found.</div>';
  document.getElementById('cards').innerHTML=html;
}
function selectCluster(cid){
  activeId=cid;searchQ='';sortCol='distress';sortAsc=false;filterType='';filterAcreage='';
  var fs=document.getElementById('filterType');if(fs)fs.value='';
  var fa=document.getElementById('filterAcreage');if(fa)fa.value='';

// ── View tab switching ────────────────────────────────────────────────────
document.getElementById('viewTabClusters').addEventListener('click', function() {
  document.getElementById('clustersView').style.display = 'block';
  document.getElementById('scatterView').style.display = 'none';
  document.getElementById('viewTabClusters').classList.add('active');
  document.getElementById('viewTab3D').classList.remove('active');
});
document.getElementById('viewTab3D').addEventListener('click', function() {
  try {
    document.getElementById('clustersView').style.display = 'none';
    document.getElementById('scatterView').style.display = 'block';
    document.getElementById('viewTabClusters').classList.remove('active');
    document.getElementById('viewTab3D').classList.add('active');
    initScatter();
  } catch(err) { console.error('3D tab error:', err); }
});
document.getElementById('btn9903D').addEventListener('click', function() { scatter3DSrc='990'; buildPoints(); drawScene(); buildLegend(); });

// ── 3D Scatter Engine ─────────────────────────────────────────────────────
var scatter3DSrc = 'ipeds';
var scatterPts   = [];  // {x,y,z,color,name,state,d,cr,a,hasOSM}
var camera = { rotX: 0.4, rotY: -0.6, zoom: 1.0, tx: 0, ty: 0 };
var dragging = false, lastMX = 0, lastMY = 0;
var scatterInited = false;

function initScatter() {
  if (scatterInited) return;
  scatterInited = true;
  // Force scatterView visible before measuring canvas
  var sv = document.getElementById('scatterView');
  if (sv) sv.style.display = 'block';
  buildPoints();
  buildLegend();
  setupScatterEvents();
  // Give layout time to settle then draw
  setTimeout(function() { drawScene(); }, 50);
}
function buildPoints() {
  var cl = scatter3DSrc === 'ipeds' ? IPEDS_DATA : HB990_DATA;
  scatterPts = [];
  for (var ci = 0; ci < cl.length; ci++) {
    var c = cl[ci];
    var pts = c.points || [];
    for (var pi = 0; pi < pts.length; pi++) {
      var p = pts[pi];
      var hasOSM = (p.f !== null && p.f !== undefined);
      // Normalize to 0-1 range for rendering
      var x = (p.u || 0) / 100;
      var y = (p.l || 0) / 100;
      var z = hasOSM ? (p.f / 100) : 0.45;  // unscraped at midpoint
      scatterPts.push({
        x: x, y: y, z: z,
        color: hasOSM ? c.color : '#3d4f6e',
        name: p.n, state: p.st,
        u: p.u, l: p.l, f: p.f,
        d: p.d, cr: p.cr, a: p.a,
        hasOSM: hasOSM,
        cluster: c.name,
      });
    }
  }
}

function buildLegend() {
  var cl = scatter3DSrc === 'ipeds' ? IPEDS_DATA : HB990_DATA;
  var html = '';
  for (var i = 0; i < cl.length; i++) {
    html += '<div class="scatter-leg-item">'
      + '<div class="scatter-leg-dot" style="background:' + cl[i].color + '"></div>'
      + cl[i].name
      + '</div>';
  }
  html += '<div class="scatter-leg-item">'
    + '<div class="scatter-leg-dot" style="background:#3d4f6e"></div>'
    + 'Not yet scraped'
    + '</div>';
  document.getElementById('scatterLegend').innerHTML = html;
}

function project(x, y, z) {
  // Rotate around Y axis then X axis
  var cosY = Math.cos(camera.rotY), sinY = Math.sin(camera.rotY);
  var cosX = Math.cos(camera.rotX), sinX = Math.sin(camera.rotX);
  // Center around 0.5,0.5,0.5
  var cx = x - 0.5, cy = y - 0.5, cz = z - 0.5;
  // Rotate Y
  var rx = cx * cosY - cz * sinY;
  var rz = cx * sinY + cz * cosY;
  // Rotate X
  var ry = cy * cosX - rz * sinX;
  var rz2 = cy * sinX + rz * cosX;
  // Perspective
  var fov = 2.5 * camera.zoom;
  var pz = rz2 + 2.0;
  var sx = rx / pz * fov;
  var sy = ry / pz * fov;
  return { sx: sx, sy: sy, depth: pz };
}

function drawScene() {
  var canvas = document.getElementById('scatterCanvas');
  if (!canvas) return;
  var W = Math.max(canvas.offsetWidth || 0, canvas.parentElement ? canvas.parentElement.offsetWidth : 0, 600);
  var H = 600;
  canvas.width = W; canvas.height = H;
  var ctx = canvas.getContext('2d');
  ctx.fillStyle = '#0f1117';
  ctx.fillRect(0, 0, W, H);

  // Draw axis lines
  var cx = W/2 + camera.tx, cy = H/2 + camera.ty;
  var axisEnds = [
    [[0.5,0.5,0.5],[1,0.5,0.5],'#ff6b35','Urgency'],
    [[0.5,0.5,0.5],[0.5,1,0.5],'#00b894','Land'],
    [[0.5,0.5,0.5],[0.5,0.5,1],'#a29bfe','PR Fit'],
  ];
  axisEnds.forEach(function(ax) {
    var p0 = project(ax[0][0],ax[0][1],ax[0][2]);
    var p1 = project(ax[1][0],ax[1][1],ax[1][2]);
    ctx.strokeStyle = ax[2];
    ctx.lineWidth = 1.5;
    ctx.globalAlpha = 0.5;
    ctx.beginPath();
    ctx.moveTo(cx + p0.sx*W*0.5, cy - p0.sy*H*0.5);
    ctx.lineTo(cx + p1.sx*W*0.5, cy - p1.sy*H*0.5);
    ctx.stroke();
    // Label
    ctx.globalAlpha = 0.7;
    ctx.fillStyle = ax[2];
    ctx.font = '10px JetBrains Mono, monospace';
    ctx.fillText(ax[3], cx + p1.sx*W*0.5 + 4, cy - p1.sy*H*0.5 + 4);
    ctx.globalAlpha = 1;
  });

  // Sort by depth for painter's algorithm
  var projected = scatterPts.map(function(p) {
    var proj = project(p.x, p.y, p.z);
    return { p: p, proj: proj };
  }).sort(function(a,b) { return b.proj.depth - a.proj.depth; });

  // Draw points
  projected.forEach(function(item) {
    var proj = item.proj;
    var px = cx + proj.sx * W * 0.5;
    var py = cy - proj.sy * H * 0.5;
    var r = item.p.hasOSM ? 3.5 : 2.5;
    ctx.beginPath();
    ctx.arc(px, py, r, 0, Math.PI*2);
    ctx.fillStyle = item.p.color;
    ctx.globalAlpha = item.p.hasOSM ? 0.85 : 0.35;
    ctx.fill();
    ctx.globalAlpha = 1;
  });

  // Axis origin labels
  var labels = [
    {pos:[1,0.5,0.5], text:'100', color:'#ff6b35'},
    {pos:[0.5,1,0.5], text:'100', color:'#00b894'},
    {pos:[0.5,0.5,1], text:'100', color:'#a29bfe'},
    {pos:[0,0.5,0.5], text:'0', color:'#3d4f6e'},
    {pos:[0.5,0,0.5], text:'0', color:'#3d4f6e'},
    {pos:[0.5,0.5,0], text:'0', color:'#3d4f6e'},
  ];
  labels.forEach(function(lb) {
    var proj = project(lb.pos[0],lb.pos[1],lb.pos[2]);
    ctx.fillStyle = lb.color;
    ctx.globalAlpha = 0.5;
    ctx.font = '9px JetBrains Mono, monospace';
    ctx.fillText(lb.text, cx + proj.sx*W*0.5 + 3, cy - proj.sy*H*0.5 + 3);
    ctx.globalAlpha = 1;
  });
}

function setupScatterEvents() {
  var canvas = document.getElementById('scatterCanvas');
  var tooltip = document.getElementById('scatterTooltip');

  canvas.addEventListener('mousedown', function(e) {
    dragging = true; lastMX = e.clientX; lastMY = e.clientY;
  });
  window.addEventListener('mouseup', function() { dragging = false; });
  window.addEventListener('mousemove', function(e) {
    if (dragging) {
      camera.rotY += (e.clientX - lastMX) * 0.008;
      camera.rotX += (e.clientY - lastMY) * 0.008;
      lastMX = e.clientX; lastMY = e.clientY;
      drawScene();
    }
    // Hover tooltip
    var rect = canvas.getBoundingClientRect();
    var mx = e.clientX - rect.left;
    var my = e.clientY - rect.top;
    var W = canvas.width, H = canvas.height;
    var cx = W/2 + camera.tx, cy = H/2 + camera.ty;
    var closest = null, closestDist = 14;
    scatterPts.forEach(function(p) {
      var proj = project(p.x, p.y, p.z);
      var px = cx + proj.sx*W*0.5;
      var py = cy - proj.sy*H*0.5;
      var dist = Math.sqrt((mx-px)*(mx-px) + (my-py)*(my-py));
      if (dist < closestDist) { closest = p; closestDist = dist; }
    });
    if (closest) {
      tooltip.style.display = 'block';
      tooltip.style.left = (e.clientX - rect.left + 12) + 'px';
      tooltip.style.top = (e.clientY - rect.top - 10) + 'px';
      var fitStr = closest.hasOSM ? closest.f.toFixed(1) : 'Not scraped';
      tooltip.innerHTML = '<div class="tt-name">' + esc(closest.name) + '</div>'
        + '<div class="tt-row">' + esc(closest.state) + ' · ' + esc(closest.cluster) + '</div>'
        + '<div class="tt-row">Urgency <b>' + closest.u + '</b> · Land <b>' + closest.l + '</b> · Fit <b>' + fitStr + '</b></div>'
        + '<div class="tt-row">Distress <b>' + closest.d + '</b> · Closure Risk <b>' + (closest.cr*100).toFixed(1) + '%</b></div>'
        + (closest.a > 0 ? '<div class="tt-row">Acreage <b>' + closest.a + ' ac</b></div>' : '');
    } else {
      tooltip.style.display = 'none';
    }
  });
  canvas.addEventListener('wheel', function(e) {
    e.preventDefault();
    camera.zoom *= e.deltaY > 0 ? 0.92 : 1.08;
    camera.zoom = Math.max(0.3, Math.min(4.0, camera.zoom));
    drawScene();
  }, {passive: false});
  // Touch support
  var lastTouchDist = 0;
  canvas.addEventListener('touchstart', function(e) {
    if (e.touches.length === 1) { dragging = true; lastMX = e.touches[0].clientX; lastMY = e.touches[0].clientY; }
    if (e.touches.length === 2) { lastTouchDist = Math.hypot(e.touches[0].clientX-e.touches[1].clientX, e.touches[0].clientY-e.touches[1].clientY); }
    e.preventDefault();
  }, {passive:false});
  canvas.addEventListener('touchmove', function(e) {
    if (e.touches.length === 1 && dragging) {
      camera.rotY += (e.touches[0].clientX - lastMX) * 0.008;
      camera.rotX += (e.touches[0].clientY - lastMY) * 0.008;
      lastMX = e.touches[0].clientX; lastMY = e.touches[0].clientY;
      drawScene();
    }
    if (e.touches.length === 2) {
      var d = Math.hypot(e.touches[0].clientX-e.touches[1].clientX, e.touches[0].clientY-e.touches[1].clientY);
      camera.zoom *= d / lastTouchDist;
      camera.zoom = Math.max(0.3, Math.min(4.0, camera.zoom));
      lastTouchDist = d; drawScene();
    }
    e.preventDefault();
  }, {passive:false});
  canvas.addEventListener('touchend', function() { dragging = false; });
}

function resetCamera() {
  camera = { rotX: 0.4, rotY: -0.6, zoom: 1.0, tx: 0, ty: 0 };
}

  renderCards();
  var cl=getClusters(),c=null;
  for(var i=0;i<cl.length;i++){if(cl[i].id===cid){c=cl[i];break;}}
  if(!c)return;
  renderDetail(c);
}
function renderDetail(c){
  var s=c.stats,isIPEDS=(src==='ipeds');
  var urgColor=s.urgency_score>60?'#ff4757':s.urgency_score>40?'#ff6b35':'#ffa502';
  var landColor=s.land_score>60?'#00b894':s.land_score>30?'#10b981':'#8b8fa3';
  var fitColor=s.fit_score?envColor(s.fit_score):'#3d4f6e';
  var fitLabel=s.fit_score?s.fit_score+(s.osm_scraped_pct<30?' ('+s.osm_scraped_pct+'% scraped)':''):'\u2014';
  var axisHtml='<div class="axis-strip">'
    +'<div class="axis-cell"><span class="axis-val" style="color:'+urgColor+'">'+s.urgency_score+'</span><div class="axis-lbl">Urgency</div><div class="axis-sub">Distress trajectory</div></div>'
    +'<div class="axis-cell"><span class="axis-val" style="color:'+landColor+'">'+s.land_score+'</span><div class="axis-lbl">Land</div><div class="axis-sub">Physical asset potential</div></div>'
    +'<div class="axis-cell"><span class="axis-val" style="color:'+fitColor+'">'+fitLabel+'</span><div class="axis-lbl">PR Fit</div><div class="axis-sub">Partnership readiness</div></div>'
    +'</div>';
  var statCols,statHtml='';
  if(isIPEDS){
    statCols=4;
    statHtml+=mkStat(s.median_distress,'Distress')+mkStat((s.median_enrollment||0).toLocaleString(),'Enroll')
      +mkStat(s.median_acres>0?s.median_acres+' ac':'\u2014','Acres')
      +mkStat(s.median_closure_risk?(s.median_closure_risk*100).toFixed(1)+'%':'\u2014','Closure Risk');
  }else{
    statCols=4;
    statHtml+=mkStat(s.median_distress,'Distress')+mkStat('$'+(s.median_revenue_k||0).toLocaleString()+'K','Revenue')
      +mkStat(s.median_ppe_k?'$'+s.median_ppe_k.toLocaleString()+'K':'\u2014','PPE')
      +mkStat(s.median_acres>0?s.median_acres+' ac':'\u2014','Acres');
  }
  var flagDefs=isIPEDS
    ?[['Enrollment Decline',s.pct_enrollment_decline,'#ff6b35'],['Op. Losses',s.pct_operating_losses,'#ff4757'],
      ['Neg. Net Worth',s.pct_negative_net_worth,'#ff4757'],['Land Potential',s.pct_land_potential,'#00b894']]
    :[['Operating Loss',s.pct_operating_loss,'#ff6b35'],['Neg. Net Assets',s.pct_negative_assets,'#ff4757'],
      ['High Debt',s.pct_high_debt,'#ffa502'],['Consec. Losses',s.pct_consecutive_losses,'#ff6b35'],
      ['Revenue Decline',s.pct_revenue_decline,'#ff4757']];
  var flagHtml='';
  for(var fi=0;fi<flagDefs.length;fi++){
    var fd=flagDefs[fi],pv=fd[1]||0;
    flagHtml+='<div class="frow"><div class="flabel">'+fd[0]+'</div>'
      +'<div class="fbar-wrap"><div class="fbar" style="width:'+Math.min(pv,100)+'%;background:'+fd[2]+'"></div></div>'
      +'<div class="fpct">'+pv+'%</div></div>';
  }
  var html='<div class="d-head" style="border-left:3px solid '+c.color+'">'
    +'<div class="dh-icon">'+c.icon+'</div>'
    +'<div><div class="dh-name" style="color:'+c.color+'">'+c.name+'</div>'
    +'<div class="dh-count">'+c.count.toLocaleString()+' institutions \u00b7 '+s.top_region+' most common</div>'
    +'<div class="dh-desc">'+c.desc+'</div></div></div>'
    +'<div class="hv-angle"><div class="hv-angle-label">HV Angle</div><div class="hv-angle-text">'+c.hv_angle+'</div></div>'
    +axisHtml
    +'<div class="stat-strip" style="grid-template-columns:repeat('+statCols+',1fr)">'+statHtml+'</div>'
    +'<div class="flags"><div class="flags-title">Distress Indicators</div>'+flagHtml+'</div>'
    +buildTable(c);
  updateFilterCount(c);
  document.getElementById('detail').innerHTML=html;
}
function mkStat(val,label){return '<div class="stat-cell"><span class="stat-v">'+val+'</span><div class="stat-l">'+label+'</div></div>';}
function buildTable(c){
  var filtered=filterRows(c.rows),sorted=sortRows(filtered),isIPEDS=(src==='ipeds');
  var headers=isIPEDS
    ?[['distress','Distress'],['name','Institution'],['enrollment','Enroll'],['revenue','Rev($K)'],['acres','Acres'],['closure_risk','Risk'],['env_score','Env']]
    :[['distress','Distress'],['name','Institution'],['revenue','Rev($K)'],['acres','Acres'],['env_score','Env'],['type','Type']];
  var thead='';
  for(var hi=0;hi<headers.length;hi++){
    var h=headers[hi],arrow=(sortCol===h[0])?(sortAsc?' \u2191':' \u2193'):'';
    thead+='<th data-col="'+h[0]+'">'+h[1]+arrow+'</th>';
  }
  var tbody='',limit=Math.min(sorted.length,200);
  for(var ri=0;ri<limit;ri++){
    var r=sorted[ri];
    var dc=(r.category||'').toLowerCase().replace(/ /g,'');
    var dpill='<span class="dp d-'+dc+'">'+(r.distress||0).toFixed(0)+' '+(r.category||'')+'</span>';
    var tshort=(r.type||'').replace('Private Non-Profit | ','NP ').replace('Private For-Profit | ','FP ').replace('Public | ','Pub ');
    var envPill=r.env_score!=null?('<span class="env-pill" style="background:'+envColor(r.env_score)+'22;color:'+envColor(r.env_score)+'">'+r.env_score+'</span>'):'\u2014';
    var nameCell='<td class="td-name">'+esc(r.name)+'<small>'+esc(r.city)+', '+esc(r.state)+'</small></td>';
    var tds='<td>'+dpill+'</td>'+nameCell;
    if(isIPEDS){
      tds+='<td>'+(r.enrollment!=null?r.enrollment.toLocaleString():'\u2014')+'</td>';
      tds+='<td>'+(r.revenue!=null?'$'+r.revenue.toLocaleString()+'K':'\u2014')+'</td>';
      tds+='<td>'+(r.acres!=null?r.acres:'\u2014')+'</td>';
      tds+='<td>'+(r.closure_risk!=null?(r.closure_risk*100).toFixed(1)+'%':'\u2014')+'</td>';
      tds+='<td>'+envPill+'</td>';
    }else{
      tds+='<td>'+(r.revenue!=null?'$'+r.revenue.toLocaleString()+'K':'\u2014')+'</td>';
      tds+='<td>'+(r.acres!=null?r.acres:'\u2014')+'</td>';
      tds+='<td>'+envPill+'</td>';
      tds+='<td style="font-size:9px;color:var(--text2)">'+esc(tshort)+'</td>';
    }
    tbody+='<tr>'+tds+'</tr>';
  }
  return '<div class="tbar">'
    +'<input class="tsearch" placeholder="Search\u2026" oninput="onSearch(this.value)" value="'+esc(searchQ)+'">'
    +'<span class="tcount">'+filtered.length.toLocaleString()+' shown</span>'
    +'<button class="export-btn" onclick="doExport()">\u2193 CSV</button>'
    +'</div>'
    +'<div class="twrap"><table><thead><tr>'+thead+'</tr></thead><tbody>'+tbody+'</tbody></table></div>';
}
function filterRows(rows){
  var lq=searchQ.toLowerCase();
  var out=[];
  for(var i=0;i<rows.length;i++){
    var r=rows[i];
    if(lq && (r.name||'').toLowerCase().indexOf(lq)<0 && (r.state||'').toLowerCase().indexOf(lq)<0 && (r.city||'').toLowerCase().indexOf(lq)<0) continue;
    if(filterType && (r.type||'').indexOf(filterType)<0) continue;
    if(filterAcreage){
      if(filterAcreage==='any' && (r.acres===null||r.acres===undefined)) continue;
      else if(filterAcreage!=='any' && (r.acres===null||r.acres===undefined||r.acres < parseFloat(filterAcreage))) continue;
    }
    out.push(r);
  }
  return out;
}
function sortRows(rows){
  var col=sortCol,asc=sortAsc;
  return rows.slice().sort(function(a,b){
    var av=a[col],bv=b[col];
    if(av==null&&bv==null)return 0;if(av==null)return 1;if(bv==null)return -1;
    if(typeof av==='string')av=av.toLowerCase();if(typeof bv==='string')bv=bv.toLowerCase();
    var cmp=av<bv?-1:av>bv?1:0;return asc?cmp:-cmp;
  });
}
function setSort(col){
  if(sortCol===col)sortAsc=!sortAsc;else{sortCol=col;sortAsc=false;}
  var cl=getClusters();for(var i=0;i<cl.length;i++){if(cl[i].id===activeId){renderDetail(cl[i]);break;}}
}
function onSearch(val){
  searchQ=val;
  var cl=getClusters();for(var i=0;i<cl.length;i++){if(cl[i].id===activeId){renderDetail(cl[i]);break;}}
}
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function updateFilterCount(c){
  if(!c) return;
  var filtered=filterRows(c.rows);
  var total=c.rows.length;
  var el=document.getElementById('filterCount');
  if(el){
    if(filtered.length===total) el.textContent='';
    else el.textContent=filtered.length.toLocaleString()+' of '+total.toLocaleString()+' shown';
  }
}
function applyFilters(){
  var cl=getClusters(),c=null;
  for(var i=0;i<cl.length;i++){if(cl[i].id===activeId){c=cl[i];break;}}
  if(c) renderDetail(c);
}
function doExport(){
  var cl=getClusters(),c=null;
  for(var i=0;i<cl.length;i++){if(cl[i].id===activeId){c=cl[i];break;}}
  if(!c)return;
  var rows=filterRows(c.rows);
  var cols=src==='ipeds'
    ?['name','state','city','type','distress','category','enrollment','revenue','acres','closure_risk','env_score','risk_tier','urbanization']
    :['name','state','city','type','distress','category','revenue','acres','env_score'];
  var csvLines=[cols.join(',')];
  for(var ei=0;ei<rows.length;ei++){
    csvLines.push(cols.map(function(col){return'"'+String(rows[ei][col]!=null?rows[ei][col]:'').replace(/"/g,'""')+'"';}).join(','));
  }
  var blob=new Blob([csvLines.join(String.fromCharCode(10))],{type:'text/csv'});
  var a=document.createElement('a');
  a.href=URL.createObjectURL(blob);
  a.download='hb_cluster_'+c.name.replace(/[^a-z0-9]+/g,'_').toLowerCase()+'.csv';
  a.click();
}
document.getElementById('tabIPEDS').addEventListener('click',function(){
  filterType='';filterAcreage='';
  var fs=document.getElementById('filterType');if(fs)fs.value='';
  var fa=document.getElementById('filterAcreage');if(fa)fa.value='';
  src='ipeds';activeId=null;searchQ='';
  document.querySelectorAll('.s-tab').forEach(function(t){t.classList.remove('active');});
  this.classList.add('active');renderCards();
  document.getElementById('detail').innerHTML='<div class="d-empty"><div class="d-empty-arrow">\u2190</div><div class="d-empty-title">Select a cluster</div><div class="d-empty-sub">Click any card to explore its institutions</div></div>';
});
document.getElementById('tab990').addEventListener('click',function(){
  filterType='';filterAcreage='';
  var fs=document.getElementById('filterType');if(fs)fs.value='';
  var fa=document.getElementById('filterAcreage');if(fa)fa.value='';
  src='990';activeId=null;searchQ='';
  document.querySelectorAll('.s-tab').forEach(function(t){t.classList.remove('active');});
  this.classList.add('active');renderCards();
  document.getElementById('detail').innerHTML='<div class="d-empty"><div class="d-empty-arrow">\u2190</div><div class="d-empty-title">Select a cluster</div><div class="d-empty-sub">Click any card to explore its institutions</div></div>';
});
document.getElementById('cards').addEventListener('click',function(e){
  var card=e.target.closest('[data-cid]');
  if(card)selectCluster(parseInt(card.getAttribute('data-cid'),10));
});
document.getElementById('detail').addEventListener('click',function(e){
  var th=e.target.closest('th[data-col]');
  if(th)setSort(th.getAttribute('data-col'));
});
document.getElementById('methToggle').addEventListener('click',function(){
  document.getElementById('methBody').classList.toggle('open');
  document.getElementById('methArrow').classList.toggle('open');
});
document.getElementById('filterType').addEventListener('change',function(){
  filterType=this.value; applyFilters();
});
document.getElementById('filterAcreage').addEventListener('change',function(){
  filterAcreage=this.value; applyFilters();
});
// Hide filter bar on 990 tab since those rows have less type/acreage data
document.getElementById('tabIPEDS').addEventListener('click',function(){},true);
renderCards();
</script>
</body>
</html>""")

    return ''.join(parts)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description='Hummingbird Cluster Engine v3')
    parser.add_argument('--master',   default=DEFAULT_MASTER)
    parser.add_argument('--osm',      default=DEFAULT_OSM)
    parser.add_argument('--out-html', default=DEFAULT_OUT_HTML)
    parser.add_argument('--out-csv',  default=DEFAULT_OUT_CSV)
    parser.add_argument('--k-ipeds',  type=int, default=K_IPEDS)
    parser.add_argument('--k-990',    type=int, default=K_HB990)
    args = parser.parse_args()

    try: import sklearn
    except ImportError:
        print("ERROR: scikit-learn not installed. Run: pip install scikit-learn")
        sys.exit(1)

    scorer = load_scorer()
    if scorer: print("  OSM scorer loaded")
    else:      print("  Warning: osm_scorer.py not found — env scores will be None")

    print(f"Loading {args.master}...")
    csv.field_size_limit(100 * 1024 * 1024)
    rows = []
    with open(args.master, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f): rows.append(row)
    print(f"  {len(rows):,} rows loaded")

    # Build name→urbanization and name→state lookups for context-aware scoring
    _urb_lookup = {r.get('institution_name',''): r.get('urbanization','')
                   for r in rows if r.get('institution_name')}
    _state_lookup = {r.get('institution_name',''): r.get('state','')
                     for r in rows if r.get('institution_name')}

    # Load and score OSM data
    osm_scores = {}
    osm_scraped = 0
    if os.path.exists(args.osm):
        print(f"Loading + scoring OSM from {args.osm}...")
        csv.field_size_limit(100 * 1024 * 1024)
        with open(args.osm, newline='', encoding='utf-8', errors='replace') as f:
            for r in csv.DictReader(f):
                name     = r.get('institution_name','').strip()
                feat_str = r.get('osm_env_features','').strip()
                queried  = r.get('osm_env_queried_at','').strip()
                if not name or not queried or feat_str == 'ERROR': continue
                by_cat = parse_osm_feature_string(feat_str)
                count  = r.get('osm_env_count', 0)
                radius = r.get('osm_env_radius_miles', 20)
                if scorer:
                    score_result = scorer.score_osm_environment(
                        by_cat, feature_count=count, radius_miles=radius,
                        urbanization=_urb_lookup.get(name),
                        state=_state_lookup.get(name)
                    )
                else:
                    score_result = {'score': None, 'context_mult': None}
                osm_scores[name] = score_result
                osm_scraped += 1
        print(f"  {osm_scraped:,} institutions scored")
        scored = sum(1 for v in osm_scores.values() if v.get('score') is not None)
        if scored > 0:
            vals = sorted(v['score'] for v in osm_scores.values() if v.get('score') is not None)
            print(f"  Score distribution: min={vals[0]:.1f}  med={vals[len(vals)//2]:.1f}  max={vals[-1]:.1f}")
    else:
        print(f"  OSM file not found — env scores will be None")

    osm_pct = round(osm_scraped / len(rows) * 100)
    run_date = datetime.datetime.now().strftime('%Y-%m-%d')

    # IPEDS clustering
    print(f"\nIPEDS clustering (k={args.k_ipeds})...")
    ipeds_matrix, _, ipeds_idx = extract_ipeds_features(rows, osm_scores)
    print(f"  {len(ipeds_matrix):,} institutions x {len(ipeds_matrix[0])} features")
    ipeds_labels, _ = run_kmeans(ipeds_matrix, args.k_ipeds)

    for pos, row_idx in enumerate(ipeds_idx):
        rows[row_idx]['cluster_ipeds'] = str(ipeds_labels[pos])

    ipeds_groups = defaultdict(list)
    for pos, row_idx in enumerate(ipeds_idx):
        ipeds_groups[ipeds_labels[pos]].append(rows[row_idx])

    ipeds_clusters = []
    cid = 0
    for cid_local in sorted(ipeds_groups.keys()):
        info = name_ipeds_cluster(ipeds_groups[cid_local], cid, osm_scores)
        ipeds_clusters.append(info)
        print(f"  [{cid}] {info['icon']} {info['name']} — {info['count']:,}  urgency={info['stats']['urgency_score']}  land={info['stats']['land_score']}  fit={info['stats']['fit_score']}")
        cid += 1

    # Sort by priority then distress
    ipeds_clusters.sort(key=lambda c: (c['priority'], -c['stats']['median_distress']))

    # 990 clustering
    print(f"\n990 clustering (k={args.k_990})...")
    hb990_matrix, _, hb990_idx = extract_990_features(rows, osm_scores)
    print(f"  {len(hb990_matrix):,} institutions x {len(hb990_matrix[0])} features")
    hb990_labels, _ = run_kmeans(hb990_matrix, args.k_990)

    for pos, row_idx in enumerate(hb990_idx):
        rows[row_idx]['cluster_990'] = str(hb990_labels[pos])

    hb990_groups = defaultdict(list)
    for pos, row_idx in enumerate(hb990_idx):
        hb990_groups[hb990_labels[pos]].append(rows[row_idx])

    hb990_clusters = []
    for cid_local in sorted(hb990_groups.keys()):
        info = name_990_cluster(hb990_groups[cid_local], cid, osm_scores)
        hb990_clusters.append(info)
        print(f"  [{cid}] {info['icon']} {info['name']} — {info['count']:,}  urgency={info['stats']['urgency_score']}  land={info['stats']['land_score']}  fit={info['stats']['fit_score']}")
        cid += 1

    hb990_clusters.sort(key=lambda c: (c['priority'], -c['stats']['median_distress']))

    # Write CSV
    print(f"\nWriting CSV to {args.out_csv}...")
    fieldnames = list(rows[0].keys())
    for col in ['cluster_ipeds','cluster_990']:
        if col not in fieldnames: fieldnames.append(col)
    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    with open(args.out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    print("  Done.")

    # Write HTML
    print(f"\nGenerating {args.out_html}...")
    meta = {'run_date': run_date, 'osm_scraped': osm_scraped, 'osm_pct': osm_pct}
    html = build_html(ipeds_clusters, hb990_clusters, meta)
    with open(args.out_html, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  Done. ({len(html):,} chars)")

    print(f"\n\u2713 Complete.")
    print(f"  IPEDS: {len(ipeds_clusters)} clusters across {len(ipeds_matrix):,} institutions")
    print(f"  990:   {len(hb990_clusters)} clusters across {len(hb990_matrix):,} institutions")
    print(f"  HTML:  {args.out_html}")
    print(f"  CSV:   {args.out_csv}")

if __name__ == '__main__':
    main()