"""
osm_scorer.py — Hummingbird OSM Environment Scorer
====================================================
Computes a 0-100 environment quality score for each institution
based on the OSM land features scraped nearby.

Used by both master_standalone.py (land panel) and clusters.py
(as a feature in the opportunity cluster model).

SCORE FORMULA
-------------
osm_environment_score (0-100) =
    feature_score  x 0.55   (named feature tiers - richest signal)
  + category_score x 0.30   (coarse category presence)
  + density_bonus  x 0.10   (log-scaled feature count)
  + radius_bonus   x 0.05   (closer features = better)

FEATURE TIERS
-------------
Named multiplier   = 1.0   (has a proper name)
Unnamed multiplier = 0.35  (unnamed lake != Crater Lake)

Tier 1 (30 pts): Ski Resort, National Park, National Forest,
                 Coastline, Glacier, Volcano, Protected Area,
                 Nature Reserve, Wilderness Hut, Winter Sports Area
Tier 2 (22 pts): Lake/Pond, Beach, Mountain Peak, River, Forest,
                 Conservation Land, Cliff, Cave, Hot Spring,
                 Canal, Gondola, Cable Car, Chair Lift
Tier 3 (14 pts): Stream, Wetland, Woodland, Natural Spring,
                 Hiking Trail, Mountain Bike Trail, Canoe Route,
                 Campground, Rock Climbing, Surfing, Kayaking,
                 Skiing Facility, Downhill Ski Run, Nordic Ski Trail,
                 Equestrian Trail, Horse Riding, Rowing
Tier 4  (8 pts): Golf Course, Marina, Swimming Facility,
                 Cycling Route, Sports Centre, Fishing, Hunting,
                 Ice Skating, Drag Lift, Botanical Garden,
                 Scenic Viewpoint, Resort, Park
Tier 5  (3 pts): Tourist Attraction, Museum, Zoo, Aquarium,
                 Theme Park, Historic Monument, Ferry Terminal,
                 Train Station, Airport, Casino
"""
import math, re

TIER_1_LABELS = {
    'Ski Resort', 'National Park', 'National Forest', 'Protected Area',
    'Coastline', 'Glacier', 'Volcano', 'Nature Reserve', 'Wilderness Hut',
    'Winter Sports Area',
}
TIER_2_LABELS = {
    'Lake/Pond', 'Beach', 'Mountain Peak', 'River', 'Conservation Land',
    'Forest', 'Cliff', 'Cave', 'Hot Spring', 'Canal',
    'Gondola', 'Cable Car', 'Chair Lift',
}
TIER_3_LABELS = {
    'Stream', 'Wetland', 'Woodland', 'Natural Spring', 'Heathland', 'Scrubland',
    'Hiking Trail', 'Mountain Bike Trail', 'Canoe Route', 'Equestrian Trail',
    'Campground', 'Rock Climbing', 'Surfing', 'Kayaking', 'Rowing',
    'Skiing Facility', 'Downhill Ski Run', 'Nordic Ski Trail', 'Horse Riding',
}
TIER_4_LABELS = {
    'Golf Course', 'Golf', 'Marina', 'Swimming Facility', 'Cycling Route',
    'Bridleway', 'Sports Centre', 'Fishing', 'Hunting',
    'Ice Skating', 'Ice Hockey Rink', 'Drag Lift',
    'Botanical Garden', 'Scenic Viewpoint', 'Resort', 'Park',
}
TIER_5_LABELS = {
    'Tourist Attraction', 'Museum', 'Zoo', 'Aquarium', 'Theme Park',
    'Historic Monument', 'Ferry Terminal', 'Train Station', 'Airport', 'Casino',
}
TIER_POINTS = {1: 30, 2: 22, 3: 14, 4: 8, 5: 3}

def _get_tier(label):
    if label in TIER_1_LABELS: return 1
    if label in TIER_2_LABELS: return 2
    if label in TIER_3_LABELS: return 3
    if label in TIER_4_LABELS: return 4
    if label in TIER_5_LABELS: return 5
    return None

NAMED_MULTIPLIER   = 1.0
UNNAMED_MULTIPLIER = 0.35

# ---------------------------------------------------------------------------
# Urban context multipliers
# Applied to the final composite score based on urbanization classification.
# Rationale: OSM features within 20 miles of a dense urban institution are
# rarely "accessible" to that institution — they are regional geography, not
# campus-adjacent land. Rural institutions truly sit inside their environment.
# ---------------------------------------------------------------------------
URBAN_CONTEXT_MULT = {
    'City: Large':    0.30,   # dense urban — features are regional noise
    'City: Midsize':  0.40,
    'City: Small':    0.50,
    'Suburb: Large':  0.65,
    'Suburb: Midsize':0.72,
    'Suburb: Small':  0.78,
    'Town: Fringe':   0.85,
    'Town: Distant':  0.88,
    'Town: Remote':   0.92,
    'Rural: Fringe':  0.95,
    'Rural: Distant': 0.97,
    'Rural: Remote':  1.00,
}

def get_urban_mult(urbanization):
    """Return the context multiplier for a given urbanization string."""
    if not urbanization:
        return 0.80   # unknown — be conservative
    urb = urbanization.strip()
    # Exact match first
    if urb in URBAN_CONTEXT_MULT:
        return URBAN_CONTEXT_MULT[urb]
    # Prefix match fallback
    prefix = urb.split(':')[0].strip()
    if prefix == 'City':    return 0.40
    if prefix == 'Suburb':  return 0.70
    if prefix == 'Town':    return 0.88
    if prefix == 'Rural':   return 0.97
    return 0.80

def _count_named_unnamed(display_str):
    if not display_str: return 0, 0
    s = display_str.strip()
    unnamed_extra = 0
    m = re.search(r'\+(\d+) more$', s)
    if m:
        unnamed_extra = int(m.group(1))
        s = s[:m.start()].strip().rstrip(',').strip()
    if s.lower().startswith('unnamed') or s.startswith('+') or not s:
        return 0, unnamed_extra + (1 if s else 0)
    names = [n.strip() for n in s.split(',') if n.strip()]
    return len(names), unnamed_extra

CATEGORY_WEIGHTS = {
    'winter': 25, 'water': 20, 'outdoor_recreation': 20,
    'protected_land': 15, 'natural_features': 10,
    'tourism_leisure': 8, 'infrastructure': 2,
}
MAX_CATEGORY_SCORE = sum(CATEGORY_WEIGHTS.values())

# ---------------------------------------------------------------------------
# Winter climate multiplier
# Gondolas, sky rides, drag lifts in warm states are tourism infrastructure
# (theme parks, zoos, scenic rides), not winter sports development potential.
# Cold-climate states get full credit; warm states get heavy discount.
# ---------------------------------------------------------------------------
_WINTER_CLIMATE_MULT = {
    # Full credit — genuine winter sports states
    'Alaska': 1.0, 'Colorado': 1.0, 'Idaho': 1.0, 'Maine': 1.0,
    'Michigan': 1.0, 'Minnesota': 1.0, 'Montana': 1.0, 'New Hampshire': 1.0,
    'New York': 1.0, 'Oregon': 1.0, 'Utah': 1.0, 'Vermont': 1.0,
    'Washington': 1.0, 'Wisconsin': 1.0, 'Wyoming': 1.0,
    # Moderate — some winter sports potential in highland areas
    'California': 0.7, 'Connecticut': 0.8, 'Illinois': 0.7,
    'Indiana': 0.7, 'Iowa': 0.7, 'Kansas': 0.6, 'Kentucky': 0.5,
    'Maryland': 0.6, 'Massachusetts': 0.8, 'Missouri': 0.5,
    'Nebraska': 0.6, 'Nevada': 0.7, 'New Jersey': 0.7,
    'New Mexico': 0.6, 'North Carolina': 0.5, 'North Dakota': 0.9,
    'Ohio': 0.7, 'Pennsylvania': 0.8, 'Rhode Island': 0.7,
    'South Dakota': 0.8, 'Virginia': 0.5, 'West Virginia': 0.7,
    'Delaware': 0.5,
    # Heavy discount — warm climates where "winter" features are tourism noise
    'Alabama': 0.15, 'Arizona': 0.15, 'Arkansas': 0.2,
    'Florida': 0.1, 'Georgia': 0.15, 'Hawaii': 0.05,
    'Louisiana': 0.1, 'Mississippi': 0.1, 'Oklahoma': 0.2,
    'South Carolina': 0.15, 'Tennessee': 0.3, 'Texas': 0.15,
    'District of Columbia': 0.4,
}

def _get_winter_climate_mult(state):
    """Return a 0.0-1.0 multiplier for winter feature relevance in this state."""
    if not state:
        return 0.5  # unknown — moderate discount
    return _WINTER_CLIMATE_MULT.get(state.strip(), 0.5)


def score_osm_environment(by_category, feature_count=0, radius_miles=20,
                          urbanization=None, state=None):
    if not by_category:
        return {'score': None, 'feature_score': None, 'category_score': None,
                'density_bonus': None, 'radius_bonus': None,
                'context_mult': None, 'winter_climate_mult': None,
                'urbanization': urbanization,
                'breakdown': [], 'top_features': [], 'has_osm_data': False}

    winter_mult = _get_winter_climate_mult(state)

    breakdown = []
    label_best = {}
    for cat, features in by_category.items():
        for feat in features:
            label   = feat.get('label', '')
            display = feat.get('display', '')
            tier    = _get_tier(label)
            if tier is None: continue
            named, unnamed = _count_named_unnamed(display)
            mult = NAMED_MULTIPLIER if named > 0 else UNNAMED_MULTIPLIER
            # Apply winter climate discount for winter features in warm states
            if cat == 'winter':
                mult *= winter_mult
            if label not in label_best or mult > label_best[label][0]:
                label_best[label] = (mult, named, unnamed, tier, cat)

    raw_pts = 0
    for label, (mult, named, unnamed, tier, cat) in sorted(
            label_best.items(), key=lambda x: -TIER_POINTS[x[1][3]]):
        pts = TIER_POINTS[tier]
        weighted = pts * mult
        raw_pts += weighted
        breakdown.append({'label': label, 'tier': tier, 'cat': cat,
                          'named': named, 'unnamed': unnamed,
                          'base_pts': pts, 'mult': round(mult, 2),
                          'pts': round(weighted, 1)})

    MAX_FEATURE_RAW = 120.0
    feature_score = min(100.0, (raw_pts / MAX_FEATURE_RAW) * 100.0)
    # Also discount the winter category weight for warm states
    effective_cat_weights = dict(CATEGORY_WEIGHTS)
    effective_cat_weights['winter'] = CATEGORY_WEIGHTS['winter'] * winter_mult
    cat_raw = sum(effective_cat_weights.get(cat, 0) for cat in by_category.keys())
    effective_max = sum(effective_cat_weights.values())
    category_score = min(100.0, (cat_raw / max(effective_max, 1)) * 100.0)
    try: fc = max(0, int(feature_count))
    except: fc = 0
    density_score = min(100.0, (math.log1p(fc) / math.log1p(50)) * 100.0)
    try: rm = float(radius_miles)
    except: rm = 20.0
    radius_score = max(0.0, min(100.0, (20.0 - rm) / 20.0 * 100.0))

    # Raw composite before urban context adjustment
    raw_composite = min(100.0, max(0.0,
        feature_score  * 0.55 +
        category_score * 0.30 +
        density_score  * 0.10 +
        radius_score   * 0.05
    ))

    # Apply urban context multiplier — reduces score for dense urban institutions
    # where OSM features within the search radius aren't truly accessible land
    context_mult = get_urban_mult(urbanization) if urbanization else 0.80
    composite = round(raw_composite * context_mult, 1)

    top_features = [b['label'] for b in sorted(breakdown, key=lambda x: (x['tier'], -x['pts']))[:6]]

    return {'score': composite, 'feature_score': round(feature_score, 1),
            'category_score': round(category_score, 1),
            'density_bonus': round(density_score, 1),
            'radius_bonus': round(radius_score, 1),
            'context_mult': round(context_mult, 2),
            'winter_climate_mult': round(winter_mult, 2),
            'urbanization': urbanization,
            'breakdown': breakdown, 'top_features': top_features,
            'has_osm_data': True}

def score_label(score):
    if score is None:  return 'Not Scraped'
    if score >= 85:    return 'Exceptional'
    if score >= 70:    return 'Strong'
    if score >= 55:    return 'Moderate'
    if score >= 35:    return 'Limited'
    return 'Minimal'

def score_color(score):
    if score is None:  return '#3d4f6e'
    if score >= 85:    return '#00b894'
    if score >= 70:    return '#10b981'
    if score >= 55:    return '#ffa502'
    if score >= 35:    return '#ff6b35'
    return '#8b8fa3'

def score_from_osm_row(osm_row, parse_fn, urbanization=None, state=None):
    """
    Convenience wrapper: given a raw OSM CSV row dict and the
    parse_osm_feature_string function, return a full score dict.
    Pass urbanization string from the master CSV for context adjustment.
    Pass state for winter climate discounting.
    """
    feat_str = osm_row.get('osm_env_features', '').strip()
    by_cat   = parse_fn(feat_str)
    count    = osm_row.get('osm_env_count', 0)
    radius   = osm_row.get('osm_env_radius_miles', 20)
    return score_osm_environment(by_cat, feature_count=count,
                                 radius_miles=radius, urbanization=urbanization,
                                 state=state)
    test_by_cat = {
        'winter': [
            {'label': 'Ski Resort',          'display': 'Stowe Mountain Resort'},
            {'label': 'Chair Lift',          'display': 'Summit Chair +3 more'},
        ],
        'water': [
            {'label': 'Lake/Pond',           'display': 'Lake Mansfield, Morrisville Pond'},
            {'label': 'River',               'display': 'Lamoille River'},
        ],
        'outdoor_recreation': [
            {'label': 'Hiking Trail',        'display': 'Long Trail, Monroe Trail +12 more'},
            {'label': 'Mountain Bike Trail', 'display': 'Kingdom Trails +5 more'},
        ],
        'protected_land': [
            {'label': 'Forest',              'display': 'Mount Mansfield State Forest'},
            {'label': 'Nature Reserve',      'display': 'Smugglers Notch State Park'},
        ],
        'natural_features': [
            {'label': 'Mountain Peak',       'display': 'Mount Mansfield, Camel Hump +3 more'},
        ],
        'tourism_leisure': [
            {'label': 'Resort',              'display': 'Stowe Mountain Resort'},
        ],
    }
    r = score_osm_environment(test_by_cat, feature_count=47, radius_miles=12)
    print(f"Strong outdoor site: {r['score']} — {score_label(r['score'])}")
    for b in r['breakdown']:
        print(f"  [{b['tier']}] {b['label']:<25} {b['pts']:5.1f} pts  (x{b['mult']})")

    test_urban = {
        'tourism_leisure': [
            {'label': 'Museum', 'display': 'unnamed Museum +2 more'},
            {'label': 'Park',   'display': 'City Park'},
        ],
        'infrastructure': [
            {'label': 'Train Station', 'display': 'Central Station'},
        ],
    }
    r2 = score_osm_environment(test_urban, feature_count=4, radius_miles=20)
    print(f"\nUrban site: {r2['score']} — {score_label(r2['score'])}")