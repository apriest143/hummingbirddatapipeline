"""
partnership_readiness.py — Hummingbird Partnership Readiness Scorer
=====================================================================
Computes a 0-100 Partnership Readiness Score for each institution
across three axes:

  Axis 1: STATE POLICY ENVIRONMENT (35 pts)
    - Outdoor economy % of state GDP (BEA 2024)         0-15 pts
    - Enrollment cliff severity (WICHE 2024 projections) 0-12 pts
    - Prior closure activity in state 2020-2025          0-8 pts

  Axis 2: FUNDING ENVIRONMENT (30 pts)
    - Rural development program access                    0-10 pts
    - State higher ed funding trend                       0-10 pts
    - Accreditation body flexibility                      0-10 pts

  Axis 3: GOVERNANCE & COMMUNITY URGENCY (35 pts)
    - Institution distress trajectory                     0-15 pts
    - Urbanization / community receptiveness              0-12 pts
    - Institutional distress flags                        0-8 pts

Sources:
  BEA Outdoor Recreation Satellite Account 2024 (released March 5, 2026)
  WICHE Knocking at the College Door, 11th Edition, Dec 2024
  IPEDS master data (already in master CSV)
"""

import math

# ---------------------------------------------------------------------------
# BEA 2024 — Outdoor Recreation Value Added as % of State GDP
# Source: BEA ORSA release March 5, 2026
# National average: 2.4%
# Range: Hawaii 6.1% to DC 1.0%
# ---------------------------------------------------------------------------
BEA_OUTDOOR_GDP_PCT = {
    'Hawaii':          6.1,
    'Alaska':          5.3,
    'Montana':         4.9,
    'Vermont':         4.5,
    'Wyoming':         4.3,
    'Maine':           4.1,
    'Idaho':           3.9,
    'New Mexico':      3.7,
    'Oregon':          3.5,
    'Colorado':        3.4,
    'South Dakota':    3.3,
    'North Dakota':    3.2,
    'Utah':            3.1,
    'West Virginia':   3.0,
    'Arkansas':        2.9,
    'New Hampshire':   2.8,
    'Mississippi':     2.8,
    'Kentucky':        2.7,
    'Tennessee':       2.7,
    'South Carolina':  2.6,
    'Alabama':         2.6,
    'Nevada':          2.6,
    'Wisconsin':       2.5,
    'Minnesota':       2.5,
    'Washington':      2.5,
    'Nebraska':        2.5,
    'Kansas':          2.4,
    'Iowa':            2.4,
    'Missouri':        2.4,
    'Indiana':         2.4,
    'North Carolina':  2.3,
    'Arizona':         2.3,
    'Oklahoma':        2.3,
    'Louisiana':       2.3,
    'Georgia':         2.2,
    'Virginia':        2.2,
    'Michigan':        2.2,
    'Ohio':            2.1,
    'Pennsylvania':    2.1,
    'Texas':           2.1,
    'California':      2.0,
    'Florida':         2.0,
    'Illinois':        1.9,
    'Maryland':        1.9,
    'Massachusetts':   1.8,
    'New Jersey':      1.7,
    'Connecticut':     1.6,
    'New York':        1.6,
    'Delaware':        1.6,
    'Rhode Island':    1.5,
    'District of Columbia': 1.0,
}

# ---------------------------------------------------------------------------
# WICHE 2024 — Projected % change in HS graduates 2023 to 2041
# Source: WICHE Knocking at the College Door 11th Ed, Dec 2024
# Negative = enrollment cliff severity; more negative = higher urgency
# National average: -13%
# ---------------------------------------------------------------------------
WICHE_HS_GRAD_CHANGE_2041 = {
    'Hawaii':         -33,
    'California':     -29,
    'New York':       -27,
    'West Virginia':  -26,
    'Wyoming':        -23,
    'New Mexico':     -21,
    'Oregon':         -19,
    'Pennsylvania':   -17,
    'Mississippi':    -16,
    'Rhode Island':   -15,
    'Vermont':        -15,
    'Alaska':         -14,
    'Maine':          -10,
    'New Hampshire':  -10,
    'Colorado':       -12,
    'Michigan':       -11,
    'Ohio':           -11,
    'Illinois':       -10,
    'Connecticut':    -9,
    'Massachusetts':  -9,
    'Maryland':       -8,
    'Missouri':       -8,
    'Wisconsin':      -8,
    'Indiana':        -7,
    'Minnesota':      -7,
    'Kansas':         -7,
    'Nebraska':       -6,
    'Iowa':           -6,
    'Washington':     -5,
    'Virginia':       -5,
    'Nevada':         -4,
    'Delaware':       -4,
    'New Jersey':     -1,
    'Arizona':        2,
    'Florida':        3,
    'Georgia':        4,
    'North Carolina': 5,
    'Tennessee':      6,
    'South Carolina': 7,
    'Texas':          8,
    'Kentucky':       -3,
    'Alabama':        -2,
    'Louisiana':      -3,
    'Arkansas':       -2,
    'Oklahoma':       1,
    'Utah':           3,
    'Idaho':          6,
    'Montana':        4,
    'North Dakota':   5,
    'South Dakota':   4,
    'District of Columbia': -5,
}

# ---------------------------------------------------------------------------
# Prior closure activity by state 2020-2025
# Based on BestColleges tracking + IPEDS + news sources
# Higher = more closures = more urgency / precedent for HV
# ---------------------------------------------------------------------------
STATE_CLOSURE_COUNT = {
    # High closure states (≥5)
    'Pennsylvania': 12,  # Penn State campuses + private closures
    'New York':     10,
    'California':    8,
    'Massachusetts': 7,
    'Illinois':      6,
    'Ohio':          6,
    'Michigan':      5,
    'Virginia':      5,
    'Iowa':          5,
    # Medium (2-4)
    'Indiana':       4,
    'Wisconsin':     4,
    'Minnesota':     4,
    'Missouri':      4,
    'Connecticut':   4,
    'New Jersey':    3,
    'Texas':         3,
    'Florida':       3,
    'Georgia':       3,
    'North Carolina': 3,
    'Vermont':       3,
    'Maine':         2,
    'Oregon':        2,
    'Washington':    2,
    'Kansas':        2,
    'Nebraska':      2,
    'Montana':       2,
    'Tennessee':     2,
    'Alabama':       2,
    'Kentucky':      2,
    'Oklahoma':      2,
    # Low (1)
    'Colorado':      1,
    'Arizona':       1,
    'New Hampshire': 1,
    'Rhode Island':  1,
    'West Virginia': 1,
    'Maryland':      1,
    'South Carolina': 1,
    'Mississippi':   1,
    'Arkansas':      1,
    'Louisiana':     1,
    'Nevada':        1,
    'Idaho':         1,
    'New Mexico':    1,
    'Hawaii':        1,
}

# ---------------------------------------------------------------------------
# State higher ed funding trend (rough tier based on state budget trajectory)
# Higher = more state support available / better policy environment
# Sources: SHEEO State Higher Education Finance (SHEF) report trends
# ---------------------------------------------------------------------------
STATE_FUNDING_TIER = {
    # Strong funding / growing
    'Washington': 3, 'Colorado': 3, 'Oregon': 3, 'California': 3,
    'Minnesota': 3, 'Maryland': 3, 'Massachusetts': 3, 'Connecticut': 3,
    'New York': 3, 'Utah': 3, 'Idaho': 3, 'Montana': 3,
    'North Dakota': 3, 'South Dakota': 3,
    # Moderate / stable
    'Texas': 2, 'Florida': 2, 'Georgia': 2, 'North Carolina': 2,
    'Virginia': 2, 'Tennessee': 2, 'Indiana': 2, 'Ohio': 2,
    'Wisconsin': 2, 'Michigan': 2, 'Iowa': 2, 'Nebraska': 2,
    'Kansas': 2, 'Missouri': 2, 'Oklahoma': 2, 'Arkansas': 2,
    'South Carolina': 2, 'New Jersey': 2, 'Delaware': 2,
    'Nevada': 2, 'Arizona': 2, 'Hawaii': 2, 'Alaska': 2,
    'New Mexico': 2, 'Wyoming': 2,
    # Declining / constrained
    'Illinois': 1, 'Pennsylvania': 1, 'New Hampshire': 1, 'Vermont': 1,
    'Maine': 1, 'Rhode Island': 1, 'Kentucky': 1, 'West Virginia': 1,
    'Mississippi': 1, 'Louisiana': 1, 'Alabama': 1, 'District of Columbia': 1,
}

# ---------------------------------------------------------------------------
# Accreditation body flexibility (for alternative programming / conversion)
# HLC and SACSCOC have shown more openness to mission change than others
# ---------------------------------------------------------------------------
ACCREDITOR_FLEXIBILITY = {
    # Most flexible — have approved mission conversions
    'Higher Learning Commission':                        3,
    'Southern Association of Colleges and Schools':      3,
    'New England Commission of Higher Education':        2,
    'Middle States Commission on Higher Education':      2,
    'Northwest Commission on Colleges and Universities': 2,
    # Less flexible — specialized focus
    'WASC Senior College and University Commission':     2,
    'Accrediting Commission for Community and Junior Colleges': 2,
    # Specialized accreditors — less relevant for conversion
    'Accrediting Council for Independent Colleges':      1,
    'Accrediting Bureau of Health Education Schools':    1,
    'Council on Occupational Education':                 1,
}

# ---------------------------------------------------------------------------
# Scoring functions
# ---------------------------------------------------------------------------

def score_outdoor_economy(state):
    """0-15 pts based on BEA 2024 outdoor economy % of state GDP"""
    pct = BEA_OUTDOOR_GDP_PCT.get(state, 2.4)  # national avg default
    if pct >= 4.0:  return 15
    if pct >= 3.5:  return 13
    if pct >= 3.0:  return 11
    if pct >= 2.5:  return 9
    if pct >= 2.0:  return 7
    if pct >= 1.7:  return 5
    return 3

def score_enrollment_cliff(state):
    """0-12 pts — more severe projected decline = higher urgency = higher score"""
    chg = WICHE_HS_GRAD_CHANGE_2041.get(state, -13)  # national avg default
    if chg <= -20:  return 12
    if chg <= -15:  return 10
    if chg <= -10:  return 8
    if chg <= -5:   return 6
    if chg <= 0:    return 4
    if chg <= 5:    return 2
    return 0  # strong growth states

def score_prior_closures(state):
    """0-8 pts — prior closures signal precedent + urgency"""
    n = STATE_CLOSURE_COUNT.get(state, 0)
    if n >= 8:  return 8
    if n >= 5:  return 6
    if n >= 3:  return 4
    if n >= 1:  return 2
    return 0

def score_rural_access(urbanization):
    """0-10 pts — rural/town institutions have better access to rural dev programs"""
    urb = (urbanization or '').split(':')[0].strip()
    if urb == 'Rural':   return 10
    if urb == 'Town':    return 7
    if urb == 'Suburb':  return 3
    if urb == 'City':    return 0
    return 5  # unknown

def score_state_funding(state):
    """0-10 pts — state higher ed funding trajectory"""
    tier = STATE_FUNDING_TIER.get(state, 2)
    return {3: 10, 2: 6, 1: 2}.get(tier, 6)

def score_accreditor(accreditor):
    """0-10 pts — accreditor flexibility for mission change"""
    if not accreditor:
        return 5
    for key, val in ACCREDITOR_FLEXIBILITY.items():
        if key.lower() in (accreditor or '').lower():
            return {3: 10, 2: 6, 1: 2}.get(val, 5)
    return 5  # unknown accreditor

def score_distress_trajectory(distress_score, closure_risk):
    """0-15 pts — institution-level distress as urgency signal"""
    ds = distress_score or 0
    cr = (closure_risk or 0) * 100
    # High distress + high closure risk = high urgency = high readiness signal
    combined = ds * 0.6 + cr * 0.4
    if combined >= 60:  return 15
    if combined >= 45:  return 12
    if combined >= 30:  return 9
    if combined >= 20:  return 6
    if combined >= 10:  return 3
    return 1

def score_community_context(urbanization, distress_category):
    """0-12 pts — community receptiveness + location fit"""
    urb = (urbanization or '').split(':')[0].strip()
    # Rural and small town communities more receptive to transformation
    urb_pts = {'Rural': 12, 'Town': 9, 'Suburb': 5, 'City': 2}.get(urb, 6)
    # Distressed institutions more likely to engage
    dist_bonus = {'Critical': 2, 'High': 2, 'Moderate': 1}.get(distress_category or '', 0)
    return min(12, urb_pts + dist_bonus)

def score_distress_flags(flags):
    """0-8 pts — specific distress flags as urgency signals"""
    pts = 0
    if flags.get('neg_net_worth'):     pts += 3
    if flags.get('enrollment_decline'): pts += 2
    if flags.get('operating_loss'):    pts += 2
    if flags.get('high_cr'):           pts += 1
    return min(8, pts)


def compute_partnership_score(r):
    """
    Compute the full Partnership Readiness Score for one institution row.
    Returns a dict with total score, axis scores, and component breakdown.
    """
    state       = r.get('state', '')
    urbanization= r.get('urbanization', '')
    accreditor  = r.get('accreditation_agency', '') or r.get('accreditor', '')
    dist_score  = _fv(r, 'distress_score')
    closure_risk= _fv(r, 'closure_risk_score')
    dist_cat    = r.get('distress_category', '')
    source      = r.get('data_source', '')

    flags = {
        'neg_net_worth':     _flag(r, 'flag_negative_net_worth') or _flag(r, 'flag_990_negative_net_assets'),
        'enrollment_decline':_flag(r, 'flag_enrollment_decline'),
        'operating_loss':    _flag(r, 'flag_operating_losses') or _flag(r, 'flag_990_operating_loss'),
        'high_cr':           (closure_risk or 0) > 0.3,
    }

    # ── AXIS 1: State Policy Environment (35 pts) ─────────────────────────
    s_outdoor   = score_outdoor_economy(state)
    s_cliff     = score_enrollment_cliff(state)
    s_closures  = score_prior_closures(state)
    axis1 = s_outdoor + s_cliff + s_closures

    # ── AXIS 2: Funding Environment (30 pts) ─────────────────────────────
    s_rural     = score_rural_access(urbanization)
    s_funding   = score_state_funding(state)
    s_accred    = score_accreditor(accreditor)
    axis2 = s_rural + s_funding + s_accred

    # ── AXIS 3: Governance & Community Urgency (35 pts) ──────────────────
    s_traj      = score_distress_trajectory(dist_score, closure_risk)
    s_community = score_community_context(urbanization, dist_cat)
    s_flags     = score_distress_flags(flags)
    axis3 = s_traj + s_community + s_flags

    total = axis1 + axis2 + axis3
    total = round(min(100, max(0, total)))

    return {
        'partnership_readiness_score': total,
        'pr_state_policy_score':       axis1,
        'pr_funding_score':            axis2,
        'pr_governance_score':         axis3,
        'pr_outdoor_economy_pts':      s_outdoor,
        'pr_enrollment_cliff_pts':     s_cliff,
        'pr_closure_activity_pts':     s_closures,
        'pr_rural_access_pts':         s_rural,
        'pr_state_funding_pts':        s_funding,
        'pr_accreditor_pts':           s_accred,
        'pr_distress_traj_pts':        s_traj,
        'pr_community_pts':            s_community,
        'pr_flags_pts':                s_flags,
        'pr_bea_outdoor_pct':          round(BEA_OUTDOOR_GDP_PCT.get(state, 2.4), 1),
        'pr_wiche_cliff_pct':          WICHE_HS_GRAD_CHANGE_2041.get(state, -13),
        'pr_state_closures':           STATE_CLOSURE_COUNT.get(state, 0),
        'pr_label':                    _pr_label(total),
    }


def _pr_label(score):
    if score >= 75: return 'Excellent'
    if score >= 60: return 'Strong'
    if score >= 45: return 'Moderate'
    if score >= 30: return 'Limited'
    return 'Low'


def _fv(r, col):
    v = r.get(col, '')
    if v in ('', 'nan', 'NaN', 'None', None): return None
    try:
        x = float(v)
        return None if (math.isnan(x) or math.isinf(x)) else x
    except: return None


def _flag(r, col):
    return str(r.get(col, '')).strip().lower() in ('1', 'true', 'yes', '1.0')


# ---------------------------------------------------------------------------
# Batch scoring
# ---------------------------------------------------------------------------
def score_all(rows):
    """Score all rows and add partnership readiness columns."""
    results = []
    for r in rows:
        scores = compute_partnership_score(r)
        row_copy = dict(r)
        row_copy.update(scores)
        results.append(row_copy)
    return results


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    test_cases = [
        {
            'institution_name': 'Vermont Rural College (test)',
            'state': 'Vermont',
            'urbanization': 'Rural: Remote',
            'accreditation_agency': 'New England Commission of Higher Education',
            'distress_score': '72',
            'closure_risk_score': '0.55',
            'distress_category': 'High',
            'data_source': 'IPEDS',
            'flag_negative_net_worth': '1',
            'flag_enrollment_decline': '1',
            'flag_operating_losses': '1',
        },
        {
            'institution_name': 'NYC Urban College (test)',
            'state': 'New York',
            'urbanization': 'City: Large',
            'accreditation_agency': 'Middle States Commission on Higher Education',
            'distress_score': '25',
            'closure_risk_score': '0.12',
            'distress_category': 'Low',
            'data_source': 'IPEDS',
            'flag_negative_net_worth': '0',
            'flag_enrollment_decline': '0',
            'flag_operating_losses': '0',
        },
        {
            'institution_name': 'Montana Small Town College (test)',
            'state': 'Montana',
            'urbanization': 'Town: Distant',
            'accreditation_agency': 'Northwest Commission on Colleges and Universities',
            'distress_score': '55',
            'closure_risk_score': '0.38',
            'distress_category': 'High',
            'data_source': 'IPEDS',
            'flag_negative_net_worth': '0',
            'flag_enrollment_decline': '1',
            'flag_operating_losses': '1',
        },
        {
            'institution_name': 'Colorado Suburban Campus (test)',
            'state': 'Colorado',
            'urbanization': 'Suburb: Large',
            'accreditation_agency': 'Higher Learning Commission',
            'distress_score': '40',
            'closure_risk_score': '0.25',
            'distress_category': 'Moderate',
            'data_source': 'IPEDS',
            'flag_negative_net_worth': '1',
            'flag_enrollment_decline': '0',
            'flag_operating_losses': '1',
        },
    ]

    print("Partnership Readiness Score — Self Test")
    print("=" * 70)
    for t in test_cases:
        s = compute_partnership_score(t)
        print(f"\n{t['institution_name']}")
        print(f"  State: {t['state']} | Urb: {t['urbanization']}")
        print(f"  BEA outdoor GDP: {s['pr_bea_outdoor_pct']}% | WICHE cliff: {s['pr_wiche_cliff_pct']}% | Closures: {s['pr_state_closures']}")
        print(f"  Axis 1 (State Policy):   {s['pr_state_policy_score']:3d}/35  "
              f"[outdoor={s['pr_outdoor_economy_pts']} cliff={s['pr_enrollment_cliff_pts']} closures={s['pr_closure_activity_pts']}]")
        print(f"  Axis 2 (Funding):        {s['pr_funding_score']:3d}/30  "
              f"[rural={s['pr_rural_access_pts']} funding={s['pr_state_funding_pts']} accred={s['pr_accreditor_pts']}]")
        print(f"  Axis 3 (Gov/Urgency):    {s['pr_governance_score']:3d}/35  "
              f"[traj={s['pr_distress_traj_pts']} community={s['pr_community_pts']} flags={s['pr_flags_pts']}]")
        print(f"  ─────────────────────────────────────")
        print(f"  TOTAL: {s['partnership_readiness_score']:3d}/100  → {s['pr_label']}")