"""
summary_generator.py — Hummingbird Market Summary Dashboard
============================================================
Generates summary.html. Called by master_standalone.py or run directly:
    python hv_master_data/data/summary_generator.py
"""
import os, sys, csv, json, math, datetime, argparse, re, importlib.util as _ilu
from collections import Counter, defaultdict

DEFAULT_MASTER = 'hv_master_data/data/Hummingbird_Master_Combined_v6.csv'
DEFAULT_OSM    = 'hv_master_data/acreage_scripts/osm_land_results.csv'
DEFAULT_OUT    = 'summary.html'

# ── Load osm_scorer.py (shared with master_standalone / clusters) ──────────
_osm_scorer = None
for _p in [
    os.path.join(os.path.dirname(__file__), 'osm_scorer.py'),
    os.path.join(os.path.dirname(__file__), '..', 'data', 'osm_scorer.py'),
    'osm_scorer.py',
    'hv_master_data/data/osm_scorer.py',
    'hv_master_data/acreage_scripts/osm_scorer.py',
]:
    if os.path.exists(_p):
        _spec = _ilu.spec_from_file_location('osm_scorer', _p)
        _osm_scorer = _ilu.module_from_spec(_spec)
        _spec.loader.exec_module(_osm_scorer)
        print(f'  Loaded osm_scorer from {_p}')
        break
if _osm_scorer is None:
    print("  Warning: osm_scorer.py not found — environment scores will be blank")

# ── OSM feature label→category mapping (mirrors master_standalone.py) ──────
_FEATURES = [
    ("Coastline","water"),("Lake/Pond","water"),("River","water"),("Beach","water"),
    ("Canal","water"),("Stream","water"),("Wetland","water"),("Marina","water"),
    ("Natural Spring","water"),("Hot Spring","water"),
    ("Ski Resort","winter"),("Winter Sports Area","winter"),("Downhill Ski Run","winter"),
    ("Nordic Ski Trail","winter"),("Gondola","winter"),("Chair Lift","winter"),
    ("Drag Lift","winter"),("Cable Car","winter"),("Ice Skating","winter"),
    ("Skiing Facility","winter"),("Ice Hockey Rink","winter"),
    ("Hiking Trail","outdoor_recreation"),("Mountain Bike Trail","outdoor_recreation"),
    ("Cycling Route","outdoor_recreation"),("Equestrian Trail","outdoor_recreation"),
    ("Canoe Route","outdoor_recreation"),("Dedicated Cycleway","outdoor_recreation"),
    ("Bridleway","outdoor_recreation"),("Rock Climbing","outdoor_recreation"),
    ("Surfing","outdoor_recreation"),("Kayaking","outdoor_recreation"),
    ("Rowing","outdoor_recreation"),("Fishing","outdoor_recreation"),
    ("Hunting","outdoor_recreation"),("Golf","outdoor_recreation"),
    ("Golf Course","outdoor_recreation"),("Sports Centre","outdoor_recreation"),
    ("Horse Riding","outdoor_recreation"),("Swimming Facility","outdoor_recreation"),
    ("Campground","outdoor_recreation"),("Wilderness Hut","outdoor_recreation"),
    ("National Park","protected_land"),("Protected Area","protected_land"),
    ("Nature Reserve","protected_land"),("Forest","protected_land"),
    ("National Forest","protected_land"),("Conservation Land","protected_land"),
    ("Woodland","natural_features"),("Mountain Peak","natural_features"),
    ("Volcano","natural_features"),("Cliff","natural_features"),
    ("Cave","natural_features"),("Glacier","natural_features"),
    ("Scrubland","natural_features"),("Heathland","natural_features"),
    ("Resort","tourism_leisure"),("Theme Park","tourism_leisure"),
    ("Zoo","tourism_leisure"),("Aquarium","tourism_leisure"),
    ("Museum","tourism_leisure"),("Tourist Attraction","tourism_leisure"),
    ("Scenic Viewpoint","tourism_leisure"),("Historic Monument","tourism_leisure"),
    ("Park","tourism_leisure"),("Botanical Garden","tourism_leisure"),
    ("Casino","tourism_leisure"),
    ("Airport","infrastructure"),("Train Station","infrastructure"),
    ("Ferry Terminal","infrastructure"),
]
_LABEL_TO_CAT = {f[0]: f[1] for f in _FEATURES}

def _parse_osm_feature_string(feat_str):
    """Lightweight parser — just enough to feed osm_scorer."""
    by_category = {}
    if not feat_str or feat_str.strip() in ('', 'ERROR'):
        return by_category
    for part in feat_str.split(' | '):
        part = part.strip()
        if not part: continue
        label, display = '', part
        if part.endswith(']') and '[' in part:
            bracket = part.rfind('[')
            label   = part[bracket+1:-1].strip()
            display = part[:bracket].strip()
        cat = _LABEL_TO_CAT.get(label, 'other')
        if cat == 'other': continue
        by_category.setdefault(cat, []).append({'label': label, 'display': display})
    return by_category

def build_env_score_lookup(osm_path, urb_lookup=None):
    """
    Load osm_land_results.csv and compute environment scores.
    Returns dict keyed by name||city||state → float score (0-100).
    """
    if not osm_path or not os.path.exists(osm_path):
        print(f'  OSM env file not found: {osm_path} — env scores will be blank')
        return {}
    if _osm_scorer is None:
        return {}

    csv.field_size_limit(10 * 1024 * 1024)
    lookup = {}
    with open(osm_path, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            name    = row.get('institution_name', '').strip()
            queried = row.get('osm_env_queried_at', '').strip()
            feat_str = row.get('osm_env_features', '').strip()
            if not name or not queried or feat_str == 'ERROR':
                continue
            by_cat = _parse_osm_feature_string(feat_str)
            _count_str = str(row.get('osm_env_count', '0') or '0').strip()
            _count = int(_count_str) if _count_str.lstrip('-').isdigit() else 0
            _radius = row.get('osm_env_radius_miles', '20')
            city  = row.get('city', '').strip()
            state = row.get('state', '').strip()
            key   = f"{name}||{city}||{state}"
            _urb  = (urb_lookup or {}).get(key)
            result = _osm_scorer.score_osm_environment(
                by_cat, feature_count=_count, radius_miles=_radius,
                urbanization=_urb
            )
            lookup[key] = result.get('score')  # float or None
    print(f'  OSM env scores: {sum(1 for v in lookup.values() if v is not None):,} scored')
    return lookup

def fv(r,c):
    v=r.get(c,'')
    if v in ('','nan','NaN','None',None): return None
    try: x=float(v); return None if (math.isnan(x) or math.isinf(x)) else x
    except: return None
def flag(r,c): return str(r.get(c,'')).strip().lower() in ('1','true','yes','1.0')
def med(vals):
    vals=[v for v in vals if v is not None]
    if not vals: return None
    vals.sort(); return vals[len(vals)//2]
def avg(vals):
    vals=[v for v in vals if v is not None]
    return sum(vals)/len(vals) if vals else None
def pct(n,d): return round(n/max(d,1)*100,1)
def get_acres(r): return fv(r,'acreage_primary') or fv(r,'scraper_acres') or fv(r,'osm_acres')
def fmt_m(v): return f"${round(v/1e6,1)}M" if v else "—"
def fmt_b(v): return f"${round(v/1e9,1)}B" if v else "—"
def fmt_n(v, decimals=1): return f"{round(v,decimals):,}" if v is not None else "—"

def _env(r, env_lookup):
    """Get OSM environment score for a row from the lookup."""
    key = f"{r.get('institution_name','').strip()}||{r.get('city','').strip()}||{r.get('state','').strip()}"
    return env_lookup.get(key)

def _env_label(score):
    """Human label for OSM environment score."""
    if score is None:  return '—'
    if score >= 85:    return 'Exceptional'
    if score >= 70:    return 'Strong'
    if score >= 55:    return 'Moderate'
    if score >= 35:    return 'Limited'
    return 'Minimal'


def compute_stats(rows, env_lookup=None):
    if env_lookup is None: env_lookup = {}
    ipeds = [r for r in rows if r.get('data_source')=='IPEDS']
    hb990 = [r for r in rows if r.get('data_source')=='Hummingbird_990']

    # ── IPEDS distress breakdown ──────────────────────────────────────────
    dist_order = ['Critical','High','Moderate','Low','Healthy']
    ipeds_dist = []
    for cat in dist_order:
        rs = [r for r in ipeds if r.get('distress_category')==cat]
        if not rs: continue
        acres_vals = [get_acres(r) for r in rs if get_acres(r)]
        ipeds_dist.append({
            'cat': cat, 'count': len(rs),
            'pct': pct(len(rs), len(ipeds)),
            'avg_distress': round(avg([fv(r,'distress_score') for r in rs]) or 0, 1),
            'avg_revenue_m': round((avg([fv(r,'revenue_2024') for r in rs]) or 0)/1e6, 1),
            'avg_enroll': round(avg([fv(r,'enrollment_2024') for r in rs]) or 0),
            'avg_cr': round(avg([fv(r,'closure_risk_score') for r in rs]) or 0, 3),
            'avg_acres': round(avg(acres_vals) or 0),
            'land_potential': sum(1 for r in rs if flag(r,'flag_land_potential')),
            'avg_op_margin': round(avg([fv(r,'operating_margin') for r in rs]) or 0, 1),
            'avg_env_score': round(avg([_env(r,env_lookup) for r in rs if _env(r,env_lookup) is not None]) or 0, 1),
        })

    # ── IPEDS ML risk tiers ───────────────────────────────────────────────
    risk_order = ['Critical','High','Elevated','Moderate','Low']
    ipeds_risk = []
    for tier in risk_order:
        rs = [r for r in ipeds if r.get('risk_tier')==tier]
        if not rs: continue
        ipeds_risk.append({
            'tier': tier, 'count': len(rs),
            'pct': pct(len(rs), len(ipeds)),
            'avg_cr': round(avg([fv(r,'closure_risk_score') for r in rs]) or 0, 3),
            'avg_distress': round(avg([fv(r,'distress_score') for r in rs]) or 0, 1),
            'avg_revenue_m': round((avg([fv(r,'revenue_2024') for r in rs]) or 0)/1e6, 1),
            'avg_enroll': round(avg([fv(r,'enrollment_2024') for r in rs]) or 0),
            'avg_acres': round(avg([get_acres(r) for r in rs if get_acres(r)]) or 0),
            'avg_enr_trend': round(avg([fv(r,'enrollment_2yr_pct') for r in rs]) or 0, 1),
            'land_potential': sum(1 for r in rs if flag(r,'flag_land_potential')),
            'avg_env_score': round(avg([_env(r,env_lookup) for r in rs if _env(r,env_lookup) is not None]) or 0, 1),
        })
    no_ml = sum(1 for r in ipeds if not r.get('risk_tier','').strip())
    if no_ml:
        ipeds_risk.append({'tier':'No ML Score','count':no_ml,'pct':pct(no_ml,len(ipeds)),
                           'avg_cr':None,'avg_distress':None,'avg_revenue_m':None,'avg_enroll':None,
                           'avg_acres':None,'avg_enr_trend':None,'land_potential':None,'avg_env_score':None})

    # ── IPEDS by type ─────────────────────────────────────────────────────
    by_type_ipeds = defaultdict(list)
    for r in ipeds: by_type_ipeds[r.get('institution_type','Unknown')].append(r)
    ipeds_types = []
    for t, rs in sorted(by_type_ipeds.items(), key=lambda x:-len(x[1])):
        if len(rs) < 2: continue
        acres_list = [get_acres(r) for r in rs if get_acres(r)]
        ipeds_types.append({
            'type': t, 'count': len(rs),
            'pct': pct(len(rs), len(ipeds)),
            'total_assets_b': round(sum(fv(r,'assets_2024') for r in rs if fv(r,'assets_2024'))/1e9, 1),
            'total_liab_b': round(sum(fv(r,'liabilities_2024') for r in rs if fv(r,'liabilities_2024'))/1e9, 1),
            'total_rev_b': round(sum(fv(r,'revenue_2024') for r in rs if fv(r,'revenue_2024'))/1e9, 1),
            'total_acres': round(sum(acres_list)),
            'w_acres': len(acres_list),
            'avg_distress': round(avg([fv(r,'distress_score') for r in rs]) or 0, 1),
            'med_enroll': round(med([fv(r,'enrollment_2024') for r in rs]) or 0),
            'crit_high': sum(1 for r in rs if r.get('distress_category') in ('Critical','High')),
            'avg_op_margin': round(avg([fv(r,'operating_margin') for r in rs]) or 0, 1),
            'avg_pr_score': round(avg([fv(r,'partnership_readiness_score') for r in rs]) or 0, 1),
            'high_land': sum(1 for r in rs if flag(r,'flag_high_land_potential')),
            'avg_env_score': round(avg([_env(r,env_lookup) for r in rs if _env(r,env_lookup) is not None]) or 0, 1),
        })

    # ── 990 by type ───────────────────────────────────────────────────────
    by_type_990 = defaultdict(list)
    for r in hb990: by_type_990[r.get('institution_type','Unknown')].append(r)
    n990_types = []
    for t, rs in sorted(by_type_990.items(), key=lambda x:-len(x[1])):
        acres_list = [get_acres(r) for r in rs if get_acres(r)]
        n990_types.append({
            'type': t, 'count': len(rs),
            'pct': pct(len(rs), len(hb990)),
            'total_assets_m': round(sum(fv(r,'assets_2024') for r in rs if fv(r,'assets_2024'))/1e6),
            'total_liab_m': round(sum(fv(r,'liabilities_2024') for r in rs if fv(r,'liabilities_2024'))/1e6),
            'total_rev_m': round(sum(fv(r,'revenue_2024') for r in rs if fv(r,'revenue_2024'))/1e6),
            'total_acres': round(sum(acres_list)),
            'w_acres': len(acres_list),
            'avg_distress': round(avg([fv(r,'distress_score') for r in rs]) or 0, 1),
            'med_rev_k': round((med([fv(r,'revenue_2024') for r in rs]) or 0)/1000),
            'pct_op_loss': round(pct(sum(1 for r in rs if flag(r,'flag_990_operating_loss')), len(rs))),
            'pct_neg_ast': round(pct(sum(1 for r in rs if flag(r,'flag_990_negative_net_assets')), len(rs))),
        })

    # ── Top 50 distress ───────────────────────────────────────────────────
    top50_ipeds = []
    for r in sorted(ipeds, key=lambda r:-(fv(r,'distress_score') or 0))[:50]:
        top50_ipeds.append({
            'name': r.get('institution_name',''), 'city': r.get('city',''),
            'state': r.get('state',''), 'type': r.get('institution_type',''),
            'distress': round(fv(r,'distress_score') or 0, 1),
            'category': r.get('distress_category',''),
            'cr': round(fv(r,'closure_risk_score') or 0, 3),
            'risk_tier': r.get('risk_tier',''),
            'enrollment': round(fv(r,'enrollment_2024') or 0),
            'revenue_m': round((fv(r,'revenue_2024') or 0)/1e6, 1),
            'acres': round(get_acres(r) or 0),
            'urbanization': r.get('urbanization',''),
            'op_margin': round(fv(r,'operating_margin') or 0, 1),
            'enr_trend': round(fv(r,'enrollment_2yr_pct') or 0, 1),
            'pr_label': r.get('pr_label',''),
            'land_flag': flag(r,'flag_land_potential'),
            'env_score': _env(r, env_lookup),
        })

    top50_990 = []
    for r in sorted(hb990, key=lambda r:-(fv(r,'distress_score') or 0))[:50]:
        top50_990.append({
            'name': r.get('institution_name',''), 'city': r.get('city',''),
            'state': r.get('state',''), 'type': r.get('institution_type',''),
            'distress': round(fv(r,'distress_score') or 0, 1),
            'category': r.get('distress_category',''),
            'revenue_k': round((fv(r,'revenue_2024') or 0)/1000),
            'acres': round(get_acres(r) or 0),
            'pct_neg_ast': flag(r,'flag_990_negative_net_assets'),
            'pct_op_loss': flag(r,'flag_990_operating_loss'),
            'ppe_k': round((fv(r,'plant_property_equipment') or 0)/1000),
        })

    # ── Full acreage table (IPEDS) ────────────────────────────────────────
    acreage_rows = []
    for r in sorted(ipeds, key=lambda r:-(get_acres(r) or 0)):
        a = get_acres(r)
        if not a: continue
        acreage_rows.append({
            'name': r.get('institution_name',''), 'city': r.get('city',''),
            'state': r.get('state',''), 'type': r.get('institution_type',''),
            'acres': round(a, 1),
            'distress': round(fv(r,'distress_score') or 0, 1),
            'category': r.get('distress_category',''),
            'cr': round(fv(r,'closure_risk_score') or 0, 3),
            'enrollment': round(fv(r,'enrollment_2024') or 0),
            'revenue_m': round((fv(r,'revenue_2024') or 0)/1e6, 1),
            'urbanization': r.get('urbanization',''),
            'source': r.get('acreage_source','scraper'),
            'pr_label': r.get('pr_label',''),
            'land_flag': flag(r,'flag_land_potential'),
            'env_score': _env(r, env_lookup),
        })

    # ── Aggregate numbers ─────────────────────────────────────────────────
    ipeds_crit = [r for r in ipeds if r.get('distress_category') in ('Critical','High')]
    hb990_crit = [r for r in hb990 if r.get('distress_category') in ('Critical','High')]
    ia = sum(get_acres(r) for r in ipeds if get_acres(r))
    ha = sum(get_acres(r) for r in hb990 if get_acres(r))
    # Land-rich distressed: institutions with >=50 ac AND Critical/High distress
    land_distressed = sum(1 for r in ipeds_crit if flag(r,'flag_land_potential'))
    high_land_distressed = sum(1 for r in ipeds_crit if flag(r,'flag_high_land_potential'))
    # Avg env score across all IPEDS with data
    all_env = [_env(r,env_lookup) for r in ipeds if _env(r,env_lookup) is not None]
    avg_env_all = round(avg(all_env) or 0, 1) if all_env else None

    return {
        'run_date': datetime.datetime.now().strftime('%B %d, %Y'),
        'total': len(rows), 'n_ipeds': len(ipeds), 'n_990': len(hb990),
        'ipeds_enr_total_m': round(sum(fv(r,'enrollment_2024') for r in ipeds if fv(r,'enrollment_2024'))/1e6, 1),
        'ipeds_rev_total_b': round(sum(fv(r,'revenue_2024') for r in ipeds if fv(r,'revenue_2024'))/1e9, 1),
        'ipeds_exp_total_b': round(sum(fv(r,'expenses_2024') for r in ipeds if fv(r,'expenses_2024'))/1e9, 1),
        'ipeds_acres_total': round(ia), 'ipeds_w_acres': sum(1 for r in ipeds if get_acres(r)),
        'ipeds_acres_pct': round(pct(sum(1 for r in ipeds if get_acres(r)), len(ipeds))),
        'ipeds_crit_count': len(ipeds_crit), 'ipeds_crit_pct': round(pct(len(ipeds_crit), len(ipeds))),
        'n990_rev_total_b': round(sum(fv(r,'revenue_2024') for r in hb990 if fv(r,'revenue_2024'))/1e9, 2),
        'n990_acres_total': round(ha), 'n990_w_acres': sum(1 for r in hb990 if get_acres(r)),
        'n990_crit_count': len(hb990_crit), 'n990_crit_pct': round(pct(len(hb990_crit), len(hb990))),
        'combined_acres': round(ia+ha), 'combined_crit': len(ipeds_crit)+len(hb990_crit),
        'combined_dist_acres': round(sum(get_acres(r) for r in ipeds_crit if get_acres(r)) + ha),
        'land_distressed': land_distressed, 'high_land_distressed': high_land_distressed,
        'avg_env_all': avg_env_all, 'n_env_scored': len(all_env),
        'ipeds_dist': ipeds_dist, 'ipeds_risk': ipeds_risk,
        'ipeds_types': ipeds_types, 'n990_types': n990_types,
        'top50_ipeds': top50_ipeds, 'top50_990': top50_990,
        'acreage_rows': acreage_rows,
    }


# ── HTML helpers ──────────────────────────────────────────────────────────

CSS = """
:root{--bg:#0f1117;--surface:#181a20;--surface2:#1e2028;--border:#2a2d37;
--text:#e2e4e9;--text2:#8b8fa3;--accent:#6c5ce7;--accent2:#a29bfe;--hb990:#00b894;
--font:'DM Sans',-apple-system,sans-serif;--mono:'JetBrains Mono',monospace;}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
html,body{background:var(--bg);color:var(--text);font-family:var(--font);font-size:14px;}
nav{position:sticky;top:0;z-index:100;display:flex;align-items:center;gap:14px;
    padding:0 24px;height:52px;background:var(--surface);border-bottom:1px solid var(--border);}
.nav-logo{font-size:16px;font-weight:700;letter-spacing:-.5px;
  background:linear-gradient(135deg,var(--accent2),#74b9ff);
  -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;}
.nav-sep{width:1px;height:18px;background:var(--border);}
.nav-spacer{flex:1;}
.nav-back{padding:5px 12px;border-radius:6px;background:var(--surface2);
  border:1px solid var(--border);color:var(--text2);font-size:11px;font-weight:600;
  text-decoration:none;margin-left:6px;}
.nav-back:hover{border-color:var(--accent);color:var(--text);}
.page{max-width:1280px;margin:0 auto;padding:28px 24px 80px;}
.tabs{display:flex;gap:0;margin-bottom:24px;border-bottom:1px solid var(--border);}
.tab{padding:8px 20px;border:1px solid transparent;border-bottom:none;border-radius:6px 6px 0 0;
  background:transparent;color:var(--text2);font-family:var(--font);font-size:12px;
  font-weight:600;cursor:pointer;transition:all .15s;margin-bottom:-1px;}
.tab.active{background:var(--surface);border-color:var(--border);
  border-bottom-color:var(--surface);color:var(--text);}
.tab:hover:not(.active){color:var(--text);}
.panel{display:none;} .panel.active{display:block;}
.section{margin-bottom:28px;}
.section-head{font-family:var(--mono);font-size:10px;color:var(--text2);
  text-transform:uppercase;letter-spacing:1px;margin-bottom:12px;
  padding-bottom:8px;border-bottom:1px solid var(--border);}
.metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:24px;}
.metrics-3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-bottom:24px;}
.metric{background:var(--surface2);border-radius:8px;padding:14px;}
.metric.accent{background:rgba(108,92,231,.1);border:1px solid rgba(108,92,231,.2);}
.metric.green{background:rgba(0,184,148,.08);border:1px solid rgba(0,184,148,.2);}
.metric.red{background:rgba(226,75,74,.08);border:1px solid rgba(226,75,74,.2);}
.metric.hb990{background:rgba(0,184,148,.08);border:1px solid rgba(0,184,148,.3);}
.metric-label{font-size:11px;color:var(--text2);margin-bottom:4px;}
.metric-value{font-size:22px;font-weight:600;line-height:1.1;}
.metric-sub{font-size:10px;color:var(--text2);margin-top:3px;}
.callout{padding:14px 18px;border-radius:8px;border-left:3px solid var(--hb990);
  background:rgba(0,184,148,.06);margin-bottom:24px;
  display:flex;gap:40px;align-items:flex-end;flex-wrap:wrap;}
.callout-val{font-size:30px;font-weight:700;color:#00b894;}
.callout-lbl{font-size:11px;color:#a8e6da;}
.tbl-wrap{overflow-x:auto;border-radius:8px;border:1px solid var(--border);}
.tbl-wrap::-webkit-scrollbar{height:4px;}
.tbl-wrap::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px;}
table{width:100%;border-collapse:collapse;font-size:11px;}
thead th{padding:8px 10px;text-align:left;font-family:var(--mono);font-size:9px;
  color:var(--text2);text-transform:uppercase;letter-spacing:.6px;
  border-bottom:1px solid var(--border);background:var(--surface2);
  white-space:nowrap;cursor:pointer;user-select:none;position:sticky;top:0;}
thead th:hover{color:var(--text);}
tbody td{padding:7px 10px;border-bottom:1px solid rgba(42,45,55,.4);vertical-align:middle;}
tbody tr:hover td{background:rgba(255,255,255,.015);}
tbody tr:last-child td{border-bottom:none;}
.mono{font-family:var(--mono);}
.pill{display:inline-block;padding:2px 7px;border-radius:3px;
  font-family:var(--mono);font-size:9px;font-weight:600;}
.pill-critical{background:rgba(255,71,87,.15);color:#ff4757;}
.pill-high{background:rgba(255,107,53,.15);color:#ff6b35;}
.pill-moderate{background:rgba(255,165,2,.15);color:#ffa502;}
.pill-low{background:rgba(59,130,246,.15);color:#74b9ff;}
.pill-healthy{background:rgba(16,185,129,.15);color:#10b981;}
.pill-elevated{background:rgba(186,117,23,.15);color:#ffa502;}
.tbl-search{width:100%;padding:8px 12px;background:var(--surface2);
  border:1px solid var(--border);border-radius:6px 6px 0 0;
  color:var(--text);font-family:var(--font);font-size:11px;outline:none;
  border-bottom:none;}
.tbl-search:focus{border-color:var(--accent);}
.tbl-count{font-family:var(--mono);font-size:10px;color:var(--text2);
  padding:6px 12px;background:var(--surface2);border-bottom:1px solid var(--border);}
.tbl-scroll{max-height:520px;overflow-y:auto;}
.tbl-scroll::-webkit-scrollbar{width:4px;}
.tbl-scroll::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px;}
.footer{font-size:10px;color:var(--text2);text-align:right;margin-top:32px;
  padding-top:16px;border-top:1px solid var(--border);}
@media(max-width:768px){.metrics{grid-template-columns:repeat(2,1fr);}
  .metrics-3{grid-template-columns:1fr 1fr;}}
"""

def metric(label, value, sub='', cls=''):
    vc = {'accent':'#a29bfe','green':'#00b894','red':'#ff6b35','hb990':'#00b894'}.get(cls,'var(--text)')
    return (f'<div class="metric {cls}"><div class="metric-label">{label}</div>'
            f'<div class="metric-value" style="color:{vc}">{value}</div>'
            f'<div class="metric-sub">{sub}</div></div>')

def th(label, col=''):
    return f'<th data-col="{col}">{label}</th>'

def pill(cat):
    c = (cat or '').lower().replace(' ','').replace('/','')
    return f'<span class="pill pill-{c}">{cat}</span>'

def section(title, body):
    return f'<div class="section"><div class="section-head">{title}</div>{body}</div>'


def build_html(s):
    run_date = s['run_date']

    # ── SHARED: callout bar ───────────────────────────────────────────────
    callout = f'''<div class="callout">
      <div><div class="callout-val">{s["combined_crit"]:,}</div><div class="callout-lbl">critical/high distress institutions</div></div>
      <div><div class="callout-val">{s["combined_acres"]:,}</div><div class="callout-lbl">total acres tracked</div></div>
      <div><div class="callout-val">{s["combined_dist_acres"]:,}</div><div class="callout-lbl">acres held by distressed institutions</div></div>
      <div><div class="callout-val">{s["land_distressed"]:,}</div><div class="callout-lbl">distressed with developable land (≥50 ac)</div></div>
    </div>'''

    # ── ALL tab ───────────────────────────────────────────────────────────
    panel_all = f'''
    <div class="metrics">
      {metric("Total institutions", f"{s['total']:,}", "IPEDS + IRS Form 990", "accent")}
      {metric("Higher education (IPEDS)", f"{s['n_ipeds']:,}", "Colleges &amp; universities")}
      {metric("Nonprofits (990)", f"{s['n_990']:,}", "Faith, recreation, housing, more")}
      {metric("Critical/High distress", f"{s['combined_crit']:,}", "Across both datasets", "red")}
    </div>
    <div class="metrics-3">
      {metric("Combined acreage", f"{s['combined_acres']:,}", "acres across all institutions", "green")}
      {metric("IPEDS acreage", f"{s['ipeds_acres_total']:,}", f"{s['ipeds_w_acres']:,} institutions ({s['ipeds_acres_pct']}%)")}
      {metric("Nonprofit acreage", f"{s['n990_acres_total']:,}", f"{s['n990_w_acres']:,} institutions")}
    </div>'''

    # ── IPEDS tab ─────────────────────────────────────────────────────────
    # Distress table
    dist_rows = ''.join(
        f'''<tr>
          <td>{pill(d["cat"])}</td>
          <td class="mono">{d["count"]:,}</td>
          <td class="mono">{d["pct"]}%</td>
          <td class="mono">{d["avg_distress"]}</td>
          <td class="mono">${d["avg_revenue_m"]}M</td>
          <td class="mono">{d["avg_enroll"]:,}</td>
          <td class="mono">{d["avg_cr"]}</td>
          <td class="mono">{d["avg_acres"]:,}</td>
          <td class="mono">{d["land_potential"]:,}</td>
          <td class="mono" style="color:{'#ff6b35' if d['avg_op_margin']<0 else 'var(--text)'}">{d["avg_op_margin"]}%</td>
          <td class="mono">{d["avg_env_score"]}</td>
        </tr>''' for d in s['ipeds_dist'])

    dist_table = f'''<div class="tbl-wrap"><table>
      <thead><tr>{th("Category")}{th("Count","count")}{th("%","pct")}{th("Avg Distress","avg_distress")}{th("Avg Revenue","avg_revenue_m")}{th("Avg Enrollment","avg_enroll")}{th("Avg Closure Risk","avg_cr")}{th("Avg Acres","avg_acres")}{th("Land ≥50ac","land_potential")}{th("Avg Op. Margin","avg_op_margin")}{th("Avg Env Score","avg_env_score")}</tr></thead>
      <tbody>{dist_rows}</tbody></table></div>'''

    # ML risk table
    risk_rows = ''.join(
        f'''<tr>
          <td>{pill(r["tier"])}</td>
          <td class="mono">{r["count"]:,}</td>
          <td class="mono">{r["pct"]}%</td>
          <td class="mono">{r["avg_cr"] if r["avg_cr"] is not None else "—"}</td>
          <td class="mono">{r["avg_distress"] if r["avg_distress"] is not None else "—"}</td>
          <td class="mono">{("$"+str(r["avg_revenue_m"])+"M") if r["avg_revenue_m"] is not None else "—"}</td>
          <td class="mono">{(str(r["avg_enroll"])) if r["avg_enroll"] is not None else "—"}</td>
          <td class="mono">{(str(r["avg_acres"])) if r["avg_acres"] is not None else "—"}</td>
          <td class="mono" style="color:{'#ff6b35' if r['avg_enr_trend'] is not None and r['avg_enr_trend']<0 else 'var(--text)'}">{(str(r["avg_enr_trend"])+"%") if r["avg_enr_trend"] is not None else "—"}</td>
          <td class="mono">{(str(r["land_potential"])) if r["land_potential"] is not None else "—"}</td>
          <td class="mono">{(str(r["avg_env_score"])) if r["avg_env_score"] is not None else "—"}</td>
        </tr>''' for r in s['ipeds_risk'])

    risk_table = f'''<div class="tbl-wrap"><table>
      <thead><tr>{th("ML Risk Tier")}{th("Count","count")}{th("%","pct")}{th("Avg ML Score","avg_cr")}{th("Avg Distress","avg_distress")}{th("Avg Revenue","avg_revenue_m")}{th("Avg Enrollment","avg_enroll")}{th("Avg Acres","avg_acres")}{th("Enr. Trend 2yr","avg_enr_trend")}{th("Land ≥50ac","land_potential")}{th("Avg Env Score","avg_env_score")}</tr></thead>
      <tbody>{risk_rows}</tbody></table></div>'''

    # By type table
    type_rows_ipeds = ''.join(
        f'''<tr>
          <td style="max-width:200px">{t["type"]}</td>
          <td class="mono">{t["count"]:,}</td>
          <td class="mono">{t["pct"]}%</td>
          <td class="mono">${t["total_assets_b"]}B</td>
          <td class="mono">${t["total_liab_b"]}B</td>
          <td class="mono">${t["total_rev_b"]}B</td>
          <td class="mono">{t["total_acres"]:,}</td>
          <td class="mono">{t["avg_distress"]}</td>
          <td class="mono" style="color:{'#ff6b35' if t['avg_op_margin']<0 else 'var(--text)'}">{t["avg_op_margin"]}%</td>
          <td class="mono">{t["med_enroll"]:,}</td>
          <td class="mono" style="color:#ff6b35">{t["crit_high"]:,}</td>
          <td class="mono">{t["high_land"]:,}</td>
          <td class="mono">{t["avg_pr_score"]}</td>
          <td class="mono">{t["avg_env_score"]}</td>
        </tr>''' for t in s['ipeds_types'])

    type_table_ipeds = f'''<div class="tbl-wrap"><table>
      <thead><tr>{th("Type")}{th("Count","count")}{th("%","pct")}{th("Total Assets","total_assets_b")}{th("Total Liabilities","total_liab_b")}{th("Total Revenue","total_rev_b")}{th("Total Acres","total_acres")}{th("Avg Distress","avg_distress")}{th("Avg Op. Margin","avg_op_margin")}{th("Med. Enrollment","med_enroll")}{th("Crit/High","crit_high")}{th("High Land","high_land")}{th("Avg PR Score","avg_pr_score")}{th("Avg Env Score","avg_env_score")}</tr></thead>
      <tbody>{type_rows_ipeds}</tbody></table></div>'''

    # Top 50 IPEDS
    top50_ipeds_rows = ''.join(
        f'''<tr>
          <td style="font-weight:600;max-width:200px">{r["name"]}<small style="display:block;font-size:9px;color:var(--text2);font-weight:400">{r["city"]}, {r["state"]}</small></td>
          <td style="font-size:9px;color:var(--text2)">{r["type"].replace("Private Non-Profit | ","NP ").replace("Private For-Profit | ","FP ").replace("Public | ","Pub ")}</td>
          <td>{pill(r["category"])}</td>
          <td class="mono" style="color:{'#ff4757' if r['distress']>=70 else '#ff6b35' if r['distress']>=50 else '#ffa502'}">{r["distress"]}</td>
          <td class="mono">{r["cr"]}</td>
          <td class="mono">{pill(r["risk_tier"]) if r["risk_tier"] else "—"}</td>
          <td class="mono">{r["enrollment"]:,}</td>
          <td class="mono" style="color:{'#ff6b35' if r['enr_trend']<0 else 'var(--text)'}">{r["enr_trend"]}%</td>
          <td class="mono">${r["revenue_m"]}M</td>
          <td class="mono" style="color:{'#ff6b35' if r['op_margin']<0 else 'var(--text)'}">{r["op_margin"]}%</td>
          <td class="mono">{r["acres"]:,} ac</td>
          <td>{"<span style='color:#10b981'>✓</span>" if r["land_flag"] else "<span style='color:var(--text2)'>—</span>"}</td>
          <td class="mono">{(str(r["env_score"]) if r["env_score"] is not None else "—")}</td>
          <td>{pill(r["pr_label"]) if r["pr_label"] else "—"}</td>
          <td style="font-size:9px;color:var(--text2)">{r["urbanization"]}</td>
        </tr>''' for r in s['top50_ipeds'])

    top50_ipeds_table = f'''<div class="tbl-wrap"><div class="tbl-scroll"><table>
      <thead><tr>{th("Institution")}{th("Type")}{th("Category","category")}{th("Distress","distress")}{th("Closure Risk","cr")}{th("ML Tier","risk_tier")}{th("Enrollment","enrollment")}{th("Enr. Trend","enr_trend")}{th("Revenue","revenue_m")}{th("Op. Margin","op_margin")}{th("Acres","acres")}{th("Land ≥50ac","land_flag")}{th("Env Score","env_score")}{th("PR Label","pr_label")}{th("Urbanization","urbanization")}</tr></thead>
      <tbody>{top50_ipeds_rows}</tbody></table></div></div>'''

    # Acreage table with search
    acreage_rows_html = ''.join(
        f'''<tr>
          <td style="font-weight:600;max-width:200px">{r["name"]}<small style="display:block;font-size:9px;color:var(--text2);font-weight:400">{r["city"]}, {r["state"]}</small></td>
          <td style="font-size:9px;color:var(--text2)">{r["type"].replace("Private Non-Profit | ","NP ").replace("Private For-Profit | ","FP ").replace("Public | ","Pub ")}</td>
          <td class="mono" style="font-weight:600">{r["acres"]:,}</td>
          <td>{pill(r["category"])}</td>
          <td class="mono">{r["distress"]}</td>
          <td class="mono">{r["cr"]}</td>
          <td class="mono">{r["enrollment"]:,}</td>
          <td class="mono">${r["revenue_m"]}M</td>
          <td class="mono">{(str(r["env_score"]) if r["env_score"] is not None else "—")}</td>
          <td>{pill(r["pr_label"]) if r["pr_label"] else "—"}</td>
          <td style="font-size:9px;color:var(--text2)">{r["urbanization"]}</td>
        </tr>''' for r in s['acreage_rows'])

    acreage_table = f'''
      <input class="tbl-search" id="acreageSearch" placeholder="Search institutions, state, city…" oninput="filterTable('acreageSearch','acreageBody')">
      <div class="tbl-count" id="acreageCount">{len(s['acreage_rows']):,} institutions with acreage data</div>
      <div class="tbl-wrap"><div class="tbl-scroll"><table>
        <thead><tr>{th("Institution")}{th("Type")}{th("Acres","acres")}{th("Category","category")}{th("Distress","distress")}{th("Closure Risk","cr")}{th("Enrollment","enrollment")}{th("Revenue","revenue_m")}{th("Env Score","env_score")}{th("PR Label","pr_label")}{th("Urbanization")}</tr></thead>
        <tbody id="acreageBody">{acreage_rows_html}</tbody>
      </table></div></div>'''

    panel_ipeds = f'''
    <div class="metrics">
      {metric("Institutions", f"{s['n_ipeds']:,}", "Colleges &amp; universities", "accent")}
      {metric("Total enrollment", f"{s['ipeds_enr_total_m']}M", "Students enrolled (2024)")}
      {metric("Total revenue", f"${s['ipeds_rev_total_b']}B", "Annual sector revenue")}
      {metric("Critical/High distress", f"{s['ipeds_crit_count']:,}", f"{s['ipeds_crit_pct']}% of IPEDS institutions", "red")}
    </div>
    {section("Table 1 — Distress Category Breakdown", dist_table)}
    {section("Table 2 — ML Closure Risk Tier Breakdown", risk_table)}
    {section("Table 3 — Summary by Institution Type", type_table_ipeds)}
    {section("Table 4 — Top 50 Highest Distress Institutions", top50_ipeds_table)}
    {section(f"Table 5 — Full Acreage Detail ({len(s['acreage_rows']):,} institutions)", acreage_table)}'''

    # ── 990 tab ───────────────────────────────────────────────────────────
    type_rows_990 = ''.join(
        f'''<tr>
          <td>{t["type"]}</td>
          <td class="mono">{t["count"]:,}</td>
          <td class="mono">{t["pct"]}%</td>
          <td class="mono">${t["total_assets_m"]:,}M</td>
          <td class="mono">${t["total_liab_m"]:,}M</td>
          <td class="mono">${t["total_rev_m"]:,}M</td>
          <td class="mono">{t["total_acres"]:,}</td>
          <td class="mono">{t["avg_distress"]}</td>
          <td class="mono">${t["med_rev_k"]:,}K</td>
          <td class="mono" style="color:#ff6b35">{t["pct_op_loss"]}%</td>
          <td class="mono" style="color:#ffa502">{t["pct_neg_ast"]}%</td>
        </tr>''' for t in s['n990_types'])

    type_table_990 = f'''<div class="tbl-wrap"><table>
      <thead><tr>{th("Category")}{th("Count","count")}{th("%","pct")}{th("Total Assets","total_assets_m")}{th("Total Liabilities","total_liab_m")}{th("Total Revenue","total_rev_m")}{th("Total Acres","total_acres")}{th("Avg Distress","avg_distress")}{th("Med. Revenue","med_rev_k")}{th("Op. Loss %","pct_op_loss")}{th("Neg. Assets %","pct_neg_ast")}</tr></thead>
      <tbody>{type_rows_990}</tbody></table></div>'''

    top50_990_rows = ''.join(
        f'''<tr>
          <td style="font-weight:600;max-width:200px">{r["name"]}<small style="display:block;font-size:9px;color:var(--text2);font-weight:400">{r["city"]}, {r["state"]}</small></td>
          <td style="font-size:9px;color:var(--text2)">{r["type"]}</td>
          <td>{pill(r["category"])}</td>
          <td class="mono" style="color:#ff6b35">{r["distress"]}</td>
          <td class="mono">${r["revenue_k"]:,}K</td>
          <td class="mono">${r["ppe_k"]:,}K</td>
          <td class="mono">{r["acres"]:,} ac</td>
          <td>{"<span style='color:#ff4757'>Yes</span>" if r["pct_neg_ast"] else "<span style='color:var(--text2)'>No</span>"}</td>
          <td>{"<span style='color:#ff6b35'>Yes</span>" if r["pct_op_loss"] else "<span style='color:var(--text2)'>No</span>"}</td>
        </tr>''' for r in s['top50_990'])

    top50_990_table = f'''<div class="tbl-wrap"><div class="tbl-scroll"><table>
      <thead><tr>{th("Institution")}{th("Type")}{th("Category","category")}{th("Distress","distress")}{th("Revenue","revenue_k")}{th("PP&E","ppe_k")}{th("Acres","acres")}{th("Neg. Assets")}{th("Op. Loss")}</tr></thead>
      <tbody>{top50_990_rows}</tbody></table></div></div>'''

    panel_990 = f'''
    <div class="metrics">
      {metric("Institutions", f"{s['n_990']:,}", "Nonprofits across 10 categories", "hb990")}
      {metric("Total revenue", f"${s['n990_rev_total_b']}B", "Annual nonprofit sector revenue")}
      {metric("Critical/High distress", f"{s['n990_crit_count']:,}", f"{s['n990_crit_pct']}% of 990 institutions", "red")}
      {metric("Total acreage", f"{s['n990_acres_total']:,}", f"{s['n990_w_acres']:,} institutions with data", "green")}
    </div>
    {section("Table 6 — Summary by Nonprofit Category", type_table_990)}
    {section("Table 7 — Top 50 Highest Distress Nonprofits", top50_990_table)}'''

    # ── JS ────────────────────────────────────────────────────────────────
    js = """
function filterTable(inputId, tbodyId) {
  var q = document.getElementById(inputId).value.toLowerCase();
  var rows = document.getElementById(tbodyId).querySelectorAll('tr');
  var shown = 0;
  rows.forEach(function(tr) {
    var match = tr.textContent.toLowerCase().indexOf(q) >= 0;
    tr.style.display = match ? '' : 'none';
    if (match) shown++;
  });
  var countEl = document.getElementById(inputId.replace('Search','Count'));
  if (countEl) countEl.textContent = shown.toLocaleString() + ' institutions shown';
}
['tabAll','tabIPEDS','tab990'].forEach(function(id) {
  document.getElementById(id).addEventListener('click', function() {
    document.querySelectorAll('.tab').forEach(function(t){t.classList.remove('active');});
    document.querySelectorAll('.panel').forEach(function(p){p.classList.remove('active');});
    this.classList.add('active');
    document.getElementById('panel'+id.replace('tab','')).classList.add('active');
  });
});"""

    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Hummingbird \u2014 Market Summary</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>{CSS}</style></head><body>
<nav>
  <div class="nav-logo">Hummingbird</div><div class="nav-sep"></div>
  <span style="font-size:11px;color:var(--text2);font-weight:500">Market Summary</span>
  <div class="nav-spacer"></div>
  <a href="index.html" class="nav-back">\u2190 Map</a>
  <a href="clusters.html" class="nav-back">Clusters</a>
</nav>
<div class="page">
  <div style="font-family:var(--mono);font-size:10px;color:var(--accent2);letter-spacing:2px;text-transform:uppercase;margin-bottom:8px">Phase I \u00b7 Market Intelligence</div>
  <div style="font-size:26px;font-weight:700;letter-spacing:-.5px;margin-bottom:6px">Market Opportunity Summary</div>
  <div style="font-size:12px;color:var(--text2);line-height:1.6;max-width:700px;margin-bottom:20px">
    {s['total']:,} institutions tracked across higher education and the nonprofit sector.
    Source: IPEDS/NCES, IRS Form 990, OSM land scrape. Updated {run_date}.
  </div>
  {callout}
  <div class="tabs">
    <div class="tab active" id="tabAll">All Institutions</div>
    <div class="tab" id="tabIPEDS">IPEDS \u2014 Higher Ed</div>
    <div class="tab" id="tab990">990 \u2014 Nonprofits</div>
  </div>
  <div class="panel active" id="panelAll">{panel_all}</div>
  <div class="panel" id="panelIPEDS">{panel_ipeds}</div>
  <div class="panel" id="panel990">{panel_990}</div>
  <div class="footer">Source: Hummingbird Master Dataset v6 \u00b7 IPEDS/NCES, IRS Form 990, OSM land scrape \u00b7 {run_date}</div>
</div>
<script>{js}</script>
</body></html>"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--master', default=DEFAULT_MASTER)
    parser.add_argument('--osm',    default=DEFAULT_OSM)
    parser.add_argument('--out',    default=DEFAULT_OUT)
    args = parser.parse_args()
    print(f"Loading {args.master}...")
    csv.field_size_limit(100 * 1024 * 1024)
    rows = []
    with open(args.master, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f): rows.append(row)
    print(f"  {len(rows):,} rows")

    # Build urbanization lookup for env scorer context multiplier
    print("Loading OSM environment scores...")
    urb_lookup = {}
    for r in rows:
        key = f"{r.get('institution_name','').strip()}||{r.get('city','').strip()}||{r.get('state','').strip()}"
        urb = r.get('urbanization','').strip()
        if urb:
            urb_lookup[key] = urb
    env_lookup = build_env_score_lookup(args.osm, urb_lookup=urb_lookup)

    print("Computing stats...")
    s = compute_stats(rows, env_lookup=env_lookup)
    print(f"Generating {args.out}...")
    html = build_html(s)
    with open(args.out, 'w', encoding='utf-8') as f: f.write(html)
    print(f"  Done. ({len(html):,} chars)")

if __name__ == '__main__':
    main()