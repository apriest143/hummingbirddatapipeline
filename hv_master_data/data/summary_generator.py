"""
summary_generator.py - Hummingbird Market Summary Dashboard
============================================================
Generates summary.html from master CSV. Called by master_standalone.py
or run directly: python hv_master_data/data/summary_generator.py
"""
import os, sys, csv, json, math, datetime, argparse
from collections import Counter, defaultdict

DEFAULT_MASTER = 'hv_master_data/data/Hummingbird_Master_Combined_v6.csv'
DEFAULT_OUT    = 'summary.html'

def fv(r,c):
    v=r.get(c,'')
    if v in ('','nan','NaN','None',None): return None
    try: x=float(v); return None if (math.isnan(x) or math.isinf(x)) else x
    except: return None
def flag(r,c): return str(r.get(c,'')).strip().lower() in ('1','true','yes','1.0')
def med(vals):
    vals=[v for v in vals if v is not None]
    if not vals: return 0
    vals.sort(); return vals[len(vals)//2]
def pct(n,d): return round(n/max(d,1)*100,1)
def get_acres(r): return fv(r,'acreage_primary') or fv(r,'scraper_acres') or fv(r,'osm_acres')

def compute_stats(rows):
    ipeds=[r for r in rows if r.get('data_source')=='IPEDS']
    hb990=[r for r in rows if r.get('data_source')=='Hummingbird_990']
    def type_stats_ipeds():
        by_type=defaultdict(list)
        for r in ipeds: by_type[r.get('institution_type','Unknown')].append(r)
        out={}
        for t,rs in sorted(by_type.items(),key=lambda x:-len(x[1])):
            al=[get_acres(r) for r in rs if get_acres(r)]
            out[t]={'count':len(rs),'pct':pct(len(rs),len(ipeds)),'total_acres':round(sum(al)),'w_acres':len(al),
                    'med_distress':round(med([fv(r,'distress_score') for r in rs if fv(r,'distress_score')]),1),
                    'med_rev_m':round(med([fv(r,'revenue_2024') for r in rs if fv(r,'revenue_2024')])/1e6,1),
                    'med_enr':round(med([fv(r,'enrollment_2024') for r in rs if fv(r,'enrollment_2024')])),
                    'crit_high':sum(1 for r in rs if r.get('distress_category') in ('Critical','High'))}
        return out
    def type_stats_990():
        by_type=defaultdict(list)
        for r in hb990: by_type[r.get('institution_type','Unknown')].append(r)
        out={}
        for t,rs in sorted(by_type.items(),key=lambda x:-len(x[1])):
            al=[get_acres(r) for r in rs if get_acres(r)]
            out[t]={'count':len(rs),'pct':pct(len(rs),len(hb990)),'total_acres':round(sum(al)),'w_acres':len(al),
                    'med_rev_k':round(med([fv(r,'revenue_2024') for r in rs if fv(r,'revenue_2024')])/1000),
                    'pct_op_loss':round(pct(sum(1 for r in rs if flag(r,'flag_990_operating_loss')),len(rs))),
                    'pct_neg_ast':round(pct(sum(1 for r in rs if flag(r,'flag_990_negative_net_assets')),len(rs))),
                    'pct_hi_debt':round(pct(sum(1 for r in rs if flag(r,'flag_990_high_debt')),len(rs)))}
        return out
    ipeds_crit=[r for r in ipeds if r.get('distress_category') in ('Critical','High')]
    hb990_crit=[r for r in hb990 if r.get('distress_category') in ('Critical','High')]
    ia=sum(get_acres(r) for r in ipeds if get_acres(r))
    ha=sum(get_acres(r) for r in hb990 if get_acres(r))
    ida=sum(get_acres(r) for r in ipeds_crit if get_acres(r))
    return {
        'run_date':datetime.datetime.now().strftime('%B %d, %Y'),
        'total':len(rows),'n_ipeds':len(ipeds),'n_990':len(hb990),
        'ipeds_enr_total_m':round(sum(fv(r,'enrollment_2024') for r in ipeds if fv(r,'enrollment_2024'))/1e6,1),
        'ipeds_rev_total_b':round(sum(fv(r,'revenue_2024') for r in ipeds if fv(r,'revenue_2024'))/1e9,1),
        'ipeds_exp_total_b':round(sum(fv(r,'expenses_2024') for r in ipeds if fv(r,'expenses_2024'))/1e9,1),
        'ipeds_acres_total':round(ia),'ipeds_w_acres':sum(1 for r in ipeds if get_acres(r)),
        'ipeds_acres_pct':round(pct(sum(1 for r in ipeds if get_acres(r)),len(ipeds))),
        'ipeds_med_acres':round(med([get_acres(r) for r in ipeds if get_acres(r)])),
        'ipeds_crit_count':len(ipeds_crit),'ipeds_crit_pct':round(pct(len(ipeds_crit),len(ipeds))),
        'ipeds_dist_acres':round(ida),
        'ipeds_land_potential':sum(1 for r in ipeds if flag(r,'flag_land_potential')),
        'ipeds_hi_land':sum(1 for r in ipeds if flag(r,'flag_high_land_potential')),
        'ipeds_hi_cr':sum(1 for r in ipeds if (fv(r,'closure_risk_score') or 0)>0.3),
        'ipeds_dist_cats':{k:v for k,v in Counter(r.get('distress_category','') for r in ipeds).items() if k},
        'ipeds_risk_tiers':{k:v for k,v in Counter(r.get('risk_tier','') for r in ipeds).items() if k},
        'ipeds_by_type':type_stats_ipeds(),
        'n990_rev_total_b':round(sum(fv(r,'revenue_2024') for r in hb990 if fv(r,'revenue_2024'))/1e9,2),
        'n990_acres_total':round(ha),'n990_w_acres':sum(1 for r in hb990 if get_acres(r)),
        'n990_acres_pct':round(pct(sum(1 for r in hb990 if get_acres(r)),len(hb990))),
        'n990_med_acres':round(med([get_acres(r) for r in hb990 if get_acres(r)])),
        'n990_crit_count':len(hb990_crit),'n990_crit_pct':round(pct(len(hb990_crit),len(hb990))),
        'n990_op_loss_pct':round(pct(sum(1 for r in hb990 if flag(r,'flag_990_operating_loss')),len(hb990))),
        'n990_neg_ast_pct':round(pct(sum(1 for r in hb990 if flag(r,'flag_990_negative_net_assets')),len(hb990))),
        'n990_hi_debt_pct':round(pct(sum(1 for r in hb990 if flag(r,'flag_990_high_debt')),len(hb990))),
        'n990_consec_pct':round(pct(sum(1 for r in hb990 if flag(r,'flag_990_consecutive_losses')),len(hb990))),
        'n990_by_type':type_stats_990(),
        'combined_acres':round(ia+ha),'combined_crit':len(ipeds_crit)+len(hb990_crit),
        'combined_dist_acres':round(ida+ha),
    }

def build_html(s):
    dist_order=['Critical','High','Moderate','Low','Healthy']
    dist_colors={'Critical':'#E24B4A','High':'#BA7517','Moderate':'#378ADD','Low':'#639922','Healthy':'#1D9E75'}
    risk_order=['Critical','High','Elevated','Moderate','Low']
    risk_colors={'Critical':'#E24B4A','High':'#BA7517','Elevated':'#ffa502','Moderate':'#378ADD','Low':'#1D9E75'}

    def pill(val, color):
        return f'<span style="display:inline-block;padding:2px 7px;border-radius:3px;font-family:var(--mono);font-size:9px;font-weight:600;background:{color}22;color:{color}">{val}</span>'

    def bar_row(label, pct_val, color, display_val, lw=190):
        return (f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:7px">'
                f'<span style="font-size:11px;color:var(--text2);flex-shrink:0;width:{lw}px;text-align:right">{label}</span>'
                f'<div style="flex:1;height:7px;background:var(--border);border-radius:3px;overflow:hidden">'
                f'<div style="width:{min(100,pct_val)}%;height:100%;border-radius:3px;background:{color}"></div></div>'
                f'<span style="font-size:11px;font-weight:500;color:var(--text);width:90px;font-family:var(--mono)">{display_val}</span></div>')

    def metric(label, value, sub='', cls=''):
        bg = {'accent':'background:rgba(108,92,231,.1);border:1px solid rgba(108,92,231,.2)',
              'green':'background:rgba(0,184,148,.08);border:1px solid rgba(0,184,148,.2)',
              'red':'background:rgba(226,75,74,.08);border:1px solid rgba(226,75,74,.2)',
              'hb990':'background:rgba(0,184,148,.08);border:1px solid rgba(0,184,148,.3)',
              }.get(cls,'background:var(--surface2)')
        vc = {'accent':'#a29bfe','green':'#00b894','red':'#ff6b35','hb990':'#00b894'}.get(cls,'var(--text)')
        return (f'<div style="border-radius:8px;padding:14px;{bg}">'
                f'<div style="font-size:11px;color:var(--text2);margin-bottom:4px">{label}</div>'
                f'<div style="font-size:22px;font-weight:600;color:{vc};line-height:1.1">{value}</div>'
                f'<div style="font-size:10px;color:var(--text2);margin-top:3px">{sub}</div></div>')

    def grid(*cells, cols=4):
        return (f'<div style="display:grid;grid-template-columns:repeat({cols},minmax(0,1fr));'
                f'gap:12px;margin-bottom:24px">{"".join(cells)}</div>')

    def card(title, body):
        return (f'<div style="background:var(--surface);border:1px solid var(--border);'
                f'border-radius:10px;padding:18px;margin-bottom:16px">'
                f'<div style="font-family:var(--mono);font-size:10px;color:var(--text2);'
                f'text-transform:uppercase;letter-spacing:1px;margin-bottom:14px">{title}</div>{body}</div>')

    def section_head(label):
        return (f'<div style="display:flex;align-items:center;gap:10px;margin:24px 0 14px">'
                f'<div style="font-family:var(--mono);font-size:10px;color:var(--text2);'
                f'text-transform:uppercase;letter-spacing:1px;white-space:nowrap">{label}</div>'
                f'<div style="flex:1;height:1px;background:var(--border)"></div></div>')

    th = lambda t: (f'<th style="padding:7px 10px;text-align:left;font-family:var(--mono);font-size:9px;'
                    f'color:var(--text2);text-transform:uppercase;letter-spacing:.6px;'
                    f'border-bottom:1px solid var(--border);background:var(--surface2);white-space:nowrap">{t}</th>')

    def ipeds_table():
        rows_html=''
        for t,d in s['ipeds_by_type'].items():
            if d['count']<=1: continue
            ds=d['med_distress']
            dc='#ff6b35' if ds>=60 else '#ffa502' if ds>=40 else '#74b9ff' if ds>=20 else '#10b981'
            rows_html+=(f'<tr><td style="font-weight:500">{t}</td>'
                        f'<td style="font-family:var(--mono)">{d["count"]:,}</td>'
                        f'<td style="font-family:var(--mono)">{d["pct"]}%</td>'
                        f'<td style="font-family:var(--mono)">{d["total_acres"]:,}</td>'
                        f'<td>{pill(ds,dc)}</td>'
                        f'<td style="font-family:var(--mono)">${d["med_rev_m"]}M</td>'
                        f'<td style="font-family:var(--mono)">{d["med_enr"]:,}</td>'
                        f'<td style="font-family:var(--mono);color:#ff6b35">{d["crit_high"]:,}</td></tr>')
        heads=''.join(th(h) for h in ['Type','Count','%','Total Acres','Med. Distress','Med. Revenue','Med. Enroll','Crit/High'])
        return f'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:11px"><thead><tr>{heads}</tr></thead><tbody>{rows_html}</tbody></table></div>'

    def n990_table():
        rows_html=''
        for t,d in s['n990_by_type'].items():
            rows_html+=(f'<tr><td style="font-weight:500">{t}</td>'
                        f'<td style="font-family:var(--mono)">{d["count"]:,}</td>'
                        f'<td style="font-family:var(--mono)">{d["pct"]}%</td>'
                        f'<td style="font-family:var(--mono)">{d["total_acres"]:,}</td>'
                        f'<td style="font-family:var(--mono)">${d["med_rev_k"]:,}K</td>'
                        f'<td>{pill(str(d["pct_op_loss"])+"%","#E24B4A")}</td>'
                        f'<td>{pill(str(d["pct_neg_ast"])+"%","#BA7517")}</td>'
                        f'<td>{pill(str(d["pct_hi_debt"])+"%","#378ADD")}</td></tr>')
        heads=''.join(th(h) for h in ['Category','Count','%','Total Acres','Med. Revenue','Op. Loss','Neg. Assets','High Debt'])
        return f'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:11px"><thead><tr>{heads}</tr></thead><tbody>{rows_html}</tbody></table></div>'

    dist_bars=''.join(bar_row(c,round(s['ipeds_dist_cats'].get(c,0)/s['n_ipeds']*100),dist_colors.get(c,'#888'),
                              f"{s['ipeds_dist_cats'].get(c,0):,} ({round(s['ipeds_dist_cats'].get(c,0)/s['n_ipeds']*100,1)}%)")
                      for c in dist_order if c in s['ipeds_dist_cats'])
    risk_bars=''.join(bar_row(t,round(s['ipeds_risk_tiers'].get(t,0)/s['n_ipeds']*100),risk_colors.get(t,'#888'),
                              f"{s['ipeds_risk_tiers'].get(t,0):,} ({round(s['ipeds_risk_tiers'].get(t,0)/s['n_ipeds']*100,1)}%)")
                      for t in risk_order if t in s['ipeds_risk_tiers'])

    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Hummingbird \u2014 Market Summary</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{{--bg:#0f1117;--surface:#181a20;--surface2:#1e2028;--border:#2a2d37;
--text:#e2e4e9;--text2:#8b8fa3;--accent:#6c5ce7;--accent2:#a29bfe;--hb990:#00b894;
--font:'DM Sans',-apple-system,sans-serif;--mono:'JetBrains Mono',monospace;}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
html,body{{background:var(--bg);color:var(--text);font-family:var(--font);font-size:14px;}}
nav{{position:sticky;top:0;z-index:100;display:flex;align-items:center;gap:14px;padding:0 24px;height:52px;background:var(--surface);border-bottom:1px solid var(--border);}}
.nav-logo{{font-size:16px;font-weight:700;letter-spacing:-.5px;background:linear-gradient(135deg,var(--accent2),#74b9ff);-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;}}
.nav-sep,.nav-spacer{{flex-shrink:0;}} .nav-sep{{width:1px;height:18px;background:var(--border);}} .nav-spacer{{flex:1;}}
.nav-back{{display:flex;align-items:center;gap:6px;padding:5px 12px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text2);font-size:11px;font-weight:600;text-decoration:none;margin-left:6px;}}
.nav-back:hover{{border-color:var(--accent);color:var(--text);}}
.page{{max-width:1200px;margin:0 auto;padding:28px 24px 80px;}}
.tabs{{display:flex;gap:0;margin-bottom:24px;border-bottom:1px solid var(--border);}}
.tab{{padding:8px 20px;border:1px solid transparent;border-bottom:none;border-radius:6px 6px 0 0;background:transparent;color:var(--text2);font-family:var(--font);font-size:12px;font-weight:600;cursor:pointer;transition:all .15s;margin-bottom:-1px;}}
.tab.active{{background:var(--surface);border-color:var(--border);border-bottom-color:var(--surface);color:var(--text);}}
.tab:hover:not(.active){{color:var(--text);}} .panel{{display:none;}} .panel.active{{display:block;}}
td{{padding:7px 10px;border-bottom:1px solid rgba(42,45,55,.5);vertical-align:middle;}} tr:hover td{{background:rgba(255,255,255,.015);}}
</style></head><body>
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
  <div style="font-size:12px;color:var(--text2);line-height:1.6;max-width:700px;margin-bottom:20px">Aggregated analysis of {s['total']:,} tracked institutions. Source: IPEDS/NCES, IRS Form 990, OSM land scrape. Updated {s['run_date']}.</div>
  <div style="padding:14px 18px;border-radius:8px;border-left:3px solid var(--hb990);background:rgba(0,184,148,.06);margin-bottom:24px;display:flex;gap:40px;align-items:flex-end;flex-wrap:wrap">
    <div><div style="font-size:30px;font-weight:700;color:#00b894">{s['combined_crit']:,}</div><div style="font-size:11px;color:#a8e6da">critical/high distress institutions</div></div>
    <div><div style="font-size:30px;font-weight:700;color:#00b894">{s['combined_acres']:,}</div><div style="font-size:11px;color:#a8e6da">total acres tracked</div></div>
    <div><div style="font-size:30px;font-weight:700;color:#00b894">{s['combined_dist_acres']:,}</div><div style="font-size:11px;color:#a8e6da">distressed acres</div></div>
    <div><div style="font-size:30px;font-weight:700;color:#00b894">~{round(s['combined_dist_acres']/640):,}</div><div style="font-size:11px;color:#a8e6da">sq miles addressable</div></div>
  </div>
  <div class="tabs">
    <div class="tab active" id="tabAll">All Institutions</div>
    <div class="tab" id="tabIPEDS">IPEDS \u2014 Higher Ed</div>
    <div class="tab" id="tab990">990 \u2014 Nonprofits</div>
  </div>

  <div class="panel active" id="panelAll">
    {grid(metric('Total institutions','12,911','IPEDS + IRS Form 990','accent'),metric('Higher education',f"{s['n_ipeds']:,}",'Colleges & universities'),metric('Nonprofits',f"{s['n_990']:,}",'Faith, recreation, housing, more'),metric('Critical/High distress',f"{s['combined_crit']:,}",'Across both datasets','red'),cols=4)}
    {grid(metric('Combined acreage',f"{s['combined_acres']:,}","acres across both datasets",'green'),metric('IPEDS acreage',f"{s['ipeds_acres_total']:,}",f"{s['ipeds_w_acres']:,} institutions ({s['ipeds_acres_pct']}%)"),metric('Nonprofit acreage',f"{s['n990_acres_total']:,}",f"{s['n990_w_acres']:,} institutions ({s['n990_acres_pct']}%)"),cols=3)}
    {card('Distressed acreage',bar_row('IPEDS distressed',round(s['ipeds_dist_acres']/max(s['combined_dist_acres'],1)*100),'#E24B4A',f"{s['ipeds_dist_acres']:,} ac")+bar_row('990 distressed (all)',100,'#00b894',f"{s['n990_acres_total']:,} ac")+bar_row('Combined',100,'#a29bfe',f"{s['combined_dist_acres']:,} ac")+f'<div style="font-size:10px;color:var(--text2);margin-top:10px">~{round(s["combined_dist_acres"]/640):,} square miles of addressable land opportunity</div>')}
  </div>

  <div class="panel" id="panelIPEDS">
    {grid(metric('Institutions',f"{s['n_ipeds']:,}",'Colleges & universities','accent'),metric('Total enrollment',f"{s['ipeds_enr_total_m']}M",'Students enrolled (2024)'),metric('Total revenue',f"${s['ipeds_rev_total_b']}B",'Annual sector revenue'),metric('Critical/High distress',f"{s['ipeds_crit_count']:,}",f"{s['ipeds_crit_pct']}% of IPEDS institutions",'red'),cols=4)}
    {grid(metric('Total acreage',f"{s['ipeds_acres_total']:,}",f"{s['ipeds_acres_pct']}% have data",'green'),metric('Median acres',str(s['ipeds_med_acres']),'Per institution'),metric('Land potential flag',f"{s['ipeds_land_potential']:,}",'Flagged for opportunity'),metric('High closure risk',f"{s['ipeds_hi_cr']:,}",'ML score > 0.3'),cols=4)}
    {section_head('Distress distribution')}
    {grid(card('By distress category',dist_bars),card('By ML closure risk tier',risk_bars),cols=2)}
    {section_head('By institution type')}
    {card('Summary by ownership and level',ipeds_table())}
  </div>

  <div class="panel" id="panel990">
    {grid(metric('Institutions',f"{s['n_990']:,}",'Nonprofits across 10 categories','hb990'),metric('Total revenue',f"${s['n990_rev_total_b']}B",'Annual nonprofit sector revenue'),metric('Critical/High distress',f"{s['n990_crit_count']:,}",f"{s['n990_crit_pct']}% of 990 institutions",'red'),metric('Operating losses',f"{s['n990_op_loss_pct']}%",'Running a current year loss'),cols=4)}
    {grid(metric('Total acreage',f"{s['n990_acres_total']:,}",f"{s['n990_acres_pct']}% have acreage data",'green'),metric('Negative net assets',f"{s['n990_neg_ast_pct']}%",'Liabilities exceed assets'),metric('High debt burden',f"{s['n990_hi_debt_pct']}%",'Flagged for unsustainable debt'),cols=3)}
    {section_head('Distress indicators')}
    {card('% of all 990 institutions flagged',bar_row('Operating loss',s['n990_op_loss_pct'],'#E24B4A',f"{s['n990_op_loss_pct']}%")+bar_row('High debt',s['n990_hi_debt_pct'],'#BA7517',f"{s['n990_hi_debt_pct']}%")+bar_row('Negative net assets',s['n990_neg_ast_pct'],'#ff6b35',f"{s['n990_neg_ast_pct']}%")+bar_row('Consecutive losses',s['n990_consec_pct'],'#ffa502',f"{s['n990_consec_pct']}%"))}
    {section_head('By nonprofit category')}
    {card('Summary by institution type',n990_table())}
  </div>

  <div style="font-size:10px;color:var(--text2);text-align:right;margin-top:32px;padding-top:16px;border-top:1px solid var(--border)">
    Source: Hummingbird Master Dataset v6 \u00b7 IPEDS/NCES, IRS Form 990, OSM land scrape \u00b7 Updated {s['run_date']}
  </div>
</div>
<script>
['tabAll','tabIPEDS','tab990'].forEach(function(id){{
  document.getElementById(id).addEventListener('click',function(){{
    document.querySelectorAll('.tab').forEach(function(t){{t.classList.remove('active');}});
    document.querySelectorAll('.panel').forEach(function(p){{p.classList.remove('active');}});
    this.classList.add('active');
    document.getElementById('panel'+id.replace('tab','')).classList.add('active');
  }});
}});
</script>
</body></html>"""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--master', default=DEFAULT_MASTER)
    parser.add_argument('--out',    default=DEFAULT_OUT)
    args = parser.parse_args()
    print(f"Loading {args.master}...")
    csv.field_size_limit(100 * 1024 * 1024)
    rows = []
    with open(args.master, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f): rows.append(row)
    print(f"  {len(rows):,} rows")
    print("Computing stats...")
    s = compute_stats(rows)
    print(f"Generating {args.out}...")
    html = build_html(s)
    with open(args.out, 'w', encoding='utf-8') as f: f.write(html)
    print(f"  Done. ({len(html):,} chars)")

if __name__ == '__main__':
    main()