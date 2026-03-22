"""
Generates scatter3d.html — standalone 3D opportunity space viewer.
Run: python hv_master_data/data/build_scatter.py
Input: clusters.py output data (regenerated inline)
Output: scatter3d.html (repo root)
"""
import sys, os, re, json

# Pull data from clusters.html if it exists, otherwise run clusters.py
CLUSTERS_HTML = 'clusters.html'
OUTPUT = 'scatter3d.html'

def extract_data(clusters_html_path):
    with open(clusters_html_path, 'r', encoding='utf-8') as f:
        html = f.read()
    m = re.search(r'var IPEDS_DATA=(\[.+?\]);var HB990_DATA=(\[.+?\]);', html, re.DOTALL)
    if not m:
        raise ValueError("Could not find IPEDS_DATA and HB990_DATA in clusters.html")
    return m.group(1), m.group(2)

def build_html(ipeds_json, hb990_json):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Hummingbird — 3D Opportunity Space</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{{--bg:#0f1117;--surface:#181a20;--surface2:#1e2028;--border:#2a2d37;
--text:#e2e4e9;--text2:#8b8fa3;--accent:#6c5ce7;--accent2:#a29bfe;
--font:'DM Sans',-apple-system,sans-serif;--mono:'JetBrains Mono',monospace;}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
html,body{{background:var(--bg);color:var(--text);font-family:var(--font);
  font-size:14px;height:100%;overflow:hidden;}}
nav{{display:flex;align-items:center;gap:14px;padding:0 20px;height:48px;
  background:var(--surface);border-bottom:1px solid var(--border);flex-shrink:0;}}
.logo{{font-size:15px;font-weight:700;background:linear-gradient(135deg,var(--accent2),#74b9ff);
  -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;}}
.nav-sep{{width:1px;height:18px;background:var(--border);}}
.nav-sub{{font-size:11px;color:var(--text2);font-weight:500;}}
.nav-spacer{{flex:1;}}
.back{{padding:4px 12px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);
  color:var(--text2);font-size:11px;font-weight:600;text-decoration:none;}}
.back:hover{{border-color:var(--accent);color:var(--text);}}
.seg-btn{{padding:4px 12px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);
  color:var(--text2);font-size:11px;font-weight:600;cursor:pointer;transition:all .15s;}}
.seg-btn.active{{background:rgba(108,92,231,.15);border-color:rgba(108,92,231,.4);color:var(--accent2);}}
.seg-btn:hover:not(.active){{color:var(--text);}}
.layout{{display:flex;height:calc(100vh - 48px);}}
.sidebar{{width:260px;flex-shrink:0;background:var(--surface);border-right:1px solid var(--border);
  display:flex;flex-direction:column;overflow:hidden;}}
.sidebar-head{{padding:14px 16px;border-bottom:1px solid var(--border);}}
.sidebar-title{{font-family:var(--mono);font-size:9px;text-transform:uppercase;
  letter-spacing:1.5px;color:var(--accent2);margin-bottom:10px;}}
.legend{{padding:10px 16px;flex:1;overflow-y:auto;}}
.leg-item{{display:flex;align-items:flex-start;gap:8px;padding:6px 0;
  border-bottom:1px solid rgba(42,45,55,.5);cursor:pointer;transition:opacity .15s;}}
.leg-item:last-child{{border-bottom:none;}}
.leg-item.dimmed{{opacity:0.35;}}
.leg-dot{{width:10px;height:10px;border-radius:50%;flex-shrink:0;margin-top:3px;}}
.leg-name{{font-size:11px;line-height:1.4;}}
.leg-count{{font-family:var(--mono);font-size:9px;color:var(--text2);margin-top:2px;}}
.info-box{{padding:12px 16px;border-top:1px solid var(--border);min-height:120px;}}
.info-title{{font-size:11px;font-weight:600;margin-bottom:4px;}}
.info-sub{{font-size:10px;color:var(--text2);line-height:1.5;}}
.info-rows{{margin-top:6px;}}
.info-row{{display:flex;justify-content:space-between;font-size:10px;padding:2px 0;}}
.info-row .lbl{{color:var(--text2);}}
.info-row .val{{font-family:var(--mono);color:var(--text);}}
.canvas-wrap{{flex:1;position:relative;background:#0f1117;}}
canvas{{display:block;width:100%;height:100%;cursor:grab;}}
canvas:active{{cursor:grabbing;}}
.tooltip{{position:absolute;background:var(--surface);border:1px solid var(--border);
  border-radius:6px;padding:8px 12px;font-size:11px;pointer-events:none;
  display:none;z-index:100;max-width:220px;box-shadow:0 4px 20px rgba(0,0,0,.5);}}
.tt-name{{font-weight:600;margin-bottom:3px;}}
.tt-row{{font-family:var(--mono);font-size:9px;color:var(--text2);margin-top:2px;}}
.hint{{position:absolute;bottom:12px;left:50%;transform:translateX(-50%);
  font-size:10px;color:var(--text2);pointer-events:none;white-space:nowrap;
  background:rgba(15,17,23,.7);padding:4px 10px;border-radius:4px;}}
.axis-labels{{position:absolute;top:12px;left:12px;display:flex;gap:12px;}}
.axis-lbl{{font-family:var(--mono);font-size:10px;font-weight:600;
  padding:3px 8px;border-radius:4px;}}
.controls{{position:absolute;top:12px;right:12px;display:flex;flex-direction:column;gap:6px;}}
.ctrl-btn{{padding:5px 10px;border-radius:5px;background:rgba(30,32,40,.85);
  border:1px solid var(--border);color:var(--text2);font-size:10px;cursor:pointer;
  backdrop-filter:blur(4px);}}
.ctrl-btn:hover{{color:var(--text);border-color:var(--accent);}}
</style>
</head>
<body>
<nav>
  <div class="logo">Hummingbird</div>
  <div class="nav-sep"></div>
  <div class="nav-sub">3D Opportunity Space</div>
  <div class="nav-spacer"></div>
  <button class="seg-btn active" id="btnIPEDS">IPEDS</button>
  <button class="seg-btn" id="btn990">990</button>
  <div class="nav-sep"></div>
  <a href="clusters.html" class="back">← Clusters</a>
  <a href="index.html" class="back">Map</a>
</nav>

<div class="layout">
  <div class="sidebar">
    <div class="sidebar-head">
      <div class="sidebar-title">Clusters</div>
      <div style="font-size:10px;color:var(--text2);line-height:1.6">
        Each dot is one institution.<br>
        Grey = OSM not yet scraped.
      </div>
    </div>
    <div class="legend" id="legend"></div>
    <div class="info-box" id="infoBox">
      <div class="info-sub" style="color:var(--text2)">Hover over a dot to see institution details.</div>
    </div>
  </div>

  <div class="canvas-wrap" id="canvasWrap">
    <canvas id="c"></canvas>
    <div class="tooltip" id="tip"></div>
    <div class="hint">Drag to rotate &nbsp;·&nbsp; Scroll to zoom &nbsp;·&nbsp; Hover for details</div>
    <div class="axis-labels">
      <span class="axis-lbl" style="background:rgba(255,107,53,.15);color:#ff6b35">Urgency</span>
      <span class="axis-lbl" style="background:rgba(0,184,148,.15);color:#00b894">Land</span>
      <span class="axis-lbl" style="background:rgba(162,155,254,.15);color:#a29bfe">PR Fit</span>
    </div>
    <div class="controls">
      <button class="ctrl-btn" id="btnReset">Reset view</button>
    </div>
  </div>
</div>

<script>
var IPEDS_DATA = {ipeds_json};
var HB990_DATA = {hb990_json};

// ── State ─────────────────────────────────────────────────────────────────────
var src = 'ipeds';
var pts = [];
var camera = {{rotX:0.45, rotY:-0.6, zoom:1.0}};
var drag = false, lx = 0, ly = 0;
var hidden = {{}};  // clusterId -> true if hidden via legend click

// ── 3D projection ─────────────────────────────────────────────────────────────
function project(x, y, z) {{
  var cY=Math.cos(camera.rotY), sY=Math.sin(camera.rotY);
  var cX=Math.cos(camera.rotX), sX=Math.sin(camera.rotX);
  var cx=x-0.5, cy=y-0.5, cz=z-0.5;
  var rx=cx*cY-cz*sY, rz=cx*sY+cz*cY;
  var ry=cy*cX-rz*sX, rz2=cy*sX+rz*cX;
  var pz=rz2+2.0, f=2.5*camera.zoom/pz;
  return {{sx:rx*f, sy:ry*f, depth:pz}};
}}

// ── Build points ──────────────────────────────────────────────────────────────
function buildPoints() {{
  var cl = src === 'ipeds' ? IPEDS_DATA : HB990_DATA;
  pts = [];
  for (var ci=0; ci<cl.length; ci++) {{
    var c = cl[ci], cpts = c.points || [];
    for (var pi=0; pi<cpts.length; pi++) {{
      var p = cpts[pi], h = (p.f !== null && p.f !== undefined);
      pts.push({{
        x:(p.u||0)/100, y:(p.l||0)/100, z:h?p.f/100:0.45,
        color: hidden[c.id] ? null : (h ? c.color : '#3d4f6e'),
        name:p.n, state:p.st, u:p.u, l:p.l, f:p.f, d:p.d, cr:p.cr, a:p.a,
        hasOSM:h, cluster:c.name, clusterId:c.id,
      }});
    }}
  }}
}}

// ── Legend ────────────────────────────────────────────────────────────────────
function buildLegend() {{
  var cl = src === 'ipeds' ? IPEDS_DATA : HB990_DATA;
  var el = document.getElementById('legend');
  el.innerHTML = '';
  cl.forEach(function(c) {{
    var item = document.createElement('div');
    item.className = 'leg-item' + (hidden[c.id] ? ' dimmed' : '');
    item.innerHTML = '<div class="leg-dot" style="background:'+c.color+'"></div>'
      + '<div><div class="leg-name">'+c.icon+' '+c.name+'</div>'
      + '<div class="leg-count">'+c.count.toLocaleString()+' institutions</div></div>';
    item.addEventListener('click', function() {{
      hidden[c.id] = !hidden[c.id];
      buildPoints();
      buildLegend();
      draw();
    }});
    el.appendChild(item);
  }});
  // Not-scraped legend item
  var nsItem = document.createElement('div');
  nsItem.className = 'leg-item';
  nsItem.innerHTML = '<div class="leg-dot" style="background:#3d4f6e"></div>'
    + '<div><div class="leg-name">Not yet scraped</div>'
    + '<div class="leg-count">Placed at midpoint on Fit axis</div></div>';
  el.appendChild(nsItem);
}}

// ── Draw ──────────────────────────────────────────────────────────────────────
function draw() {{
  var canvas = document.getElementById('c');
  var W = canvas.parentElement.clientWidth || 900;
  var H = canvas.parentElement.clientHeight || 600;
  canvas.width = W;
  canvas.height = H;
  var ctx = canvas.getContext('2d');
  var cx = W/2, cy = H/2, scale = Math.min(W, H) * 0.42;

  ctx.fillStyle = '#0f1117';
  ctx.fillRect(0, 0, W, H);

  // Axes
  var axes = [
    [0.5,0.5,0.5, 1,0.5,0.5, '#ff6b35', 'Urgency'],
    [0.5,0.5,0.5, 0.5,1,0.5, '#00b894', 'Land'],
    [0.5,0.5,0.5, 0.5,0.5,1, '#a29bfe', 'PR Fit'],
    [0,0.5,0.5,   0.5,0.5,0.5, '#2a2d37', ''],
    [0.5,0,0.5,   0.5,0.5,0.5, '#2a2d37', ''],
    [0.5,0.5,0,   0.5,0.5,0.5, '#2a2d37', ''],
  ];
  axes.forEach(function(ax) {{
    var p0=project(ax[0],ax[1],ax[2]), p1=project(ax[3],ax[4],ax[5]);
    ctx.strokeStyle=ax[6]; ctx.lineWidth=ax[7]?1.5:0.5;
    ctx.globalAlpha=ax[7]?0.6:0.2;
    ctx.beginPath();
    ctx.moveTo(cx+p0.sx*scale, cy-p0.sy*scale);
    ctx.lineTo(cx+p1.sx*scale, cy-p1.sy*scale);
    ctx.stroke();
    if (ax[7]) {{
      ctx.globalAlpha=0.8; ctx.fillStyle=ax[6];
      ctx.font='bold 11px monospace';
      ctx.fillText(ax[7], cx+p1.sx*scale+6, cy-p1.sy*scale+4);
    }}
    ctx.globalAlpha=1;
  }});

  // Tick marks at 0 and 100 ends
  var tickPts = [
    [1,0.5,0.5,'#ff6b35','100'],[0,0.5,0.5,'#ff6b35','0'],
    [0.5,1,0.5,'#00b894','100'],[0.5,0,0.5,'#00b894','0'],
    [0.5,0.5,1,'#a29bfe','100'],[0.5,0.5,0,'#a29bfe','0'],
  ];
  tickPts.forEach(function(t) {{
    var p=project(t[0],t[1],t[2]);
    ctx.fillStyle=t[3]; ctx.globalAlpha=0.4;
    ctx.font='9px monospace';
    ctx.fillText(t[4], cx+p.sx*scale+3, cy-p.sy*scale+3);
    ctx.globalAlpha=1;
  }});

  // Sort by depth, draw points
  var sorted = pts.filter(function(p){{return p.color !== null;}})
    .map(function(p){{var pr=project(p.x,p.y,p.z);return{{p:p,pr:pr}};}})
    .sort(function(a,b){{return b.pr.depth-a.pr.depth;}});

  sorted.forEach(function(item) {{
    var pr=item.pr, p=item.p;
    var px=cx+pr.sx*scale, py=cy-pr.sy*scale;
    var r = p.hasOSM ? 3.5 : 2.5;
    ctx.beginPath(); ctx.arc(px,py,r,0,Math.PI*2);
    ctx.fillStyle=p.color;
    ctx.globalAlpha=p.hasOSM?0.85:0.4;
    ctx.fill();
    ctx.globalAlpha=1;
  }});
}}

// ── Tooltip ───────────────────────────────────────────────────────────────────
function findNearest(mx, my) {{
  var canvas=document.getElementById('c');
  var W=canvas.width, H=canvas.height, cx=W/2, cy=H/2;
  var scale=Math.min(W,H)*0.42;
  var best=null, bd=16;
  pts.forEach(function(p) {{
    if (p.color===null) return;
    var pr=project(p.x,p.y,p.z);
    var px=cx+pr.sx*scale, py=cy-pr.sy*scale;
    var d=Math.sqrt((mx-px)*(mx-px)+(my-py)*(my-py));
    if(d<bd){{best=p;bd=d;}}
  }});
  return best;
}}

function showInfo(p) {{
  if (!p) {{
    document.getElementById('infoBox').innerHTML =
      '<div class="info-sub" style="color:var(--text2)">Hover over a dot to see institution details.</div>';
    return;
  }}
  var fitStr = p.hasOSM ? p.f.toFixed(1) : 'Not scraped';
  var crStr = p.cr ? (p.cr*100).toFixed(1)+'%' : '—';
  document.getElementById('infoBox').innerHTML =
    '<div class="info-title">'+p.name+'</div>'
    +'<div class="info-sub">'+p.state+' &nbsp;·&nbsp; '+p.cluster+'</div>'
    +'<div class="info-rows">'
    +'<div class="info-row"><span class="lbl">Urgency</span><span class="val">'+p.u+'</span></div>'
    +'<div class="info-row"><span class="lbl">Land</span><span class="val">'+p.l+'</span></div>'
    +'<div class="info-row"><span class="lbl">PR Fit</span><span class="val">'+fitStr+'</span></div>'
    +'<div class="info-row"><span class="lbl">Distress</span><span class="val">'+p.d+'</span></div>'
    +'<div class="info-row"><span class="lbl">Closure Risk</span><span class="val">'+crStr+'</span></div>'
    +(p.a>0?'<div class="info-row"><span class="lbl">Acreage</span><span class="val">'+p.a+' ac</span></div>':'')
    +'</div>';
}}

// ── Events ────────────────────────────────────────────────────────────────────
var canvas = document.getElementById('c');

canvas.addEventListener('mousedown', function(e){{drag=true;lx=e.clientX;ly=e.clientY;}});
window.addEventListener('mouseup', function(){{drag=false;}});

window.addEventListener('mousemove', function(e) {{
  if (drag) {{
    camera.rotY += (e.clientX-lx)*0.007;
    camera.rotX += (e.clientY-ly)*0.007;
    lx=e.clientX; ly=e.clientY;
    draw();
  }}
  var wrap=document.getElementById('canvasWrap');
  var rect=wrap.getBoundingClientRect();
  var mx=e.clientX-rect.left, my=e.clientY-rect.top;
  var nearest = findNearest(mx, my);
  showInfo(nearest);
  var tip=document.getElementById('tip');
  if (nearest) {{
    tip.style.display='block';
    tip.style.left=(mx+14)+'px'; tip.style.top=(my-10)+'px';
    tip.innerHTML='<div class="tt-name">'+nearest.name+'</div>'
      +'<div class="tt-row">'+nearest.state+' · '+nearest.cluster+'</div>'
      +'<div class="tt-row">U:<b>'+nearest.u+'</b> L:<b>'+nearest.l+'</b> Fit:<b>'+(nearest.hasOSM?nearest.f.toFixed(1):'--')+'</b></div>'
      +(nearest.a>0?'<div class="tt-row">'+nearest.a+' ac</div>':'');
  }} else {{
    tip.style.display='none';
  }}
}});

canvas.addEventListener('wheel', function(e) {{
  e.preventDefault();
  camera.zoom *= e.deltaY>0 ? 0.93 : 1.07;
  camera.zoom = Math.max(0.3, Math.min(5, camera.zoom));
  draw();
}}, {{passive:false}});

// Touch
var ltd=0;
canvas.addEventListener('touchstart',function(e){{
  if(e.touches.length===1){{drag=true;lx=e.touches[0].clientX;ly=e.touches[0].clientY;}}
  if(e.touches.length===2)ltd=Math.hypot(e.touches[0].clientX-e.touches[1].clientX,e.touches[0].clientY-e.touches[1].clientY);
  e.preventDefault();
}},{{passive:false}});
canvas.addEventListener('touchmove',function(e){{
  if(e.touches.length===1&&drag){{
    camera.rotY+=(e.touches[0].clientX-lx)*0.007;
    camera.rotX+=(e.touches[0].clientY-ly)*0.007;
    lx=e.touches[0].clientX;ly=e.touches[0].clientY;draw();
  }}
  if(e.touches.length===2){{
    var d=Math.hypot(e.touches[0].clientX-e.touches[1].clientX,e.touches[0].clientY-e.touches[1].clientY);
    camera.zoom*=d/ltd;camera.zoom=Math.max(0.3,Math.min(5,camera.zoom));ltd=d;draw();
  }}
  e.preventDefault();
}},{{passive:false}});
canvas.addEventListener('touchend',function(){{drag=false;}});

document.getElementById('btnReset').addEventListener('click', function(){{
  camera={{rotX:0.45,rotY:-0.6,zoom:1.0}};draw();
}});

document.getElementById('btnIPEDS').addEventListener('click', function(){{
  src='ipeds'; hidden={{}};
  this.classList.add('active');
  document.getElementById('btn990').classList.remove('active');
  buildPoints(); buildLegend(); draw();
}});
document.getElementById('btn990').addEventListener('click', function(){{
  src='990'; hidden={{}};
  this.classList.add('active');
  document.getElementById('btnIPEDS').classList.remove('active');
  buildPoints(); buildLegend(); draw();
}});

window.addEventListener('resize', draw);

// ── Init ──────────────────────────────────────────────────────────────────────
buildPoints();
buildLegend();
draw();
</script>
</body>
</html>"""

if __name__ == '__main__':
    if not os.path.exists(CLUSTERS_HTML):
        print(f"ERROR: {CLUSTERS_HTML} not found. Run clusters.py first.")
        sys.exit(1)
    print(f"Reading {CLUSTERS_HTML}...")
    ipeds_json, hb990_json = extract_data(CLUSTERS_HTML)
    print(f"  IPEDS: {len(json.loads(ipeds_json))} clusters")
    print(f"  990:   {len(json.loads(hb990_json))} clusters")
    print(f"Generating {OUTPUT}...")
    html = build_html(ipeds_json, hb990_json)
    with open(OUTPUT, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  Done. ({len(html)//1024}KB)")
    print(f"  → {OUTPUT}")