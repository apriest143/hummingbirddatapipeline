#!/usr/bin/env python3
"""
osm_land_enrichment.py
─────────────────────────────────────────────────────────────────────────────
Queries OSM Overpass API for environment / land-value features within a
configurable radius of each institution in the Hummingbird master CSV.

OUTPUT COLUMNS (written to standalone results CSV):
  osm_env_features      pipe-delimited feature strings (all named, then "N unnamed X")
  osm_env_categories    pipe-delimited category keys present
  osm_env_count         total distinct named + unnamed features found
  osm_env_radius_miles  radius searched
  osm_env_queried_at    ISO timestamp of query

CATEGORIES (land-development focused):
  water               coastline, lakes, rivers, beaches, wetlands, springs, marinas
  winter              ski resorts, lifts, ice facilities
  outdoor_recreation  hiking, MTB, cycling, climbing, kayaking, fishing, hunting,
                      equestrian, golf, surfing, rowing, camping, sports
  protected_land      national parks, protected areas, nature reserves, forests,
                      conservation land, national forests
  natural_features    peaks, cliffs, caves, glaciers, volcanoes, woodland, scrub, heath
  tourism_leisure     resorts, theme parks, zoos, museums, viewpoints, attractions,
                      botanical gardens, casinos, hot springs
  infrastructure      airports, train stations, ferry terminals

RESUME BEHAVIOUR:
  --resume skips rows with a valid osm_env_queried_at timestamp.
  ERROR rows are always re-queued (retried on every resume run).
  Empty results (genuinely no features) are kept as-is — empty ≠ error.

USAGE:
  python3 osm_land_enrichment.py \\
      --input  Hummingbird_Master_Combined_v6.csv \\
      --output osm_land_results.csv \\
      [--radius 20]          # miles, default 20
      [--sleep  1.5]         # seconds between requests, default 1.5
      [--resume]             # continue from last run, retry errors
      [--limit  100]         # only process first N rows (for testing)
      [--data-source IPEDS]  # filter to IPEDS or Hummingbird_990 only
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import csv
import json
import os
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone

# ── Configuration ─────────────────────────────────────────────────────────────

OVERPASS_URL        = "https://overpass-api.de/api/interpreter"
OVERPASS_URL_BACKUP = "https://lz4.overpass-api.de/api/interpreter"

MILES_TO_METERS    = 1609.344
DEFAULT_RADIUS_MILES = 20
DEFAULT_SLEEP      = 1.5
MAX_RETRIES        = 3
RETRY_BACKOFF      = [5, 15, 45]   # seconds between retries

# ── Feature definitions ────────────────────────────────────────────────────────
# (tag_key, tag_value, display_label, category, osm_element_types)
#
# category keys:
#   water | winter | outdoor_recreation | protected_land | natural_features |
#   tourism_leisure | infrastructure

FEATURES = [

    # ── WATER ─────────────────────────────────────────────────────────────────
    ("natural",   "coastline",       "Coastline",              "water",              "w"),
    ("natural",   "water",           "Lake/Pond",              "water",              "w"),
    ("waterway",  "river",           "River",                  "water",              "w"),
    ("natural",   "beach",           "Beach",                  "water",              "nw"),
    ("waterway",  "canal",           "Canal",                  "water",              "w"),
    ("waterway",  "stream",          "Stream",                 "water",              "w"),
    ("natural",   "wetland",         "Wetland",                "water",              "w"),
    ("leisure",   "marina",          "Marina",                 "water",              "nw"),
    ("natural",   "spring",          "Natural Spring",         "water",              "n"),
    ("natural",   "hot_spring",      "Hot Spring",             "water",              "n"),

    # ── WINTER / SKI ──────────────────────────────────────────────────────────
    ("leisure",   "ski_resort",      "Ski Resort",             "winter",             "nwr"),
    ("landuse",   "winter_sports",   "Winter Sports Area",     "winter",             "w"),
    ("piste:type","downhill",        "Downhill Ski Run",       "winter",             "w"),
    ("piste:type","nordic",          "Nordic Ski Trail",       "winter",             "w"),
    ("aerialway", "gondola",         "Gondola",                "winter",             "w"),
    ("aerialway", "chair_lift",      "Chair Lift",             "winter",             "w"),
    ("aerialway", "drag_lift",       "Drag Lift",              "winter",             "w"),
    ("aerialway", "cable_car",       "Cable Car",              "winter",             "w"),
    ("sport",     "ice_skating",     "Ice Skating",            "winter",             "n"),
    ("sport",     "skiing",          "Skiing Facility",        "winter",             "n"),
    ("sport",     "ice_hockey",      "Ice Hockey Rink",        "winter",             "n"),

    # ── OUTDOOR RECREATION ────────────────────────────────────────────────────
    ("route",     "hiking",          "Hiking Trail",           "outdoor_recreation", "r"),
    ("route",     "mtb",             "Mountain Bike Trail",    "outdoor_recreation", "r"),
    ("route",     "bicycle",         "Cycling Route",          "outdoor_recreation", "r"),
    ("route",     "horse",           "Equestrian Trail",       "outdoor_recreation", "r"),
    ("route",     "canoe",           "Canoe Route",            "outdoor_recreation", "r"),
    ("highway",   "cycleway",        "Dedicated Cycleway",     "outdoor_recreation", "w"),
    ("highway",   "bridleway",       "Bridleway",              "outdoor_recreation", "w"),
    ("sport",     "climbing",        "Rock Climbing",          "outdoor_recreation", "n"),
    ("sport",     "surfing",         "Surfing",                "outdoor_recreation", "n"),
    ("sport",     "kayaking",        "Kayaking",               "outdoor_recreation", "n"),
    ("sport",     "rowing",          "Rowing",                 "outdoor_recreation", "n"),
    ("sport",     "fishing",         "Fishing",                "outdoor_recreation", "n"),
    ("sport",     "hunting",         "Hunting",                "outdoor_recreation", "n"),
    ("sport",     "golf",            "Golf",                   "outdoor_recreation", "n"),
    ("leisure",   "golf_course",     "Golf Course",            "outdoor_recreation", "w"),
    ("leisure",   "sports_centre",   "Sports Centre",          "outdoor_recreation", "nw"),
    ("leisure",   "horse_riding",    "Horse Riding",           "outdoor_recreation", "n"),
    ("sport",     "swimming",        "Swimming Facility",      "outdoor_recreation", "n"),
    ("tourism",   "camp_site",       "Campground",             "outdoor_recreation", "n"),
    ("tourism",   "wilderness_hut",  "Wilderness Hut",         "outdoor_recreation", "n"),

    # ── PROTECTED LAND ────────────────────────────────────────────────────────
    ("boundary",  "national_park",   "National Park",          "protected_land",     "r"),
    ("boundary",  "protected_area",  "Protected Area",         "protected_land",     "r"),
    ("leisure",   "nature_reserve",  "Nature Reserve",         "protected_land",     "nwr"),
    ("landuse",   "forest",          "Forest",                 "protected_land",     "w"),
    ("boundary",  "forest",          "National Forest",        "protected_land",     "r"),
    ("landuse",   "conservation",    "Conservation Land",      "protected_land",     "w"),

    # ── NATURAL FEATURES ──────────────────────────────────────────────────────
    ("natural",   "wood",            "Woodland",               "natural_features",   "w"),
    ("natural",   "peak",            "Mountain Peak",          "natural_features",   "n"),
    ("natural",   "volcano",         "Volcano",                "natural_features",   "n"),
    ("natural",   "cliff",           "Cliff",                  "natural_features",   "w"),
    ("natural",   "cave_entrance",   "Cave",                   "natural_features",   "n"),
    ("natural",   "glacier",         "Glacier",                "natural_features",   "w"),
    ("natural",   "scrub",           "Scrubland",              "natural_features",   "w"),
    ("natural",   "heath",           "Heathland",              "natural_features",   "w"),

    # ── TOURISM & LEISURE ─────────────────────────────────────────────────────
    ("tourism",   "resort",          "Resort",                 "tourism_leisure",    "nwr"),
    ("tourism",   "theme_park",      "Theme Park",             "tourism_leisure",    "nwr"),
    ("tourism",   "zoo",             "Zoo",                    "tourism_leisure",    "nwr"),
    ("tourism",   "aquarium",        "Aquarium",               "tourism_leisure",    "n"),
    ("tourism",   "museum",          "Museum",                 "tourism_leisure",    "n"),
    ("tourism",   "attraction",      "Tourist Attraction",     "tourism_leisure",    "n"),
    ("tourism",   "viewpoint",       "Scenic Viewpoint",       "tourism_leisure",    "n"),
    ("historic",  "monument",        "Historic Monument",      "tourism_leisure",    "n"),
    ("leisure",   "park",            "Park",                   "tourism_leisure",    "w"),
    ("leisure",   "garden",          "Botanical Garden",       "tourism_leisure",    "w"),
    ("amenity",   "casino",          "Casino",                 "tourism_leisure",    "n"),

    # ── INFRASTRUCTURE ────────────────────────────────────────────────────────
    ("aeroway",   "aerodrome",       "Airport",                "infrastructure",     "nwr"),
    ("railway",   "station",         "Train Station",          "infrastructure",     "n"),
    ("amenity",   "ferry_terminal",  "Ferry Terminal",         "infrastructure",     "n"),
]

# Build lookup: (tag_key, tag_value) → (display_label, category)
TAG_LOOKUP = {(f[0], f[1]): (f[2], f[3]) for f in FEATURES}

# Canonical category order for display
CATEGORY_ORDER = [
    "water", "winter", "outdoor_recreation", "protected_land",
    "natural_features", "tourism_leisure", "infrastructure",
]


# ── Overpass query builder ─────────────────────────────────────────────────────

def build_overpass_query(lat, lon, radius_m):
    parts = []
    for key, val, label, cat, elem_types in FEATURES:
        for et in elem_types:
            type_word = {"n": "node", "w": "way", "r": "relation"}.get(et, "nwr")
            parts.append(f'  {type_word}["{key}"="{val}"](around:{radius_m},{lat},{lon});')

    return f"""[out:json][timeout:60];
(
{chr(10).join(parts)}
);
out center tags;"""


def fetch_overpass(query, use_backup=False):
    url  = OVERPASS_URL_BACKUP if use_backup else OVERPASS_URL
    data = urllib.parse.urlencode({"data": query}).encode()
    req  = urllib.request.Request(
        url, data=data, method="POST",
        headers={"User-Agent": "HummingbirdLandEnrichment/1.0"},
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        return json.loads(resp.read().decode())


# ── Result processing ──────────────────────────────────────────────────────────

def process_elements(elements):
    """
    Classify each OSM element into its feature label + category.

    Dedup strategy per label:
      - Collect ALL distinct named features (no cap)
      - Unnamed elements are counted; displayed as "N unnamed Streams" etc.
    """
    # label → {category, named: list[str], unnamed_count: int}
    by_label = {}

    for el in elements:
        tags = el.get("tags", {})

        for (key, val), (label, cat) in TAG_LOOKUP.items():
            if tags.get(key) == val:
                name = (
                    tags.get("name")
                    or tags.get("name:en")
                    or tags.get("official_name")
                    or ""
                ).strip()

                if label not in by_label:
                    by_label[label] = {"category": cat, "named": [], "unnamed_count": 0}

                entry = by_label[label]
                if name and name not in entry["named"]:
                    entry["named"].append(name)
                elif not name:
                    entry["unnamed_count"] += 1

                break  # classify each element once only

    # Flatten into sorted feature list
    features = []
    for label, entry in by_label.items():
        named         = entry["named"]          # full list, no cap
        unnamed_count = entry["unnamed_count"]
        total         = len(named) + unnamed_count

        # Build display string
        if named and unnamed_count > 0:
            display = f"{', '.join(named)} + {unnamed_count} unnamed {label}s"
        elif named:
            display = ", ".join(named)          # all named, clean list
        else:
            display = f"{unnamed_count} unnamed {label}s"

        features.append({
            "label":    label,
            "category": entry["category"],
            "display":  display,
            "count":    total,
        })

    return features


def summarise(found_features, radius_miles):
    """Collapse processed features into output column values."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if not found_features:
        return {
            "osm_env_features":     "",
            "osm_env_categories":   "",
            "osm_env_count":        "0",
            "osm_env_radius_miles": str(radius_miles),
            "osm_env_queried_at":   now,
        }

    # Group by category, preserving CATEGORY_ORDER
    by_cat = {c: [] for c in CATEGORY_ORDER}
    for f in found_features:
        cat = f["category"]
        if cat not in by_cat:
            by_cat[cat] = []
        by_cat[cat].append(f)

    # Within each category, sort alphabetically by label
    feature_parts = []
    cats_present  = []
    for cat in CATEGORY_ORDER:
        entries = sorted(by_cat.get(cat, []), key=lambda x: x["label"])
        if not entries:
            continue
        cats_present.append(cat)
        for e in entries:
            # Format: "Display string [Label]" — label in brackets for context
            feature_parts.append(f"{e['display']} [{e['label']}]")

    total_count = sum(f["count"] for f in found_features)

    return {
        "osm_env_features":     " | ".join(feature_parts),
        "osm_env_categories":   " | ".join(cats_present),
        "osm_env_count":        str(total_count),
        "osm_env_radius_miles": str(radius_miles),
        "osm_env_queried_at":   now,
    }


# ── Main ───────────────────────────────────────────────────────────────────────

OUTPUT_COLS = [
    "osm_env_features", "osm_env_categories",
    "osm_env_count", "osm_env_radius_miles", "osm_env_queried_at",
]


def main():
    parser = argparse.ArgumentParser(description="OSM land environment enrichment")
    parser.add_argument("--input",       default="hv_master_data/data/Hummingbird_Master_Combined_v6.csv")
    parser.add_argument("--output",      default="hv_master_data/acreage_scripts/osm_land_results.csv")
    parser.add_argument("--radius",      type=float, default=DEFAULT_RADIUS_MILES)
    parser.add_argument("--sleep",       type=float, default=DEFAULT_SLEEP)
    parser.add_argument("--resume",      action="store_true",
                        help="Skip completed rows; ERROR rows are always retried")
    parser.add_argument("--limit",       type=int, default=None)
    parser.add_argument("--data-source", default=None,
                        help="Filter to 'IPEDS' or 'Hummingbird_990' only")
    args = parser.parse_args()

    radius_m = args.radius * MILES_TO_METERS

    # ── Load input ──────────────────────────────────────────────────────────
    print(f"Loading {args.input}...")
    with open(args.input, newline="", encoding="utf-8", errors="replace") as f:
        all_rows = list(csv.DictReader(f))

    if args.data_source:
        all_rows = [r for r in all_rows
                    if r.get("data_source", "").strip() == args.data_source.strip()]
        print(f"Filtered to {args.data_source}: {len(all_rows)} rows")

    rows = [r for r in all_rows
            if r.get("latitude", "").strip() and r.get("longitude", "").strip()]
    skipped = len(all_rows) - len(rows)
    if skipped:
        print(f"Skipped {skipped} rows with missing coordinates")

    if args.limit:
        rows = rows[:args.limit]
        print(f"Limited to first {args.limit} rows")

    print(f"Total to process: {len(rows)}")

    # ── Load existing output for resume ─────────────────────────────────────
    # completed = names with a valid timestamp (not ERROR, not empty queried_at)
    # ERROR rows are excluded from completed → they get re-queued automatically
    completed      = set()
    existing_rows  = {}   # name → row dict (for rewriting file)

    if args.resume and os.path.exists(args.output):
        with open(args.output, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                name = row.get("institution_name", "")
                queried = row.get("osm_env_queried_at", "").strip()
                is_error = row.get("osm_env_features", "").strip() == "ERROR"
                if name:
                    existing_rows[name] = row
                    if queried and not is_error:
                        completed.add(name)

        retrying = sum(1 for n, r in existing_rows.items()
                       if r.get("osm_env_features", "").strip() == "ERROR")
        print(f"Resume: {len(completed)} completed (kept), "
              f"{retrying} ERROR rows will be retried")

    # ── Prepare output file ─────────────────────────────────────────────────
    out_fields = ["institution_name", "latitude", "longitude", "data_source"] + OUTPUT_COLS

    # On resume: rewrite file, keeping completed rows, dropping ERROR rows
    # (they will be re-added as new rows as we process them)
    out_fh = open(args.output, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_fh, fieldnames=out_fields, extrasaction="ignore")
    writer.writeheader()

    # Re-emit all successfully completed rows immediately
    if args.resume and existing_rows:
        for name, row in existing_rows.items():
            if name in completed:
                writer.writerow(row)
        out_fh.flush()
        print(f"Re-wrote {len(completed)} completed rows to output.")

    # ── Process rows ────────────────────────────────────────────────────────
    done   = 0
    errors = 0
    start  = time.time()

    for i, row in enumerate(rows):
        name = row.get("institution_name", "").strip()

        if name in completed:
            continue   # already done and re-written above

        lat = row.get("latitude", "").strip()
        lon = row.get("longitude", "").strip()

        try:
            lat_f, lon_f = float(lat), float(lon)
        except ValueError:
            print(f"  [SKIP] {name} — invalid coords ({lat},{lon})")
            continue

        query = build_overpass_query(lat_f, lon_f, radius_m)

        # Retry loop
        result_data = None
        for attempt in range(MAX_RETRIES):
            try:
                result_data = fetch_overpass(query, use_backup=(attempt > 0))
                break
            except urllib.error.HTTPError as e:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                print(f"  [HTTP {e.code}] {name} — waiting {wait}s...")
                time.sleep(wait)
            except Exception as e:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                print(f"  [ERR attempt {attempt+1}] {name} — {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(wait)

        out_row = {
            "institution_name": name,
            "latitude":         lat,
            "longitude":        lon,
            "data_source":      row.get("data_source", ""),
        }

        if result_data is None:
            errors += 1
            out_row.update({
                "osm_env_features":     "ERROR",
                "osm_env_categories":   "",
                "osm_env_count":        "",
                "osm_env_radius_miles": str(args.radius),
                "osm_env_queried_at":   "",   # no timestamp = will be retried next resume
            })
            print(f"  [ERROR] {name} — will retry on next --resume run")
        else:
            found   = process_elements(result_data.get("elements", []))
            summary = summarise(found, args.radius)
            out_row.update(summary)

            done   += 1
            elapsed = time.time() - start
            rate    = done / elapsed if elapsed > 0 else 0
            remaining = len(rows) - done - len(completed)
            eta_sec = remaining / rate if rate > 0 else 0
            eta_str = f"{eta_sec/3600:.1f}h" if eta_sec > 3600 else f"{eta_sec/60:.0f}m"

            feat_count = summary["osm_env_count"]
            cats       = summary["osm_env_categories"][:60] if summary["osm_env_categories"] else "—"
            print(f"  [{done:5d}/{len(rows)}] {name[:45]:45} | "
                  f"{feat_count:>5} features | {cats} | ETA {eta_str}")

        writer.writerow(out_row)
        out_fh.flush()
        time.sleep(args.sleep)

    out_fh.close()

    elapsed_total = time.time() - start
    print(f"\nDone. {done} processed, {errors} errors (will retry on next --resume).")
    print(f"Total time: {elapsed_total/3600:.1f}h  |  Output: {args.output}")
    print(f"\nNext step: run master_standalone.py to bake this data into index.html")



    """
    Read osm_land_results.csv and return a list of enriched dicts, one per
    institution that has valid (non-ERROR) data.

    Each dict:
      name, lat, lon, source, categories (list), count (int),
      radius_miles, queried_at,
      by_category: { cat: [ {label, display}, ... ], ... }
    """
    records = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            feat_str  = row.get("osm_env_features", "").strip()
            queried   = row.get("osm_env_queried_at", "").strip()
            if not queried or feat_str == "ERROR":
                continue   # skip errors and incomplete rows

            cats_raw = [c.strip() for c in row.get("osm_env_categories", "").split("|") if c.strip()]

            # Parse feature string back into structured list
            # Format: "display text [Label]" separated by " | "
            by_category = {}
            if feat_str:
                for part in feat_str.split(" | "):
                    part = part.strip()
                    if not part:
                        continue
                    # Extract label from trailing [Label]
                    label = ""
                    display = part
                    if part.endswith("]") and "[" in part:
                        bracket_start = part.rfind("[")
                        label   = part[bracket_start+1:-1].strip()
                        display = part[:bracket_start].strip()
                    # Find category for this label
                    cat = next(
                        (f[3] for f in FEATURES if f[2] == label),
                        "other"
                    )
                    if cat not in by_category:
                        by_category[cat] = []
                    by_category[cat].append({"label": label, "display": display})

            records.append({
                "name":        row.get("institution_name", ""),
                "lat":         row.get("latitude", ""),
                "lon":         row.get("longitude", ""),
                "source":      row.get("data_source", ""),
                "categories":  cats_raw,
                "count":       int(row.get("osm_env_count", "0") or "0"),
                "radius_miles": row.get("osm_env_radius_miles", "20"),
                "queried_at":  queried,
                "by_category": by_category,
            })
    return records


def export_js(csv_path, js_path):
    """
    Write osm_land_data.js — a single const assignment keyed by institution name.
    The map loads this file and uses window.OSM_LAND_DATA[name] at popup time.
    """
    records = parse_results_csv(csv_path)
    lookup  = {r["name"]: r for r in records}

    with open(js_path, "w", encoding="utf-8") as f:
        f.write("// Auto-generated by osm_features.py — do not edit manually\n")
        f.write(f"// {len(lookup)} institutions · generated {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}\n")
        f.write("const OSM_LAND_DATA = ")
        json.dump(lookup, f, ensure_ascii=False, separators=(",", ":"))
        f.write(";\n")

    print(f"  Wrote {len(lookup)} institutions to {js_path}")


def export_html(csv_path, html_path):
    """
    Write index.html — a self-contained dark-theme interactive report
    with searchable/filterable institution cards showing OSM land features.
    """
    records = parse_results_csv(csv_path)
    # Sort by category count desc, then name
    records.sort(key=lambda r: (-len(r["categories"]), r["name"]))

    cat_meta = {
        "water":              {"label": "Water",              "color": "#3b9edd", "icon": "💧"},
        "winter":             {"label": "Winter / Ski",       "color": "#a8d8f0", "icon": "⛷"},
        "outdoor_recreation": {"label": "Outdoor Recreation", "color": "#10b981", "icon": "🥾"},
        "protected_land":     {"label": "Protected Land",     "color": "#6bcb77", "icon": "🌲"},
        "natural_features":   {"label": "Natural Features",   "color": "#b8a9f0", "icon": "⛰"},
        "tourism_leisure":    {"label": "Tourism & Leisure",  "color": "#ffa502", "icon": "🎡"},
        "infrastructure":     {"label": "Infrastructure",     "color": "#8b8fa3", "icon": "✈"},
    }
    cat_order = ["water","winter","outdoor_recreation","protected_land",
                 "natural_features","tourism_leisure","infrastructure"]

    data_json = json.dumps(records, ensure_ascii=False)
    cat_meta_json = json.dumps(cat_meta, ensure_ascii=False)
    cat_order_json = json.dumps(cat_order, ensure_ascii=False)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    n = len(records)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Land Environment Intelligence — Hummingbird</title>
<style>
  :root {{
    --bg: #0d0f17; --surface: #151720; --surface2: #1c1f2e;
    --border: rgba(255,255,255,0.08); --text: #e8eaf2; --text2: #8b8fa3;
    --accent: #4f6ef7; --accent2: #74b9ff; --low: #10b981; --font: 'Inter',system-ui,sans-serif;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: var(--font); min-height: 100vh; }}
  .header {{ padding: 24px 24px 16px; border-bottom: 1px solid var(--border); display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 12px; }}
  .header h1 {{ font-size: 18px; font-weight: 700; background: linear-gradient(135deg,#10b981,#74b9ff); -webkit-background-clip: text; background-clip: text; -webkit-text-fill-color: transparent; }}
  .header p {{ font-size: 11px; color: var(--text2); margin-top: 3px; }}
  .header-stats {{ display: flex; gap: 16px; }}
  .stat {{ text-align: center; }}
  .stat-val {{ font-size: 20px; font-weight: 700; font-family: monospace; color: #10b981; }}
  .stat-lbl {{ font-size: 9px; color: var(--text2); text-transform: uppercase; letter-spacing: 0.5px; }}
  .controls {{ padding: 12px 24px; display: flex; flex-wrap: wrap; align-items: center; gap: 10px; border-bottom: 1px solid var(--border); background: var(--surface); position: sticky; top: 0; z-index: 10; }}
  .search-wrap {{ position: relative; display: flex; align-items: center; }}
  .search-wrap input {{ background: var(--bg); border: 1px solid var(--border); border-radius: 6px; color: var(--text); font-family: var(--font); font-size: 12px; padding: 7px 10px 7px 28px; width: 220px; outline: none; }}
  .search-wrap svg {{ position: absolute; left: 8px; color: var(--text2); pointer-events: none; }}
  .cat-btn {{ padding: 4px 10px; border-radius: 20px; border: 1px solid var(--border); background: var(--surface2); color: var(--text2); font-size: 10px; font-weight: 600; cursor: pointer; transition: all 0.15s; white-space: nowrap; }}
  .cat-btn.active {{ background: rgba(16,185,129,0.15); color: #10b981; border-color: rgba(16,185,129,0.4); }}
  .sort-select {{ background: var(--bg); border: 1px solid var(--border); border-radius: 6px; color: var(--text); font-family: var(--font); font-size: 11px; padding: 6px 10px; outline: none; cursor: pointer; margin-left: auto; }}
  .results-count {{ font-size: 10px; color: var(--text2); white-space: nowrap; }}
  .grid-wrap {{ padding: 16px 24px; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fill,minmax(340px,1fr)); gap: 12px; }}
  .card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; transition: border-color 0.15s; }}
  .card:hover {{ border-color: rgba(16,185,129,0.3); }}
  .card-head {{ padding: 12px 14px 10px; display: flex; align-items: flex-start; gap: 10px; cursor: pointer; }}
  .card-name {{ font-size: 12px; font-weight: 600; line-height: 1.3; flex: 1; }}
  .card-sub {{ font-size: 10px; color: var(--text2); margin-top: 2px; }}
  .cat-pills {{ padding: 0 14px 10px; display: flex; flex-wrap: wrap; gap: 4px; }}
  .cat-pill {{ padding: 2px 7px; border-radius: 10px; font-size: 9px; font-weight: 600; border: 1px solid transparent; }}
  .card-body {{ display: none; padding: 0 14px 12px; border-top: 1px solid var(--border); margin-top: 4px; padding-top: 10px; }}
  .card-body.open {{ display: block; }}
  .feat-section {{ margin-bottom: 10px; }}
  .feat-section-label {{ font-size: 9px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; color: var(--text2); margin-bottom: 5px; display: flex; align-items: center; gap: 5px; }}
  .feat-dot {{ width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }}
  .feat-item {{ padding: 5px 8px; background: var(--bg); border-radius: 5px; border-left: 2px solid var(--border); margin-bottom: 3px; font-size: 10px; line-height: 1.4; color: var(--text); }}
  .feat-type {{ font-size: 8px; color: var(--text2); text-transform: uppercase; letter-spacing: 0.3px; margin-top: 1px; }}
  .card-meta {{ font-size: 9px; color: var(--text2); padding: 6px 14px; border-top: 1px solid var(--border); }}
  .empty {{ text-align:center; padding:80px 24px; color:var(--text2); }}
  .count-badge {{ font-family: monospace; font-size: 11px; font-weight: 700; color: var(--text2); background: var(--bg); padding: 2px 6px; border-radius: 4px; white-space: nowrap; }}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>🌿 Land Environment Intelligence</h1>
    <p>OSM feature enrichment · 20-mile radius · {n} institutions analyzed · Generated {generated}</p>
  </div>
  <div class="header-stats">
    <div class="stat"><div class="stat-val" id="stat-shown">{n}</div><div class="stat-lbl">Shown</div></div>
    <div class="stat"><div class="stat-val">{n}</div><div class="stat-lbl">Total</div></div>
    <div class="stat"><div class="stat-val">7</div><div class="stat-lbl">Categories</div></div>
  </div>
</div>
<div class="controls">
  <div class="search-wrap">
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
    <input type="text" id="search" placeholder="Search institutions…" oninput="render()">
  </div>
  <div id="cat-btns"></div>
  <select class="sort-select" id="sort-sel" onchange="render()">
    <option value="cats-desc">Categories ↓</option>
    <option value="count-desc">Feature count ↓</option>
    <option value="name-asc">Name A–Z</option>
  </select>
  <div class="results-count" id="results-count"></div>
</div>
<div class="grid-wrap">
  <div class="grid" id="grid"></div>
  <div class="empty" id="empty" style="display:none">🔍 No institutions match your filters.</div>
</div>
<script>
const DATA       = {data_json};
const CAT_META   = {cat_meta_json};
const CAT_ORDER  = {cat_order_json};
var activeCat = null;

// Build category filter buttons
var btnWrap = document.getElementById('cat-btns');
CAT_ORDER.forEach(function(cat) {{
  var m   = CAT_META[cat];
  var btn = document.createElement('button');
  btn.className   = 'cat-btn';
  btn.textContent = m.icon + ' ' + m.label;
  btn.setAttribute('data-cat', cat);
  btn.style.setProperty('--dot', m.color);
  btn.onclick = function() {{
    activeCat = (activeCat === cat) ? null : cat;
    document.querySelectorAll('.cat-btn').forEach(function(b) {{
      b.classList.toggle('active', b.getAttribute('data-cat') === activeCat);
    }});
    render();
  }};
  btnWrap.appendChild(btn);
}});

function esc(s) {{ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }}

function render() {{
  var q    = document.getElementById('search').value.toLowerCase();
  var sort = document.getElementById('sort-sel').value;
  var rows = DATA.filter(function(r) {{
    if (q && r.name.toLowerCase().indexOf(q) < 0) return false;
    if (activeCat && r.categories.indexOf(activeCat) < 0) return false;
    return true;
  }});
  rows.sort(function(a,b) {{
    if (sort==='count-desc') return b.count - a.count;
    if (sort==='name-asc')   return a.name.localeCompare(b.name);
    return b.categories.length - a.categories.length;
  }});
  document.getElementById('stat-shown').textContent = rows.length;
  document.getElementById('results-count').textContent = rows.length + ' of ' + DATA.length;
  var grid = document.getElementById('grid');
  if (!rows.length) {{ grid.innerHTML=''; document.getElementById('empty').style.display=''; return; }}
  document.getElementById('empty').style.display='none';
  grid.innerHTML = rows.map(function(r, idx) {{
    var pills = r.categories.map(function(cat) {{
      var m = CAT_META[cat]||{{label:cat,color:'#8b8fa3',icon:''}};
      return '<span class="cat-pill" style="background:'+m.color+'22;color:'+m.color+';border-color:'+m.color+'44">'+m.icon+' '+m.label+'</span>';
    }}).join('');
    var bodyId = 'body-'+idx;
    var featHtml = CAT_ORDER.map(function(cat) {{
      var entries = r.by_category[cat];
      if (!entries||!entries.length) return '';
      var m = CAT_META[cat]||{{label:cat,color:'#8b8fa3',icon:''}};
      var items = entries.map(function(e) {{
        return '<div class="feat-item"><div>'+esc(e.display)+'</div><div class="feat-type">'+esc(e.label)+'</div></div>';
      }}).join('');
      return '<div class="feat-section">'
        +'<div class="feat-section-label"><span class="feat-dot" style="background:'+m.color+'"></span>'+m.icon+' '+m.label+'</div>'
        +items+'</div>';
    }}).join('');
    return '<div class="card">'
      +'<div class="card-head" onclick="toggleCard(\''+bodyId+'\')">'
      +'<div><div class="card-name">'+esc(r.name)+'</div>'
      +'<div class="card-sub">'+esc(r.source)+' · '+r.count+' features</div></div>'
      +'<span class="count-badge">'+r.categories.length+' cat</span>'
      +'</div>'
      +'<div class="cat-pills">'+pills+'</div>'
      +'<div class="card-body" id="'+bodyId+'">'+featHtml+'</div>'
      +'<div class="card-meta">Queried '+esc(r.queried_at.replace('T',' ').replace('Z',' UTC'))+'</div>'
      +'</div>';
  }}).join('');
}}

function toggleCard(id) {{
  var el = document.getElementById(id);
  if (el) el.classList.toggle('open');
}}

render();
</script>
</body>
</html>"""

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Wrote {n} institutions to {html_path}")


if __name__ == "__main__":
    main()