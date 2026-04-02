"""
news_scraper.py — Hummingbird News Pre-Scraper
===============================================
Fetches Google News RSS directly (no proxy needed — Python has no CORS restriction).
Writes results to news_cache.json with 30-day TTL on scrape date.

Usage:
    python news_scraper.py                        # scrape all stale IPEDS institutions
    python news_scraper.py --limit 50             # test with 50 institutions
    python news_scraper.py --force                # ignore TTL, rescrape everything
    python news_scraper.py --institution "Sterling College"
"""

import csv, json, os, re, sys, time, argparse
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.parse import quote
from urllib.error import URLError, HTTPError
import xml.etree.ElementTree as ET

csv.field_size_limit(100 * 1024 * 1024)

# =============================================================================
# CONFIGURATION
# =============================================================================

MASTER_FILE  = 'hv_master_data/data/Hummingbird_Master_Combined_v6.csv'
CACHE_FILE   = 'news_cache.json'
TTL_DAYS     = 30
DELAY_SECS   = 1.0      # polite delay between requests
MAX_ARTICLES = 8
MAX_RETRIES  = 3
TIMEOUT      = 12

# Direct Google News RSS — no proxy needed in Python
GOOGLE_NEWS  = 'https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
    'Accept': 'application/rss+xml, application/xml, text/xml, */*',
    'Accept-Language': 'en-US,en;q=0.9',
}

# =============================================================================
# HELPERS
# =============================================================================

def cache_key(uid, name, city, state):
    return f"{uid}||{name}||{city}||{state}"


def build_query(name, city, state):
    """Build a focused news search query that biases toward institutional/financial news."""
    loc = ' '.join(filter(None, [city, state]))
    # Add context terms to push results toward institutional news,
    # away from sports/obituaries. Google treats these as soft signals.
    context = 'college OR university OR campus OR enrollment OR closing OR merger OR financial'
    return f'"{name}" {loc} ({context})'


# ---------------------------------------------------------------------------
# Post-fetch article filter — drop sports, obituaries, and noise
# ---------------------------------------------------------------------------

# Title-level blocklist — if any of these appear in the title, drop the article
_TITLE_BLOCK = [
    # Sports
    'football', 'basketball', 'baseball', 'softball', 'soccer', 'volleyball',
    'tennis', 'golf', 'track and field', 'swimming', 'lacrosse', 'wrestling',
    'hockey', 'cross country', 'field hockey', 'rowing crew',
    'coach', 'roster', 'recruit', 'playoff', 'championship', 'ncaa tournament',
    'bowl game', 'pitcher', 'quarterback', 'touchdown', 'halftime', 'scoreboard',
    'batting', 'innings', 'free throw', 'three-pointer', 'rushing yards',
    'shutout', 'overtime', 'seed ', 'seeded', 'game day', 'tipoff', 'varsity',
    'all-conference', 'all-american', 'mvp', 'player of the week',
    'nfl draft', 'nba draft', 'spring practice', 'fall camp',
    'sweeps', 'drops series', 'wins series', 'takes series',
    'win over', 'loss to', 'defeat ', 'defeated by', 'edges ', 'tops ',
    'routs ', 'blanks ', 'rallies past', 'holds off', 'cruises past',
    'walks off', 'run-rule', 'no-hit', 'perfect game', 'game recap',
    'matchup', 'rivalry', 'homestand', 'road trip', 'doubleheader',
    'signing day', 'transfer portal', 'nil deal', 'redshirt',
    # Obituaries
    'obituary', 'obit ', ' obit', 'dies at', 'died at', 'death of',
    'funeral', 'memorial service', 'passed away', 'in memoriam',
    'laid to rest', 'remembered for', 'mourns loss',
    # Weather/noise
    'weather forecast', 'traffic update', 'lottery', 'horoscope', 'recipe',
    'real estate listing', 'stock price',
    # Crime (unless it involves the institution itself — hard to tell, so be conservative)
    'arrested for', 'murder charge', 'drug bust', 'robbery suspect',
    # Generic local news
    'score:', 'nfl ', 'nba ', 'nhl ', 'mlb ',
]

# Source-level blocklist — these sources rarely produce relevant institutional news
_SOURCE_BLOCK = [
    'espn', 'sports illustrated', 'bleacher report', 'the athletic',
    'yahoo sports', '247sports', 'rivals.com', 'on3.com',
    'legacy.com', 'echovita', 'tributes.com',
]


def filter_articles(articles, institution_name=''):
    """
    Filter out sports, obituaries, and noise from scraped articles.
    Returns only articles likely relevant to institutional/financial news.
    """
    if not articles:
        return articles

    filtered = []
    name_lower = institution_name.lower()

    for a in articles:
        title_lower = (a.get('title', '') or '').lower()
        source_lower = (a.get('source', '') or '').lower()
        snippet_lower = (a.get('snippet', '') or '').lower()

        # Source blocklist
        if any(s in source_lower for s in _SOURCE_BLOCK):
            continue

        # Title blocklist
        if any(term in title_lower for term in _TITLE_BLOCK):
            continue

        # Snippet-level sports catch (title might be vague like "Big Win for Eagles")
        sports_snippet = ['final score', 'yards rushing', 'points scored', 'batting average',
                          'runs batted', 'three-point', 'field goal', 'free throw']
        if any(s in snippet_lower for s in sports_snippet):
            continue

        # Obituary pattern: "Name Obituary - City, ST"
        if re.match(r'^[\w\s\.]+ obituary', title_lower):
            continue

        filtered.append(a)

    return filtered[:MAX_ARTICLES]


def fetch_rss(query, retries=MAX_RETRIES):
    """Fetch Google News RSS directly. Returns XML string or None."""
    url = GOOGLE_NEWS.format(q=quote(query))
    for attempt in range(retries):
        try:
            req = Request(url, headers=HEADERS)
            with urlopen(req, timeout=TIMEOUT) as resp:
                return resp.read().decode('utf-8', errors='replace')
        except HTTPError as e:
            if e.code == 429:
                # Rate limited — back off longer
                wait = (attempt + 1) * 10
                print(f'      Rate limited, waiting {wait}s...')
                time.sleep(wait)
            else:
                return None
        except (URLError, Exception) as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None
    return None


def parse_rss(xml_text):
    """Parse RSS XML into list of article dicts."""
    if not xml_text:
        return []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    articles = []
    for item in root.findall('.//item'):
        def gt(tag):
            el = item.find(tag)
            return el.text.strip() if el is not None and el.text else ''

        raw_title = gt('title')
        link      = gt('link')
        pub_date  = gt('pubDate')
        snippet   = gt('description')

        # Strip HTML from snippet
        snippet = re.sub(r'<[^>]+>', ' ', snippet)
        snippet = re.sub(r'\s+', ' ', snippet).strip()

        # Split "Title - Source" format
        source = ''
        m = re.match(r'^(.*?)\s+-\s+([^-]+)$', raw_title)
        if m:
            raw_title = m.group(1).strip()
            source    = m.group(2).strip()

        # Parse date
        date_str = ''
        if pub_date:
            try:
                d = datetime.strptime(pub_date[:25].strip(), '%a, %d %b %Y %H:%M:%S')
                date_str = d.strftime('%Y-%m-%d')
            except Exception:
                date_str = pub_date[:10]

        if raw_title and link:
            articles.append({
                'title':     raw_title,
                'url':       link,
                'snippet':   snippet[:400],
                'published': date_str,
                'source':    source,
            })

    return articles[:MAX_ARTICLES]


def is_stale(entry, ttl_days, force=False):
    if force or not entry or 'scraped_at' not in entry:
        return True
    try:
        scraped = datetime.strptime(entry['scraped_at'], '%Y-%m-%d')
        return datetime.now() - scraped > timedelta(days=ttl_days)
    except Exception:
        return True


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit',       type=int, default=None)
    parser.add_argument('--force',       action='store_true')
    parser.add_argument('--institution', type=str, default=None)
    args = parser.parse_args()

    # Load master — IPEDS only
    print(f'Loading master: {MASTER_FILE}')
    institutions = []
    with open(MASTER_FILE, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            if row.get('data_source', '') != 'IPEDS':
                continue
            institutions.append({
                'uid':   row.get('unitid', '').strip(),
                'name':  row.get('institution_name', '').strip(),
                'city':  row.get('city', '').strip(),
                'state': row.get('state', '').strip(),
            })
    print(f'  {len(institutions):,} IPEDS institutions')

    if args.institution:
        institutions = [i for i in institutions
                        if args.institution.lower() in i['name'].lower()]
        print(f'  Filtered to {len(institutions)} matching "{args.institution}"')

    # Load cache
    cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, encoding='utf-8') as f:
                cache = json.load(f)
            print(f'  Cache: {len(cache):,} existing entries')
        except Exception as e:
            print(f'  Cache load failed ({e}) — starting fresh')

    # Determine what needs scraping
    to_scrape = []
    skipped = 0
    for inst in institutions:
        key = cache_key(inst['uid'], inst['name'], inst['city'], inst['state'])
        if is_stale(cache.get(key), TTL_DAYS, force=args.force):
            to_scrape.append((key, inst))
        else:
            skipped += 1

    print(f'\n  Up to date (skipping): {skipped:,}')
    print(f'  Stale/missing:         {len(to_scrape):,}')

    if args.limit:
        to_scrape = to_scrape[:args.limit]
        print(f'  Limited to:            {args.limit}')

    if not to_scrape:
        print('\nAll up to date — nothing to scrape.')
        return

    today = datetime.now().strftime('%Y-%m-%d')
    success, failed, empty = 0, 0, 0

    print(f'\nScraping {len(to_scrape):,} institutions...')
    print('-' * 65)

    for i, (key, inst) in enumerate(to_scrape):
        query    = build_query(inst['name'], inst['city'], inst['state'])
        xml_text = fetch_rss(query)
        articles = parse_rss(xml_text)
        articles = filter_articles(articles, institution_name=inst['name'])

        cache[key] = {
            'scraped_at': today,
            'name':       inst['name'],
            'city':       inst['city'],
            'state':      inst['state'],
            'uid':        inst['uid'],
            'articles':   articles,
        }

        if xml_text is None:
            failed += 1
            status = '✗ ERROR'
        elif not articles:
            empty += 1
            status = '— no articles'
        else:
            success += 1
            status = f'✓ {len(articles)} articles'

        pct = (i + 1) / len(to_scrape) * 100
        print(f'  [{i+1:>5}/{len(to_scrape)} {pct:4.0f}%] {inst["name"][:45]:<45} {status}')

        # Checkpoint every 100
        if (i + 1) % 100 == 0:
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, separators=(',', ':'))
            print(f'  ── checkpoint saved ({len(cache):,} entries) ──')

        time.sleep(DELAY_SECS)

    # Final save
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, separators=(',', ':'))

    cache_mb = os.path.getsize(CACHE_FILE) / (1024 * 1024)

    print(f'\n{"="*65}')
    print('NEWS SCRAPE COMPLETE')
    print(f'{"="*65}')
    print(f'  Success:        {success:,}')
    print(f'  No articles:    {empty:,}')
    print(f'  Errors:         {failed:,}')
    print(f'  Cache entries:  {len(cache):,}')
    print(f'  Cache size:     {cache_mb:.1f} MB')
    print(f'{"="*65}')
    print(f'\nNext step: python hv_master_data/data/master_standalone.py')


if __name__ == '__main__':
    main()