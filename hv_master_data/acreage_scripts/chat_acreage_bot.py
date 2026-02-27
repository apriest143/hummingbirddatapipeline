#!/usr/bin/env python3
"""
Free Automated Acreage Verifier - ENHANCED NOTES VERSION
=========================================================
Same as the Playwright version but with MUCH better notes extraction.

Key improvements:
- Extracts contextual information around acreage mentions
- Captures facility details (buildings, amenities, features)
- Notes year established, capacity, notable features
- Captures what the acreage includes (forest, lake, fields, etc.)
- Better source attribution
- Detects if property was sold/closed with details

Usage:
  python acreage_scraper_enhanced.py --input FILE --output FILE [--limit N] [--resume]

Requirements:
  pip install requests beautifulsoup4 lxml playwright
  playwright install
"""

import csv
import re
import time
import argparse
import os
import random
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Dict, Set
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


# =============================================================================
# CONFIGURATION
# =============================================================================

DELAY_BETWEEN_SEARCHES = 12.0
DELAY_BETWEEN_FETCHES = 2.0
JITTER_MAX = 4.0

MAX_RETRIES = 3
RETRY_DELAY = 8

MAX_PAGES_PER_INSTITUTION = 5
MAX_FETCH_PAGES = 3
REQUEST_TIMEOUT = 20

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
]

# Context scoring
BAD_CONTEXT = [
    "protected", "conservation", "easement", "watershed", "county", "region",
    "service area", "served", "across", "surrounding", "district", "park system"
]
GOOD_CONTEXT = [
    "campus", "main campus", "headquarters", "hq", "located on", "sits on",
    "grounds", "site", "property", "facility", "center"
]


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class Institution:
    name: str
    city: str
    state: str
    original_type: str
    estimated_acres: float
    priority: str
    detected_type: str

    verified_acres: Optional[float] = None
    confidence: Optional[str] = None
    source: Optional[str] = None
    status: Optional[str] = None
    notes: Optional[str] = None


# =============================================================================
# ENHANCED NOTES EXTRACTION
# =============================================================================

class NotesExtractor:
    """
    Extracts rich contextual notes about institutions from scraped text.
    """
    
    # Patterns to extract various details
    PATTERNS = {
        # Year established/founded
        'founded': [
            r'(?:founded|established|opened|started|began)\s+(?:in\s+)?(\d{4})',
            r'since\s+(\d{4})',
            r'(?:est\.|established)\s*(\d{4})',
        ],
        # Capacity/attendance
        'capacity': [
            r'(?:accommodates?|hosts?|serves?|capacity\s+(?:of|for)?)\s+(?:up\s+to\s+)?(\d+(?:,\d+)?)\s*(?:campers?|guests?|people|participants?|children|youth|students?)',
            r'(\d+(?:,\d+)?)\s*(?:campers?|guests?|beds?|participants?)\s*(?:per|each|a)\s*(?:session|week|summer)',
            r'(?:sleep|house|hold)\s+(?:up\s+to\s+)?(\d+(?:,\d+)?)',
        ],
        # Water features
        'water': [
            r'\b(lake|pond|river|stream|creek|waterfront|beach|pool|swimming)\b',
            r'(?:on|along|near)\s+(?:the\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:Lake|River|Creek|Pond)',
            r'(\d+)\s*(?:feet|ft|miles?|mi)\s+(?:of\s+)?(?:lake|water)?front',
        ],
        # Land features
        'terrain': [
            r'\b(wooded|forested|forest|meadow|prairie|mountain|hill|valley|canyon|desert)\b',
            r'(\d+)\s*(?:acres?)\s+(?:of\s+)?(?:forest|woods|timber|meadow|prairie|farmland|wetland)',
        ],
        # Facilities/amenities
        'facilities': [
            r'\b(lodge|cabin|dormitor|bunk\s*house|dining\s*hall|chapel|gymnasium|gym|pool|arena|stadium|field|court|stable|barn|amphitheater|pavilion|zipline|ropes?\s*course|climbing\s*wall|archery|lake|pond)\w*\b',
            r'(\d+)\s*(?:cabins?|lodges?|buildings?|dormitor(?:y|ies)|bunk\s*houses?)',
        ],
        # Sports/activities
        'activities': [
            r'\b(hiking|swimming|canoeing|kayaking|fishing|horseback|equestrian|archery|riflery|sailing|waterskiing|wakeboarding|rock\s*climbing|ropes?\s*course|zipline|mountain\s*biking)\b',
        ],
        # Sold/closed details
        'sold_closed': [
            r'(?:sold|closed|shuttered)\s+(?:in\s+)?(\d{4})',
            r'(?:sold|acquired)\s+(?:to|by)\s+([A-Z][A-Za-z\s&]+?)(?:\s+(?:in|for)\s+|\.|,)',
            r'(?:closed|ceased)\s+(?:operations?\s+)?(?:in\s+)?(\d{4})',
            r'property\s+(?:was\s+)?sold\s+for\s+\$?([\d,]+(?:\.\d+)?)\s*(?:million|M)?',
        ],
        # Special designations
        'designation': [
            r'\b(historic|national\s+register|landmark|accredited|ACA\s+accredited)\b',
        ],
    }
    
    # Keywords that indicate what acreage includes
    INCLUDES_KEYWORDS = [
        'forest', 'woods', 'timber', 'lake', 'pond', 'river', 'waterfront',
        'meadow', 'prairie', 'farmland', 'wetland', 'mountain', 'wilderness',
        'nature preserve', 'wildlife', 'trails', 'beach', 'shoreline'
    ]
    
    @classmethod
    def extract_context_around_acreage(cls, text: str, acres_value: float) -> str:
        """
        Extract the sentence or context surrounding an acreage mention.
        """
        text_lower = text.lower()
        acres_str = str(int(acres_value)) if acres_value == int(acres_value) else str(acres_value)
        
        # Find mentions of this acreage value
        patterns = [
            rf'{acres_str}\s*(?:-|\s)?acres?\b',
            rf'{acres_str}\s*-acre\b',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                # Extract ~150 chars before and after
                start = max(0, match.start() - 150)
                end = min(len(text), match.end() + 150)
                context = text[start:end].strip()
                
                # Clean up - try to get complete sentences
                context = re.sub(r'^[^A-Z]*', '', context)  # Start at capital
                context = re.sub(r'\s+', ' ', context)
                
                # Truncate to reasonable length
                if len(context) > 250:
                    context = context[:250] + "..."
                
                return context
        
        return ""
    
    @classmethod
    def extract_founded_year(cls, text: str) -> Optional[str]:
        """Extract when institution was founded/established."""
        text_lower = text.lower()
        for pattern in cls.PATTERNS['founded']:
            match = re.search(pattern, text_lower)
            if match:
                year = match.group(1)
                if 1800 <= int(year) <= 2025:
                    return year
        return None
    
    @classmethod
    def extract_capacity(cls, text: str) -> Optional[str]:
        """Extract capacity/attendance information."""
        text_lower = text.lower()
        for pattern in cls.PATTERNS['capacity']:
            match = re.search(pattern, text_lower)
            if match:
                return match.group(0).strip()
        return None
    
    @classmethod
    def extract_water_features(cls, text: str) -> List[str]:
        """Extract water-related features."""
        text_lower = text.lower()
        features = set()
        
        # Simple keywords
        water_words = ['lake', 'pond', 'river', 'stream', 'creek', 'waterfront', 
                       'beach', 'pool', 'swimming hole', 'spring']
        for word in water_words:
            if word in text_lower:
                features.add(word)
        
        # Named water bodies
        named_pattern = r'(?:on|along|at)\s+(?:the\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:Lake|River|Creek|Pond)'
        for match in re.finditer(named_pattern, text):
            features.add(match.group(0))
        
        return list(features)[:3]  # Limit to top 3
    
    @classmethod
    def extract_terrain(cls, text: str) -> List[str]:
        """Extract terrain/landscape features."""
        text_lower = text.lower()
        terrain = set()
        
        terrain_words = ['wooded', 'forested', 'forest', 'meadow', 'prairie', 
                        'mountain', 'hills', 'valley', 'canyon', 'rolling hills',
                        'wilderness', 'nature preserve']
        for word in terrain_words:
            if word in text_lower:
                terrain.add(word)
        
        return list(terrain)[:3]
    
    @classmethod
    def extract_facilities(cls, text: str) -> List[str]:
        """Extract notable facilities/buildings."""
        text_lower = text.lower()
        facilities = set()
        
        facility_words = ['lodge', 'cabin', 'cabins', 'dormitory', 'bunkhouse',
                         'dining hall', 'chapel', 'gymnasium', 'pool', 'arena',
                         'amphitheater', 'pavilion', 'zipline', 'ropes course',
                         'climbing wall', 'archery range', 'stables', 'barn',
                         'conference center', 'retreat center']
        for word in facility_words:
            if word in text_lower:
                facilities.add(word)
        
        # Count of cabins/buildings
        cabin_match = re.search(r'(\d+)\s*(?:cabins?|lodges?|buildings?)', text_lower)
        if cabin_match:
            facilities.add(f"{cabin_match.group(1)} cabins/buildings")
        
        return list(facilities)[:5]
    
    @classmethod
    def extract_sold_closed_info(cls, text: str) -> Optional[str]:
        """Extract details about sale or closure."""
        text_lower = text.lower()
        
        # Sold with year
        sold_year = re.search(r'(?:sold|closed)\s+(?:in\s+)?(\d{4})', text_lower)
        
        # Sold to whom
        sold_to = re.search(r'(?:sold|acquired)\s+(?:to|by)\s+([A-Za-z\s&]+?)(?:\s+(?:in|for)\s+|\.|,)', text)
        
        # Sale price
        sale_price = re.search(r'sold\s+for\s+\$?([\d,]+(?:\.\d+)?)\s*(million|M)?', text_lower)
        
        parts = []
        if sold_year:
            parts.append(f"in {sold_year.group(1)}")
        if sold_to:
            buyer = sold_to.group(1).strip()[:50]
            parts.append(f"to {buyer}")
        if sale_price:
            price = sale_price.group(1)
            unit = sale_price.group(2) or ""
            parts.append(f"for ${price}{' million' if unit else ''}")
        
        if parts:
            return "Sold " + " ".join(parts)
        return None
    
    @classmethod
    def extract_acreage_breakdown(cls, text: str) -> Optional[str]:
        """Extract what the acreage includes (forest, lake, etc.)."""
        text_lower = text.lower()
        
        # Pattern: X acres of [something]
        breakdown_pattern = r'(\d+(?:,\d+)?)\s*acres?\s+(?:of\s+)?(\w+(?:\s+\w+)?)'
        matches = re.findall(breakdown_pattern, text_lower)
        
        relevant = []
        for acres, feature in matches:
            if any(kw in feature for kw in cls.INCLUDES_KEYWORDS):
                relevant.append(f"{acres} acres {feature}")
        
        if relevant:
            return "; ".join(relevant[:3])
        return None
    
    @classmethod
    def build_comprehensive_notes(cls, text: str, inst_name: str, 
                                   verified_acres: Optional[float],
                                   source_url: str) -> str:
        """
        Build comprehensive notes from all extracted information.
        """
        notes_parts = []
        
        # 1. Context around acreage (most important)
        if verified_acres:
            context = cls.extract_context_around_acreage(text, verified_acres)
            if context and len(context) > 30:
                # Clean up the context
                context = re.sub(r'\s+', ' ', context).strip()
                notes_parts.append(f"Context: {context}")
        
        # 2. What acreage includes
        breakdown = cls.extract_acreage_breakdown(text)
        if breakdown:
            notes_parts.append(f"Includes: {breakdown}")
        
        # 3. Founded year
        founded = cls.extract_founded_year(text)
        if founded:
            notes_parts.append(f"Est. {founded}")
        
        # 4. Capacity
        capacity = cls.extract_capacity(text)
        if capacity:
            cap_clean = capacity[:60]
            notes_parts.append(f"Capacity: {cap_clean}")
        
        # 5. Key features (terrain + water)
        terrain = cls.extract_terrain(text)
        water = cls.extract_water_features(text)
        features = terrain + water
        if features:
            notes_parts.append(f"Features: {', '.join(features[:4])}")
        
        # 6. Notable facilities
        facilities = cls.extract_facilities(text)
        if facilities:
            notes_parts.append(f"Facilities: {', '.join(facilities[:4])}")
        
        # 7. Sold/closed info
        sold_info = cls.extract_sold_closed_info(text)
        if sold_info:
            notes_parts.append(sold_info)
        
        # 8. Source domain
        if source_url:
            domain = urlparse(source_url).netloc.replace('www.', '')
            notes_parts.append(f"Source: {domain}")
        
        # Combine and truncate
        notes = " | ".join(notes_parts)
        
        # Ensure not too long for CSV
        if len(notes) > 500:
            notes = notes[:497] + "..."
        
        return notes if notes else "No additional details found"


# =============================================================================
# ACREAGE EXTRACTION (same as before with minor improvements)
# =============================================================================

class AcreageExtractor:
    PATTERNS = [
        (r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:-|\s)?acres?\b', 'direct'),
        (r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*-acre\b', 'direct'),
        (r'campus\s+(?:of\s+)?(?:about\s+|approximately\s+|roughly\s+)?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*acres', 'campus'),
        (r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*-acre\s+campus', 'campus'),
        (r'campus\s+(?:size|area)[\s:]+(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*acres', 'campus'),
        (r'(?:property|land|site|grounds)\s+(?:of\s+)?(?:about\s+|approximately\s+)?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*acres', 'property'),
        (r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*acres?\s+(?:of\s+)?(?:land|property|grounds|site)', 'property'),
        (r'(?:spans|sits\s+on|covers|encompasses|occupies|comprises)\s+(?:about\s+|approximately\s+)?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*acres', 'spans'),
        (r'total\s+(?:of\s+)?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*acres', 'total'),
        (r'on\s+(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*acres', 'on_acres'),
    ]

    CLOSED_PATTERNS = [
        r'\b(?:closed|shuttered|shut\s+down)\s+(?:in\s+)?\d{4}',
        r'\b(?:permanently\s+)?closed\b',
        r'\bno\s+longer\s+(?:in\s+)?operat(?:ing|es)\b',
        r'\bceased\s+operations?\b',
        r'\bwas\s+sold\b',
        r'\bmerged\s+with\b',
    ]

    SOLD_PATTERNS = [
        r'\bsold\s+(?:to|the\s+property)\b',
        r'\bproperty\s+(?:was\s+)?sold\b',
        r'\bacquired\s+by\b',
    ]

    @classmethod
    def _context_window(cls, text_lower: str, start: int, end: int, window: int = 60) -> str:
        a = max(0, start - window)
        b = min(len(text_lower), end + window)
        return text_lower[a:b]

    @classmethod
    def extract_all(cls, text: str) -> List[Tuple[float, str, int, int]]:
        results = []
        text_lower = (text or "").lower()
        for pattern, source_type in cls.PATTERNS:
            for match in re.finditer(pattern, text_lower):
                try:
                    acres = float(match.group(1).replace(",", ""))
                    if 0.1 <= acres <= 50000:
                        results.append((acres, source_type, match.start(), match.end()))
                except Exception:
                    continue
        return results

    @classmethod
    def score_match(cls, text: str, acres: float, match_type: str, start: int, end: int) -> float:
        tl = (text or "").lower()
        window = cls._context_window(tl, start, end, window=80)

        base = {
            "campus": 5.0, "property": 4.2, "total": 4.0,
            "spans": 3.0, "on_acres": 2.2, "direct": 1.0
        }.get(match_type, 0.5)

        good_hits = sum(1 for g in GOOD_CONTEXT if g in window)
        bad_hits = sum(1 for b in BAD_CONTEXT if b in window)

        base += min(1.5, 0.5 * good_hits)
        base -= min(3.0, 1.0 * bad_hits)

        if acres >= 50:
            base += 0.4
        if acres >= 200:
            base += 0.3
        if acres >= 2000:
            base -= 0.8

        return base

    @classmethod
    def get_best_estimate(cls, text: str) -> Tuple[Optional[float], str]:
        extractions = cls.extract_all(text)
        if not extractions:
            return None, "no_match"

        best = None
        best_score = -1e9
        best_type = "no_match"

        for acres, mtype, s, e in extractions:
            sc = cls.score_match(text, acres, mtype, s, e)
            if sc > best_score:
                best_score = sc
                best = acres
                best_type = mtype

        if best_score < 1.0:
            return None, "low_score_reject"

        return best, best_type

    @classmethod
    def detect_status(cls, text: str) -> str:
        tl = (text or "").lower()
        for pattern in cls.SOLD_PATTERNS:
            if re.search(pattern, tl):
                return "SOLD"
        for pattern in cls.CLOSED_PATTERNS:
            if re.search(pattern, tl):
                return "CLOSED"
        return "OPERATING"


# =============================================================================
# HELPERS
# =============================================================================

def sleep_with_jitter(base: float):
    time.sleep(base + random.uniform(0.0, JITTER_MAX))

def looks_like_bot_wall(text: str) -> bool:
    t = (text or "").lower()
    return any(x in t for x in [
        "captcha", "verify you are", "are you human", "unusual traffic",
        "robot check", "access denied", "temporarily blocked"
    ])

def normalize_name_for_search(name: str) -> str:
    clean = re.sub(r'\s+(Inc|LLC|Corp|Corporation|Ltd|Limited|Co)\s*$', '', name, flags=re.IGNORECASE)
    clean = re.sub(r'[\u201c\u201d\u2018\u2019"\']', '"', clean)  # Smart quotes to regular quotes
    clean = re.sub(r'[\u2014\u2013\-]+', ' ', clean)  # Em/en dashes to space
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean

def is_pdf(url: str) -> bool:
    return url.lower().split("?")[0].endswith(".pdf")


# =============================================================================
# WEB SCRAPER
# =============================================================================

class WebScraper:
    def __init__(self, profile_dir: str):
        self.session = requests.Session()
        self.search_count = 0
        self.fetch_count = 0
        self.profile_dir = profile_dir

    def _get_headers(self) -> dict:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        }

    def search_duckduckgo(self, query: str) -> List[Dict[str, str]]:
        results: List[Dict[str, str]] = []

        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=self.profile_dir,
                headless=False,
                viewport={"width": 700, "height": 800},
                args=[
                    "--window-size=700,850",
                    "--window-position=0,50",  # Position at left side of screen
                ]
            )
            page = context.new_page()

            url = "https://duckduckgo.com/?q=" + quote_plus(query)
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(1200)

            # Auto-continue prompt (for automation)
            print("      [Waiting for Enter to continue...]")
            input()

            page.wait_for_timeout(1500)

            html = page.content()
            soup = BeautifulSoup(html, "lxml")

            anchors = soup.select("a[data-testid='result-title-a']")
            if not anchors:
                anchors = soup.select("a.result__a")

            snippets = [s.get_text(" ", strip=True) for s in soup.select(".result__snippet")]
            if not snippets:
                snippets = [s.get_text(" ", strip=True) for s in soup.select("[data-result='snippet']")]

            seen = set()
            for i, a in enumerate(anchors):
                href = a.get("href") or ""
                title = a.get_text(" ", strip=True) or ""
                if not href.startswith("http"):
                    continue
                if href in seen:
                    continue
                seen.add(href)
                snippet = snippets[i] if i < len(snippets) else ""
                results.append({"title": title, "url": href, "snippet": snippet})
                if len(results) >= MAX_PAGES_PER_INSTITUTION:
                    break

            self.search_count += 1
            context.close()

        return results

    def fetch_page(self, url: str) -> Optional[str]:
        skip_domains = ["facebook.com", "twitter.com", "instagram.com", 
                       "youtube.com", "linkedin.com", "tiktok.com"]
        parsed = urlparse(url)
        if any(d in (parsed.netloc or "").lower() for d in skip_domains):
            return None

        if is_pdf(url):
            return None

        for attempt in range(MAX_RETRIES):
            try:
                r = self.session.get(url, headers=self._get_headers(), 
                                    timeout=REQUEST_TIMEOUT, allow_redirects=True)
                if r.status_code >= 400:
                    continue
                if looks_like_bot_wall(r.text):
                    return None

                soup = BeautifulSoup(r.text, "lxml")
                for element in soup(["script", "style", "nav", "footer", "header", "aside"]):
                    element.decompose()

                text = soup.get_text(separator=" ", strip=True)
                text = re.sub(r"\s+", " ", text).strip()
                self.fetch_count += 1

                return text[:80000]  # Increased for better notes
            except requests.RequestException:
                if attempt < MAX_RETRIES - 1:
                    sleep_with_jitter(RETRY_DELAY)
                continue
            except Exception:
                return None

        return None

    def get_stats(self) -> dict:
        return {"total_searches": self.search_count, "total_fetches": self.fetch_count}


# =============================================================================
# ACREAGE VERIFIER
# =============================================================================

class AcreageVerifier:
    def __init__(self, profile_dir: str):
        self.scraper = WebScraper(profile_dir=profile_dir)

    def verify_institution(self, inst: Institution) -> Institution:
        clean_name = normalize_name_for_search(inst.name)

        queries = [
            f"{clean_name} {inst.city} {inst.state} campus acreage",
            f"{clean_name} {inst.city} {inst.state} acres property",
            f"{clean_name} {inst.state} acreage",
        ]

        results: List[Dict[str, str]] = []
        all_text_collected = []  # Collect all text for notes extraction
        
        for q in queries:
            print(f"    Searching: {q[:80]}...")
            results = self.scraper.search_duckduckgo(q)
            if results:
                break
            sleep_with_jitter(4.0)

        if not results:
            inst.status = "UNKNOWN"
            inst.notes = "No search results found"
            return inst

        sources_found = []

        # First pass: snippets
        for res in results:
            snippet = res.get("snippet") or ""
            if snippet:
                all_text_collected.append(snippet)
            if not snippet:
                continue
            acres, mtype = AcreageExtractor.get_best_estimate(snippet)
            if acres is not None:
                sources_found.append({
                    "acres": acres,
                    "source": res.get("url", ""),
                    "title": res.get("title", ""),
                    "type": mtype,
                    "from_snippet": True,
                    "text": snippet
                })

        # Second pass: fetch pages
        if not sources_found or len(sources_found) < 2:
            print(f"    Fetching up to {min(MAX_FETCH_PAGES, len(results))} pages for details...")
            for res in results[:MAX_FETCH_PAGES]:
                sleep_with_jitter(DELAY_BETWEEN_FETCHES)
                url = res.get("url", "")
                if not url or is_pdf(url):
                    continue

                page_text = self.scraper.fetch_page(url)
                if not page_text:
                    continue

                all_text_collected.append(page_text)
                
                acres, mtype = AcreageExtractor.get_best_estimate(page_text)
                if acres is not None:
                    sources_found.append({
                        "acres": acres,
                        "source": url,
                        "title": res.get("title", ""),
                        "type": mtype,
                        "from_snippet": False,
                        "text": page_text
                    })

        # Combine all collected text for notes
        combined_text = " ".join(all_text_collected)

        # Decide best acreage
        if sources_found:
            priority = {"campus": 5, "property": 4, "total": 4, "spans": 3, "on_acres": 2, "direct": 1}
            sources_found.sort(key=lambda x: (priority.get(x["type"], 0), 1 if x["from_snippet"] else 0), reverse=True)

            best = sources_found[0]
            inst.verified_acres = float(best["acres"])
            inst.source = (best["source"] or "")[:200]

            if best["type"] in ["campus", "property", "total"]:
                inst.confidence = "HIGH"
            elif best["from_snippet"]:
                inst.confidence = "MEDIUM"
            else:
                inst.confidence = "LOW"

            # Multi-source agreement
            if len(sources_found) >= 2:
                vals = [float(s["acres"]) for s in sources_found]
                v0 = vals[0]
                close = sum(1 for v in vals if abs(v - v0) <= max(1.0, 0.02 * v0))
                if close >= 2:
                    inst.confidence = "HIGH"

            # BUILD COMPREHENSIVE NOTES
            best_text = best.get("text", combined_text)
            inst.notes = NotesExtractor.build_comprehensive_notes(
                text=best_text if best_text else combined_text,
                inst_name=inst.name,
                verified_acres=inst.verified_acres,
                source_url=inst.source
            )

        # Status detection
        inst.status = AcreageExtractor.detect_status(combined_text) if combined_text else "OPERATING"

        if inst.verified_acres is None:
            inst.status = inst.status or "UNKNOWN"
            inst.notes = inst.notes or "No acreage information found"

        return inst

    def get_stats(self) -> dict:
        return self.scraper.get_stats()


# =============================================================================
# DATA LOADING AND SAVING
# =============================================================================

def load_prioritized_data(filepath: str) -> List[Institution]:
    institutions = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("verification_priority") == "SKIP":
                continue
            if row.get("verified_acres") and row.get("verified_acres") != "":
                continue

            try:
                inst = Institution(
                    name=row["name"],
                    city=row.get("city", ""),
                    state=row.get("state", ""),
                    original_type=row.get("original_type", ""),
                    estimated_acres=float(row.get("estimated_acres", 0) or 0),
                    priority=row.get("verification_priority", "MEDIUM"),
                    detected_type=row.get("detected_type", "unknown"),
                )
                institutions.append(inst)
            except Exception:
                continue

    priority_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    institutions.sort(key=lambda x: priority_order.get(x.priority, 9))
    return institutions


def load_checkpoint(filepath: str) -> set:
    verified = set()
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("name"):
                    verified.add(row["name"])
    return verified


def init_output(filepath: str):
    fieldnames = [
        "name", "city", "state", "original_type", "detected_type",
        "estimated_acres", "priority", "verified_acres", "confidence",
        "source", "status", "notes"
    ]
    if not os.path.exists(filepath):
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()


def append_result(inst: Institution, filepath: str):
    fieldnames = [
        "name", "city", "state", "original_type", "detected_type",
        "estimated_acres", "priority", "verified_acres", "confidence",
        "source", "status", "notes"
    ]
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writerow({
            "name": inst.name,
            "city": inst.city,
            "state": inst.state,
            "original_type": inst.original_type,
            "detected_type": inst.detected_type,
            "estimated_acres": inst.estimated_acres,
            "priority": inst.priority,
            "verified_acres": inst.verified_acres if inst.verified_acres is not None else "",
            "confidence": inst.confidence or "",
            "source": inst.source or "",
            "status": inst.status or "",
            "notes": inst.notes or "",
        })


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Verify institution acreage with ENHANCED notes extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python acreage_scraper_enhanced.py --input data.csv --output results.csv
  python acreage_scraper_enhanced.py --input data.csv --limit 10 --resume
"""
    )
    parser.add_argument("--input", "-i", default="full_dataset_prioritized.csv")
    parser.add_argument("--output", "-o", default="verified_acreage_enhanced.csv")
    parser.add_argument("--limit", "-n", type=int, default=None)
    parser.add_argument("--priority", "-p", choices=["CRITICAL", "HIGH", "MEDIUM", "LOW"], default=None)
    parser.add_argument("--resume", "-r", action="store_true")
    args = parser.parse_args()

    script_dir = Path(__file__).parent.resolve()

    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = script_dir / input_path

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = script_dir / output_path

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        return

    profile_dir = str(script_dir / "ddg_profile")
    os.makedirs(profile_dir, exist_ok=True)

    print("\n" + "=" * 70)
    print("Enhanced Acreage Verifier (with Rich Notes Extraction)")
    print("=" * 70 + "\n")
    print(f"Input:  {input_path}")
    print(f"Output: {output_path}\n")

    institutions = load_prioritized_data(str(input_path))
    print(f"Loaded {len(institutions)} institutions.\n")

    if args.priority:
        institutions = [i for i in institutions if i.priority == args.priority]
        print(f"Filtered to {len(institutions)} at priority={args.priority}\n")

    if args.resume and output_path.exists():
        already = load_checkpoint(str(output_path))
        before = len(institutions)
        institutions = [i for i in institutions if i.name not in already]
        print(f"Resume: skipped {before - len(institutions)} already verified.\n")

    if args.limit:
        institutions = institutions[:args.limit]
        print(f"Limited to {len(institutions)} institutions.\n")

    if not institutions:
        print("No institutions to verify.")
        return

    init_output(str(output_path))
    verifier = AcreageVerifier(profile_dir=profile_dir)

    start_time = datetime.now()
    verified_count = 0
    found_count = 0

    print("Starting verification with enhanced notes...")
    print("-" * 70)

    for i, inst in enumerate(institutions, 1):
        print(f"\n[{i}/{len(institutions)}] {inst.name} ({inst.priority})")
        print(f"    Location: {inst.city}, {inst.state}")

        inst = verifier.verify_institution(inst)
        verified_count += 1

        if inst.verified_acres is not None:
            found_count += 1
            print(f"    ✓ Verified: {inst.verified_acres} acres ({inst.confidence})")
        else:
            print("    ? No acreage found")

        print(f"    Status: {inst.status}")
        if inst.notes:
            # Show truncated notes in console
            notes_preview = inst.notes[:100] + "..." if len(inst.notes) > 100 else inst.notes
            print(f"    Notes: {notes_preview}")

        append_result(inst, str(output_path))

        if i < len(institutions):
            sleep_with_jitter(DELAY_BETWEEN_SEARCHES)

        if i % 10 == 0:
            stats = verifier.get_stats()
            elapsed = (datetime.now() - start_time).total_seconds() / 60
            sr = (found_count / verified_count * 100) if verified_count else 0
            print(f"\n--- Progress {i}/{len(institutions)} | Found {found_count} ({sr:.0f}%) | "
                  f"Elapsed {elapsed:.1f} min ---\n")

    stats = verifier.get_stats()
    elapsed = (datetime.now() - start_time).total_seconds() / 60
    sr = (found_count / verified_count * 100) if verified_count else 0

    print("\n" + "=" * 70)
    print("VERIFICATION COMPLETE")
    print("=" * 70)
    print(f"Results: {output_path}")
    print(f"  Verified: {verified_count}")
    print(f"  Found: {found_count} ({sr:.1f}%)")
    print(f"  Time: {elapsed:.1f} minutes")
    print(f"  Cost: $0.00")
    print()


if __name__ == "__main__":
    main()