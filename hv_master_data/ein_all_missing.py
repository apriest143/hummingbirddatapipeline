### ein_lookup_all_missing.py
### COMPREHENSIVE EIN LOOKUP SCRIPT - One script to fill ALL missing EINs
### 
### Strategies (in order):
### 1. ProPublica Nonprofit Explorer API (search by name + state)
### 2. ProPublica search with shortened name
### 3. ProPublica search without state filter
### 4. IRS Tax Exempt Organization Search (if ProPublica fails)
###
### No API key required - uses free public APIs

import pandas as pd
import requests
import time
import re

# ---- Configuration ----
DATA_FILE = "hv_master_data/data/HummingbirdDataWorking_990_merged.csv"

# Rate limiting (be respectful to free APIs)
DELAY_BETWEEN_REQUESTS = 0.3  # seconds

# Matching threshold (0-100, higher = stricter matching)
MIN_MATCH_SCORE = 45

print("="*70)
print("COMPREHENSIVE EIN LOOKUP - Fill All Missing EINs")
print("="*70)

# ---- Helper functions ----
def is_missing(val):
    if pd.isna(val):
        return True
    s = str(val).strip()
    return s == '' or s.lower() == 'nan'

def clean_name_for_search(name):
    """Clean institution name for better search results"""
    if not isinstance(name, str):
        return ""
    
    clean = name
    
    # Remove garbled encoding characters
    garbled_patterns = [
        'Ã¢â‚¬â€', 'Ã¢â‚¬â„¢', 'Ã¢â‚¬Â', 'â€"', 'â€™', 'â€"',
        'Ã©', 'Ã¨', 'Ã¯', 'Ã±', 'Ã³', 'Ã¡', 'Ãº'
    ]
    for pattern in garbled_patterns:
        clean = clean.replace(pattern, ' ')
    
    # Split on common DBA indicators and take first part
    dba_separators = [' - ', '--', '—', '–', ' dba ', ' DBA ', '/']
    for sep in dba_separators:
        if sep in clean:
            clean = clean.split(sep)[0].strip()
    
    # Clean up extra whitespace
    clean = re.sub(r'\s+', ' ', clean).strip()
    
    return clean

def normalize_name(name):
    """Normalize name for comparison"""
    if not isinstance(name, str):
        return ""
    normalized = name.lower()
    # Remove common suffixes for comparison
    suffixes = [' inc', ' llc', ' corp', ' corporation', ' foundation', 
                ' ministries', ' ministry', ' church', ' of america',
                ' usa', ' us', ' the', ' a ', ' an ']
    for suffix in suffixes:
        normalized = normalized.replace(suffix, ' ')
    # Remove punctuation and extra spaces
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized

def extract_state_code(state_str):
    """Extract clean state code from state string"""
    if not state_str or pd.isna(state_str):
        return None
    # Handle multi-state like "NY-NJ-PA" - take first
    state = str(state_str).strip().split('-')[0].strip()
    if len(state) == 2:
        return state.upper()
    return None

def calculate_match_score(search_name, result_name, search_state, result_state, search_city=None, result_city=None):
    """Calculate how well a result matches our search (0-100)"""
    score = 0
    
    search_norm = normalize_name(search_name)
    result_norm = normalize_name(result_name)
    
    # Exact normalized name match = 60 points
    if search_norm == result_norm:
        score += 60
    else:
        # Check word overlap
        search_words = set(search_norm.split())
        result_words = set(result_norm.split())
        
        if search_words and result_words:
            overlap = len(search_words & result_words)
            total = len(search_words | result_words)
            word_score = (overlap / total) * 50
            score += word_score
            
            # Bonus if search name is contained in result or vice versa
            if search_norm in result_norm or result_norm in search_norm:
                score += 15
    
    # State match = 25 points
    if search_state and result_state:
        if search_state.upper() == result_state.upper():
            score += 25
    
    # City match = 15 points (bonus)
    if search_city and result_city:
        if normalize_name(search_city) == normalize_name(result_city):
            score += 15
        elif normalize_name(search_city) in normalize_name(result_city):
            score += 8
    
    return min(score, 100)

def search_propublica(search_term, state=None):
    """Search ProPublica Nonprofit Explorer API"""
    base_url = "https://projects.propublica.org/nonprofits/api/v2/search.json"
    
    params = {"q": search_term}
    if state:
        params["state[id]"] = state
    
    try:
        response = requests.get(base_url, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            return data.get('organizations', [])
    except Exception as e:
        pass
    return []

def find_best_ein_match(name, city, state, min_score=MIN_MATCH_SCORE):
    """
    Try multiple strategies to find EIN match.
    Returns (ein, matched_name, score, strategy) or (None, None, 0, None)
    """
    clean_name = clean_name_for_search(name)
    state_code = extract_state_code(state)
    
    if not clean_name:
        return None, None, 0, None
    
    strategies = []
    
    # Strategy 1: Full clean name with state filter
    if state_code:
        strategies.append((clean_name, state_code, "full_name_state"))
    
    # Strategy 2: First 3-4 words with state filter
    words = clean_name.split()
    if len(words) > 3 and state_code:
        short_name = ' '.join(words[:4])
        strategies.append((short_name, state_code, "short_name_state"))
    
    # Strategy 3: Full name without state filter
    strategies.append((clean_name, None, "full_name_nostate"))
    
    # Strategy 4: First 3 words without state
    if len(words) > 2:
        short_name = ' '.join(words[:3])
        strategies.append((short_name, None, "short_name_nostate"))
    
    best_match = (None, None, 0, None)
    
    for search_term, search_state, strategy_name in strategies:
        results = search_propublica(search_term, search_state)
        time.sleep(DELAY_BETWEEN_REQUESTS)
        
        for org in results:
            org_name = org.get('name', '')
            org_state = org.get('state', '')
            org_city = org.get('city', '')
            org_ein = org.get('ein')
            
            if not org_ein:
                continue
            
            score = calculate_match_score(name, org_name, state_code, org_state, city, org_city)
            
            if score > best_match[2] and score >= min_score:
                best_match = (str(org_ein).zfill(9), org_name, score, strategy_name)
        
        # If we found a good match (score >= 70), don't try more strategies
        if best_match[2] >= 70:
            break
    
    return best_match

# ---- Load data ----
print("\n1. Loading data...")
df = pd.read_csv(DATA_FILE, dtype=str)
print(f"   Total rows: {len(df)}")

# ---- Fix -1 EIN values (from IPEDS) ----
print("\n2. Fixing -1 EIN values...")
negative_ein_mask = df['ein_number'] == '-1'
negative_count = negative_ein_mask.sum()
print(f"   Found {negative_count} rows with EIN = '-1'")

if negative_count > 0 and 'ein_number_raw' in df.columns:
    # Restore from ein_number_raw where available
    restore_mask = (negative_ein_mask & 
                    df['ein_number_raw'].notna() & 
                    (df['ein_number_raw'] != 'nan') &
                    (df['ein_number_raw'] != ''))
    restore_count = restore_mask.sum()
    df.loc[restore_mask, 'ein_number'] = df.loc[restore_mask, 'ein_number_raw']
    print(f"   Restored {restore_count} EINs from ein_number_raw")
    
    # Clear remaining -1 values (set to empty so they get picked up as missing)
    still_negative = df['ein_number'] == '-1'
    df.loc[still_negative, 'ein_number'] = ''
    print(f"   Cleared {still_negative.sum()} remaining -1 values")

# ---- Find missing EINs ----
print("\n3. Finding missing EINs...")
missing_mask = df['ein_number'].apply(is_missing)
missing_indices = df[missing_mask].index.tolist()
print(f"   Rows missing EIN: {len(missing_indices)}")

if len(missing_indices) == 0:
    print("\n   ✓ No missing EINs! Nothing to do.")
    exit()

# Show breakdown by type
missing_df = df[missing_mask]
print(f"\n   By institution type:")
for inst_type, count in missing_df['institution_type'].value_counts().items():
    print(f"      {inst_type}: {count}")

# ---- Process missing rows ----
print(f"\n4. Looking up {len(missing_indices)} missing EINs...")
print("   (This may take several minutes due to API rate limiting)")
print("-"*70)

stats = {
    'full_name_state': 0,
    'short_name_state': 0,
    'full_name_nostate': 0,
    'short_name_nostate': 0,
    'not_found': 0
}

found_entries = []
failed_entries = []

for i, idx in enumerate(missing_indices):
    row = df.loc[idx]
    name = row['institution_name']
    city = row['city']
    state = row['state']
    inst_type = row['institution_type']
    
    ein, matched_name, score, strategy = find_best_ein_match(name, city, state)
    
    if ein and strategy:
        df.at[idx, 'ein_number'] = ein
        stats[strategy] = stats.get(strategy, 0) + 1
        found_entries.append({
            'original_name': name,
            'matched_name': matched_name,
            'ein': ein,
            'score': score,
            'strategy': strategy,
            'institution_type': inst_type
        })
    else:
        stats['not_found'] += 1
        failed_entries.append({
            'name': name,
            'city': city,
            'state': state,
            'institution_type': inst_type
        })
    
    # Progress indicator every 20 rows
    if (i + 1) % 20 == 0 or (i + 1) == len(missing_indices):
        pct = (i + 1) / len(missing_indices) * 100
        found = sum(v for k, v in stats.items() if k != 'not_found')
        print(f"   [{pct:5.1f}%] Processed {i + 1}/{len(missing_indices)} | Found: {found} | Not found: {stats['not_found']}")

print("-"*70)

# ---- Save results ----
print(f"\n5. Saving back to: {DATA_FILE}")
df.to_csv(DATA_FILE, index=False)

# ---- Print summary ----
print("\n" + "="*70)
print("EIN LOOKUP SUMMARY")
print("="*70)
total_found = sum(v for k, v in stats.items() if k != 'not_found')
print(f"Total processed: {len(missing_indices)}")
print(f"EINs found: {total_found}")
print(f"Not found: {stats['not_found']}")
print(f"Success rate: {total_found / len(missing_indices) * 100:.1f}%")

print(f"\n--- By Strategy ---")
print(f"Full name + state: {stats.get('full_name_state', 0)}")
print(f"Short name + state: {stats.get('short_name_state', 0)}")
print(f"Full name (no state): {stats.get('full_name_nostate', 0)}")
print(f"Short name (no state): {stats.get('short_name_nostate', 0)}")

if found_entries:
    # Show some matches for verification
    print(f"\n--- Sample Matches (for verification) ---")
    for entry in found_entries[:10]:
        print(f"  ✓ {entry['original_name'][:45]}")
        print(f"    → {entry['matched_name'][:45]} (EIN: {entry['ein']}, Score: {entry['score']:.0f})")
    
    # Save found entries for verification
    found_df = pd.DataFrame(found_entries)
    found_df.to_csv("hv_master_data/data/ein_lookup_found.csv", index=False)
    print(f"\n   All matches saved to: hv_master_data/data/ein_lookup_found.csv (review for accuracy)")

if failed_entries:
    print(f"\n--- Still Missing ({len(failed_entries)}) by Institution Type ---")
    failed_df = pd.DataFrame(failed_entries)
    print(failed_df['institution_type'].value_counts().to_string())
    
    print(f"\n--- Sample Failed Entries ---")
    for entry in failed_entries[:10]:
        print(f"  ✗ {entry['name'][:50]}")
        print(f"    {entry['city']}, {entry['state']} | {entry['institution_type']}")
    
    # Save failed entries
    failed_df.to_csv("hv_master_data/data/ein_lookup_failed.csv", index=False)
    print(f"\n   Failed entries saved to: hv_master_data/data/ein_lookup_failed.csv")

# Final verification
print("\n" + "="*70)
print("FINAL VERIFICATION")
print("="*70)
df_verify = pd.read_csv(DATA_FILE, dtype=str)
still_missing = df_verify[df_verify['ein_number'].apply(is_missing)]
print(f"Rows still missing EIN: {len(still_missing)}")

print("\n" + "="*70)
print("DONE!")
print("="*70)