

### geocode_all_missing.py
### COMPREHENSIVE GEOCODING SCRIPT - One script to fill ALL missing lat/long
### 
### Strategies (in order):
### 1. If city is bad (#VALUE!) but has EIN → Look up address via ProPublica first
### 2. Clean name + clean city + state → Google Geocoding
### 3. Name + state only → Google Geocoding
### 4. City + state fallback → Google Geocoding (gets city center)
### 5. Original city + state → Google Geocoding

import pandas as pd
import requests
import time
import re

# ---- Configuration ----
DATA_FILE = "hv_master_data/data/HummingbirdDataWorking_990_merged.csv"

# *** PASTE YOUR GOOGLE API KEY HERE ***
GOOGLE_API_KEY = "AIzaSyC-DEtYVxcUN-JHzAOo4qGreHR8MrMiyMQ"

# Rate limiting
DELAY_BETWEEN_REQUESTS = 0.1  # seconds

print("="*70)
print("COMPREHENSIVE GEOCODING - Fill All Missing Lat/Long")
print("="*70)

# ---- Helper functions ----
def is_missing(val):
    if pd.isna(val):
        return True
    s = str(val).strip()
    return s == '' or s.lower() == 'nan'

def is_bad_value(val):
    """Check if value is #VALUE! or similar bad data"""
    if pd.isna(val):
        return True
    s = str(val).strip()
    return s == '' or s.lower() == 'nan' or '#VALUE' in s.upper()

def clean_name(name):
    """Clean institution name for better geocoding results"""
    if not isinstance(name, str):
        return ""
    
    clean = name
    
    # Remove garbled encoding characters (common patterns)
    garbled_patterns = [
        'Ã¢â‚¬â€', 'Ã¢â‚¬â„¢', 'Ã¢â‚¬Â', 'â€"', 'â€™', 'â€"',
        'Ã©', 'Ã¨', 'Ã¯', 'Ã±', 'Ã³', 'Ã¡', 'Ãº'
    ]
    for pattern in garbled_patterns:
        clean = clean.replace(pattern, ' ')
    
    # Split on common DBA indicators and take first part
    dba_separators = [' - ', '--', '—', '–', '/', ' dba ', ' DBA ']
    for sep in dba_separators:
        if sep in clean:
            clean = clean.split(sep)[0].strip()
    
    # Remove common suffixes that don't help geocoding
    remove_suffixes = [' Inc', ' LLC', ' Corp', ' Foundation', ' Ministries', ' Ministry']
    for suffix in remove_suffixes:
        if clean.endswith(suffix):
            clean = clean[:-len(suffix)]
    
    # Clean up extra whitespace
    clean = re.sub(r'\s+', ' ', clean).strip()
    
    return clean

def clean_city(city):
    """Expand abbreviated city names"""
    if not isinstance(city, str):
        return ""
    
    # Skip if it's a bad value
    if '#VALUE' in city.upper():
        return ""
    
    abbreviations = {
        'Spgs': 'Springs',
        'Spg': 'Spring',
        'Mtn': 'Mountain',
        'Hts': 'Heights',
        'Vly': 'Valley',
        'Ft': 'Fort',
        'St': 'Saint',
        'Mt': 'Mount',
        'Is': 'Island',
        'Pt': 'Point',
        'Pk': 'Park',
        'Jct': 'Junction'
    }
    
    city_clean = city
    for abbr, full in abbreviations.items():
        # Match whole words only
        city_clean = re.sub(r'\b' + abbr + r'\b', full, city_clean)
    
    return city_clean.strip()

def lookup_address_by_ein(ein):
    """Look up organization address from ProPublica using EIN"""
    if is_missing(ein):
        return None
        
    ein_clean = str(ein).replace('-', '').replace(' ', '').strip().zfill(9)
    url = f"https://projects.propublica.org/nonprofits/api/v2/organizations/{ein_clean}.json"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            org = data.get('organization', {})
            city = org.get('city')
            state = org.get('state')
            if city and state:
                return {'city': city, 'state': state}
    except Exception as e:
        pass  # Silently fail, will try other strategies
    return None

def geocode_google(query, api_key):
    """Call Google Geocoding API"""
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": query,
        "key": api_key
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        data = response.json()
        
        if data['status'] == 'OK' and len(data['results']) > 0:
            location = data['results'][0]['geometry']['location']
            return location['lat'], location['lng'], 'success'
        else:
            return None, None, data['status']
    except Exception as e:
        return None, None, str(e)

def geocode_with_strategies(name, city, state, ein, api_key, df, idx):
    """
    Try multiple geocoding strategies in order:
    0. If city is bad but has EIN, look up address via ProPublica first
    1. Clean name + clean city + state
    2. Just clean name + state (skip city if it might be wrong)
    3. City + State only (will get city center)
    4. Original city + state (in case cleaning broke something)
    
    Returns: (lat, lng, strategy_name)
    """
    
    # Strategy 0: If city is bad, try to get address from ProPublica
    if is_bad_value(city) and not is_missing(ein):
        propublica_result = lookup_address_by_ein(ein)
        time.sleep(DELAY_BETWEEN_REQUESTS)
        
        if propublica_result:
            # Update the dataframe with found city/state
            city = propublica_result['city']
            state = propublica_result['state']
            df.at[idx, 'city'] = city
            df.at[idx, 'state'] = state
    
    clean_inst_name = clean_name(name)
    clean_city_name = clean_city(city) if city else ""
    state_str = state if state and str(state).lower() != 'nan' else ""
    
    # Skip if we still have no usable location data
    if not clean_city_name and not state_str:
        return None, None, "no_location_data"
    
    strategies = []
    
    # Strategy 1: Clean name + clean city + state
    if clean_inst_name and clean_city_name and state_str:
        strategies.append((f"{clean_inst_name}, {clean_city_name}, {state_str}", "clean_full"))
    
    # Strategy 2: Just clean name + state (skip city if it might be wrong)
    if clean_inst_name and state_str:
        strategies.append((f"{clean_inst_name}, {state_str}", "name_state"))
    
    # Strategy 3: City + State only (will get city center)
    if clean_city_name and state_str:
        strategies.append((f"{clean_city_name}, {state_str}", "city_state"))
    
    # Strategy 4: Original city + state (in case cleaning broke something)
    if city and state_str and city != clean_city_name and not is_bad_value(city):
        strategies.append((f"{city}, {state_str}", "orig_city_state"))
    
    # Try each strategy
    for query, strategy_name in strategies:
        lat, lng, status = geocode_google(query, api_key)
        if lat is not None and lng is not None:
            return lat, lng, strategy_name
        time.sleep(DELAY_BETWEEN_REQUESTS)
    
    return None, None, "all_strategies_failed"

# ---- Load data ----
print("\n1. Loading data...")
df = pd.read_csv(DATA_FILE, dtype=str)
print(f"   Total rows: {len(df)}")

# ---- Find missing coordinates ----
missing_mask = df['latitude'].apply(is_missing) | df['longitude'].apply(is_missing)
missing_indices = df[missing_mask].index.tolist()
print(f"   Rows missing lat/long: {len(missing_indices)}")

if len(missing_indices) == 0:
    print("\n   ✓ No missing coordinates! Nothing to do.")
    exit()

# Count how many have bad city data
bad_city_count = sum(1 for idx in missing_indices if is_bad_value(df.loc[idx, 'city']))
print(f"   Of which have bad city data (#VALUE!): {bad_city_count}")

# ---- Check API key ----
if GOOGLE_API_KEY == "YOUR_API_KEY_HERE":
    print("\n   ⚠️  ERROR: Please paste your Google API key in the script!")
    print("   Edit line 21: GOOGLE_API_KEY = 'your-actual-key-here'")
    exit()

# ---- Process missing rows ----
print(f"\n2. Geocoding {len(missing_indices)} missing entries...")
print("-"*70)

stats = {
    'clean_full': 0,
    'name_state': 0,
    'city_state': 0,
    'orig_city_state': 0,
    'no_location_data': 0,
    'all_strategies_failed': 0,
    'propublica_helped': 0
}

failed_entries = []

for i, idx in enumerate(missing_indices):
    row = df.loc[idx]
    name = row['institution_name']
    city = row['city']
    state = row['state']
    ein = row.get('ein_number', '')
    
    # Track if city was bad before
    city_was_bad = is_bad_value(city)
    
    lat, lng, strategy = geocode_with_strategies(name, city, state, ein, GOOGLE_API_KEY, df, idx)
    
    if lat is not None and lng is not None:
        df.at[idx, 'latitude'] = str(lat)
        df.at[idx, 'longitude'] = str(lng)
        stats[strategy] = stats.get(strategy, 0) + 1
        
        # Track if ProPublica helped fix a bad city
        if city_was_bad and not is_bad_value(df.loc[idx, 'city']):
            stats['propublica_helped'] += 1
        
        symbol = "✓"
    else:
        stats[strategy] = stats.get(strategy, 0) + 1
        symbol = "✗"
        failed_entries.append({
            'name': name,
            'city': city,
            'state': state,
            'ein': ein,
            'error': strategy
        })
    
    # Progress indicator every 25 rows
    if (i + 1) % 25 == 0 or (i + 1) == len(missing_indices):
        pct = (i + 1) / len(missing_indices) * 100
        print(f"   [{pct:5.1f}%] Processed {i + 1}/{len(missing_indices)}")

print("-"*70)

# ---- Save results ----
print(f"\n3. Saving back to: {DATA_FILE}")
df.to_csv(DATA_FILE, index=False)

# ---- Print summary ----
print("\n" + "="*70)
print("GEOCODING SUMMARY")
print("="*70)
total_success = stats['clean_full'] + stats['name_state'] + stats['city_state'] + stats.get('orig_city_state', 0)
total_failed = stats.get('no_location_data', 0) + stats.get('all_strategies_failed', 0)

print(f"Total processed: {len(missing_indices)}")
print(f"Successfully geocoded: {total_success}")
print(f"Failed: {total_failed}")
print(f"Success rate: {total_success / len(missing_indices) * 100:.1f}%")

print(f"\n--- By Strategy ---")
print(f"Clean name + city + state: {stats.get('clean_full', 0)}")
print(f"Name + state only: {stats.get('name_state', 0)}")
print(f"City + state (fallback): {stats.get('city_state', 0)}")
print(f"Original city + state: {stats.get('orig_city_state', 0)}")

print(f"\n--- Special Cases ---")
print(f"ProPublica address lookup helped: {stats.get('propublica_helped', 0)}")
print(f"No location data available: {stats.get('no_location_data', 0)}")
print(f"All strategies failed: {stats.get('all_strategies_failed', 0)}")

if failed_entries:
    print(f"\n--- Failed Entries ({len(failed_entries)}) ---")
    for entry in failed_entries[:15]:
        print(f"  • {entry['name'][:50]}")
        city_display = entry['city'] if entry['city'] else 'N/A'
        state_display = entry['state'] if entry['state'] else 'N/A'
        ein_display = entry['ein'] if entry['ein'] else 'No EIN'
        print(f"    {city_display}, {state_display} | {ein_display} | {entry['error']}")
    if len(failed_entries) > 15:
        print(f"  ... and {len(failed_entries) - 15} more")
    
    # Save failed entries
    failed_df = pd.DataFrame(failed_entries)
    failed_df.to_csv("hv_master_data/data/geocoding_failed.csv", index=False)
    print(f"\n   Failed entries saved to: hv_master_data/data/geocoding_failed.csv")

# Final verification
print("\n" + "="*70)
print("FINAL VERIFICATION")
print("="*70)
df_verify = pd.read_csv(DATA_FILE, dtype=str)
still_missing = df_verify[df_verify['latitude'].apply(is_missing) | df_verify['longitude'].apply(is_missing)]
still_bad_city = df_verify[df_verify['city'].astype(str).str.contains('#VALUE!', na=False)]
print(f"Rows still missing lat/long: {len(still_missing)}")
print(f"Rows still with #VALUE! in city: {len(still_bad_city)}")

print("\n" + "="*70)
print("DONE!")
print("="*70)