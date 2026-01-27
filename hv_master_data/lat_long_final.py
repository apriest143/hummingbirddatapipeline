### geocode_missing.py
### Fills in missing lat/long using Google Geocoding API

import pandas as pd
import requests
import time

# ---- Configuration ----
DATA_FILE = "hv_master_data/data/HummingbirdDataWorking_990_merged.csv"

# *** PASTE YOUR GOOGLE API KEY HERE ***
GOOGLE_API_KEY = "AIzaSyC-DEtYVxcUN-JHzAOo4qGreHR8MrMiyMQ"

# Rate limiting (Google allows 50 requests/second, but we'll be conservative)
DELAY_BETWEEN_REQUESTS = 0.1  # seconds

print("="*70)
print("GEOCODING MISSING COORDINATES - Google Geocoding API")
print("="*70)

# ---- Load data ----
print("\n1. Loading data...")
df = pd.read_csv(DATA_FILE, dtype=str)
print(f"   Total rows: {len(df)}")

# ---- Find rows missing coordinates ----
def is_missing(val):
    if pd.isna(val):
        return True
    return str(val).strip() == ''

missing_mask = df['latitude'].apply(is_missing) | df['longitude'].apply(is_missing)
missing_indices = df[missing_mask].index.tolist()

print(f"   Rows missing lat/long: {len(missing_indices)}")

if len(missing_indices) == 0:
    print("\n   No missing coordinates! Exiting.")
    exit()

# ---- Geocoding function ----
def geocode_google(name, city, state):
    """
    Geocode using Google Geocoding API.
    Tries full name + city + state first, then falls back to just city + state.
    """
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    
    # Clean the name - remove garbled characters and everything after em-dash
    clean_name = name
    for sep in ['Ã¢â‚¬â€', 'â€"', '—', ' - ', '--']:
        if sep in clean_name:
            clean_name = clean_name.split(sep)[0].strip()
    
    # Build address string
    if city and state and str(city).lower() != 'nan' and str(state).lower() != 'nan':
        address_full = f"{clean_name}, {city}, {state}"
        address_fallback = f"{city}, {state}"
    else:
        address_full = clean_name
        address_fallback = None
    
    # Try full address first
    params = {
        "address": address_full,
        "key": GOOGLE_API_KEY
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        data = response.json()
        
        if data['status'] == 'OK' and len(data['results']) > 0:
            location = data['results'][0]['geometry']['location']
            return location['lat'], location['lng'], 'full_address'
        
        # Try fallback (city, state only)
        if address_fallback:
            params['address'] = address_fallback
            response = requests.get(base_url, params=params, timeout=10)
            data = response.json()
            
            if data['status'] == 'OK' and len(data['results']) > 0:
                location = data['results'][0]['geometry']['location']
                return location['lat'], location['lng'], 'city_state_only'
        
        return None, None, data['status']
        
    except Exception as e:
        return None, None, str(e)

# ---- Process missing rows ----
print("\n2. Geocoding missing entries...")
print("-"*70)

stats = {
    'success_full': 0,
    'success_fallback': 0,
    'failed': 0
}

failed_entries = []

for i, idx in enumerate(missing_indices):
    row = df.loc[idx]
    name = row['institution_name']
    city = row['city']
    state = row['state']
    
    lat, lng, status = geocode_google(name, city, state)
    
    if lat is not None and lng is not None:
        df.at[idx, 'latitude'] = str(lat)
        df.at[idx, 'longitude'] = str(lng)
        
        if status == 'full_address':
            stats['success_full'] += 1
            symbol = "✓"
        else:
            stats['success_fallback'] += 1
            symbol = "~"
    else:
        stats['failed'] += 1
        symbol = "✗"
        failed_entries.append({
            'name': name,
            'city': city,
            'state': state,
            'error': status
        })
    
    # Progress indicator
    if (i + 1) % 25 == 0 or (i + 1) == len(missing_indices):
        print(f"   Processed {i + 1}/{len(missing_indices)} ({symbol} {name[:40]}...)")
    
    # Rate limiting
    time.sleep(DELAY_BETWEEN_REQUESTS)

print("-"*70)

# ---- Save results ----
print(f"\n3. Saving back to: {DATA_FILE}")
df.to_csv(DATA_FILE, index=False)

# ---- Print summary ----
print("\n" + "="*70)
print("GEOCODING SUMMARY")
print("="*70)
print(f"Successfully geocoded (full address): {stats['success_full']}")
print(f"Successfully geocoded (city/state fallback): {stats['success_fallback']}")
print(f"Failed to geocode: {stats['failed']}")
print(f"Total success rate: {(stats['success_full'] + stats['success_fallback']) / len(missing_indices) * 100:.1f}%")

if failed_entries:
    print(f"\n--- Failed Entries ({len(failed_entries)}) ---")
    for entry in failed_entries[:10]:
        print(f"  • {entry['name'][:50]}")
        print(f"    Location: {entry['city']}, {entry['state']} | Error: {entry['error']}")
    if len(failed_entries) > 10:
        print(f"  ... and {len(failed_entries) - 10} more")
    
    # Save failed entries to separate file for review
    failed_df = pd.DataFrame(failed_entries)
    failed_df.to_csv("hv_master_data/data/geocoding_failed.csv", index=False)
    print(f"\n   Failed entries saved to: hv_master_data/data/geocoding_failed.csv")

print("\n" + "="*70)
print("DONE!")
print("="*70)