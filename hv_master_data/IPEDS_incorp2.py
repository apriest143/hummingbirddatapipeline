import pandas as pd
from datetime import datetime
import math

# Configuration
HUMMINGBIRD_FILE = "hv_master_data/data/HummingbirdDataWorking_geocoded.csv"
IPEDS_FILE = "hv_master_data/data/IPEDS.csv"
OUTPUT_FILE = "hv_master_data/data/HummingbirdDataWorking_ipeds_merged.csv"
MANUAL_REVIEW_FILE = "hv_master_data/data/manual_review_multiple_matches.csv"

print("="*70)
print("IPEDS DATA MERGE SCRIPT (v2 - with EIN/Geography Resolution)")
print("="*70)

# Load datasets
print("\n1. Loading datasets...")
hb_df = pd.read_csv(HUMMINGBIRD_FILE)
ipeds_df = pd.read_csv(IPEDS_FILE)

print(f"   Hummingbird rows (before dedup): {len(hb_df)}")

# Remove exact duplicate rows from Hummingbird dataset
hb_df = hb_df.drop_duplicates()
print(f"   Hummingbird rows (after dedup):  {len(hb_df)}")

print(f"   IPEDS rows: {len(ipeds_df)}")

# Create a lookup dictionary from IPEDS for faster matching
print("\n2. Building IPEDS lookup index...")
ipeds_lookup = {}
ipeds_alias_lookup = {}

for idx, row in ipeds_df.iterrows():
    inst_name = str(row['institution name']).strip().lower()
    alias_name = str(row['HD2024.Institution name alias']).strip().lower()
    
    if inst_name and inst_name != 'nan':
        if inst_name not in ipeds_lookup:
            ipeds_lookup[inst_name] = []
        ipeds_lookup[inst_name].append(row)
    
    if alias_name and alias_name != 'nan':
        if alias_name not in ipeds_alias_lookup:
            ipeds_alias_lookup[alias_name] = []
        ipeds_alias_lookup[alias_name].append(row)

print(f"   Indexed {len(ipeds_lookup)} unique institution names")
print(f"   Indexed {len(ipeds_alias_lookup)} unique aliases")

# Tracking statistics
stats = {
    'total_processed': 0,
    'single_matches': 0,
    'multiple_matches_resolved_by_ein': 0,
    'multiple_matches_resolved_by_geo': 0,
    'multiple_matches_unresolved': 0,
    'no_match_private_colleges': [],
    'ein_filled': 0,
    'ein_replaced': 0,
    'lat_filled': 0,
    'lat_replaced': 0,
    'long_filled': 0,
    'long_replaced': 0,
    'ratio_filled': 0,
    'ratio_replaced': 0,
}

def is_empty(value):
    """Check if a value is empty, NaN, or #VALUE!"""
    if pd.isna(value):
        return True
    str_val = str(value).strip()
    return str_val == '' or '#VALUE' in str_val.upper()

def normalize_ein(ein):
    """Normalize EIN for comparison (remove dashes, leading zeros issues, etc.)"""
    if is_empty(ein):
        return None
    ein_str = str(ein).strip().replace('-', '').replace(' ', '')
    # Remove any non-numeric characters
    ein_str = ''.join(c for c in ein_str if c.isdigit())
    if len(ein_str) == 0:
        return None
    return ein_str

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in kilometers."""
    if any(is_empty(x) for x in [lat1, lon1, lat2, lon2]):
        return float('inf')
    
    try:
        lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    except (ValueError, TypeError):
        return float('inf')
    
    R = 6371  # Earth's radius in kilometers
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

def find_ipeds_match(institution_name):
    """Find IPEDS match by name or alias. Returns matching row(s) or None."""
    search_name = str(institution_name).strip().lower()
    
    if search_name in ipeds_lookup:
        return ipeds_lookup[search_name]
    
    if search_name in ipeds_alias_lookup:
        return ipeds_alias_lookup[search_name]
    
    return None

def resolve_multiple_matches(hb_row, ipeds_matches):
    """
    Try to resolve multiple IPEDS matches using EIN or geography.
    Returns (resolved_match, resolution_method) or (None, None) if unresolved.
    """
    hb_ein = normalize_ein(hb_row.get('ein_number'))
    hb_lat = hb_row.get('latitude')
    hb_lon = hb_row.get('longitude')
    
    # Try EIN matching first
    if hb_ein:
        for ipeds_row in ipeds_matches:
            ipeds_ein = normalize_ein(ipeds_row['HD2024.Employer Identification Number'])
            if ipeds_ein and hb_ein == ipeds_ein:
                return ipeds_row, 'ein'
    
    # Try geography matching if we have coordinates
    if not is_empty(hb_lat) and not is_empty(hb_lon):
        best_match = None
        best_distance = float('inf')
        
        for ipeds_row in ipeds_matches:
            ipeds_lat = ipeds_row['HD2024.Latitude location of institution']
            ipeds_lon = ipeds_row['HD2024.Longitude location of institution']
            
            distance = haversine_distance(hb_lat, hb_lon, ipeds_lat, ipeds_lon)
            
            if distance < best_distance:
                best_distance = distance
                best_match = ipeds_row
        
        # Only use geo match if within 50km (reasonable for same institution)
        if best_match is not None and best_distance < 50:
            return best_match, 'geo'
    
    return None, None

print("\n3. Processing matches and updates...")
print("-"*70)

manual_review_cases = []

for idx, hb_row in hb_df.iterrows():
    stats['total_processed'] += 1
    inst_name = hb_row['institution_name']
    inst_type = hb_row.get('institution_type', '')
    
    ipeds_matches = find_ipeds_match(inst_name)
    
    if ipeds_matches is None:
        if inst_type == 'Private College':
            stats['no_match_private_colleges'].append(inst_name)
        continue
    
    # Handle multiple matches
    if len(ipeds_matches) > 1:
        resolved_match, resolution_method = resolve_multiple_matches(hb_row, ipeds_matches)
        
        if resolved_match is not None:
            ipeds_row = resolved_match
            if resolution_method == 'ein':
                stats['multiple_matches_resolved_by_ein'] += 1
            else:
                stats['multiple_matches_resolved_by_geo'] += 1
        else:
            # Could not resolve - add to manual review
            stats['multiple_matches_unresolved'] += 1
            manual_review_cases.append({
                'hummingbird_name': inst_name,
                'hummingbird_ein': hb_row.get('ein_number', ''),
                'hummingbird_lat': hb_row.get('latitude', ''),
                'hummingbird_lon': hb_row.get('longitude', ''),
                'ipeds_match_1': ipeds_matches[0]['institution name'],
                'ipeds_ein_1': ipeds_matches[0]['HD2024.Employer Identification Number'],
                'ipeds_match_2': ipeds_matches[1]['institution name'] if len(ipeds_matches) > 1 else '',
                'ipeds_ein_2': ipeds_matches[1]['HD2024.Employer Identification Number'] if len(ipeds_matches) > 1 else '',
                'num_matches': len(ipeds_matches)
            })
            continue
    else:
        # Single match
        stats['single_matches'] += 1
        ipeds_row = ipeds_matches[0]
    
    # Apply updates from IPEDS match
    # Update EIN
    hb_ein = hb_row.get('ein_number')
    ipeds_ein = ipeds_row['HD2024.Employer Identification Number']
    
    if not is_empty(ipeds_ein):
        if is_empty(hb_ein):
            hb_df.at[idx, 'ein_number'] = ipeds_ein
            stats['ein_filled'] += 1
        elif str(hb_ein).strip() != str(ipeds_ein).strip():
            hb_df.at[idx, 'ein_number'] = ipeds_ein
            stats['ein_replaced'] += 1
    
    # Update Latitude
    hb_lat = hb_row.get('latitude')
    ipeds_lat = ipeds_row['HD2024.Latitude location of institution']
    
    if not is_empty(ipeds_lat):
        if is_empty(hb_lat):
            hb_df.at[idx, 'latitude'] = ipeds_lat
            stats['lat_filled'] += 1
        elif hb_lat != ipeds_lat:
            hb_df.at[idx, 'latitude'] = ipeds_lat
            stats['lat_replaced'] += 1
    
    # Update Longitude
    hb_long = hb_row.get('longitude')
    ipeds_long = ipeds_row['HD2024.Longitude location of institution']
    
    if not is_empty(ipeds_long):
        if is_empty(hb_long):
            hb_df.at[idx, 'longitude'] = ipeds_long
            stats['long_filled'] += 1
        elif hb_long != ipeds_long:
            hb_df.at[idx, 'longitude'] = ipeds_long
            stats['long_replaced'] += 1
    
    # Update Student-to-Faculty Ratio
    hb_ratio = hb_row.get('student_to_faculty_ratio')
    ipeds_ratio = ipeds_row['EF2024D.Student-to-faculty ratio']
    
    if not is_empty(ipeds_ratio):
        if is_empty(hb_ratio):
            hb_df.at[idx, 'student_to_faculty_ratio'] = ipeds_ratio
            stats['ratio_filled'] += 1
        elif hb_ratio != ipeds_ratio:
            hb_df.at[idx, 'student_to_faculty_ratio'] = ipeds_ratio
            stats['ratio_replaced'] += 1

print("-"*70)

# Save updated data
print(f"\n4. Saving updated data to: {OUTPUT_FILE}")
hb_df.to_csv(OUTPUT_FILE, index=False)

# Save manual review cases if any
if manual_review_cases:
    print(f"\n5. Saving manual review cases to: {MANUAL_REVIEW_FILE}")
    manual_review_df = pd.DataFrame(manual_review_cases)
    manual_review_df.to_csv(MANUAL_REVIEW_FILE, index=False)

# Print summary
print("\n" + "="*70)
print("MERGE SUMMARY")
print("="*70)
print(f"Total institutions processed: {stats['total_processed']}")

print(f"\n--- Match Resolution ---")
print(f"Single IPEDS matches: {stats['single_matches']}")
print(f"Multiple matches resolved by EIN: {stats['multiple_matches_resolved_by_ein']}")
print(f"Multiple matches resolved by geography: {stats['multiple_matches_resolved_by_geo']}")
print(f"Multiple matches UNRESOLVED (manual review): {stats['multiple_matches_unresolved']}")

total_resolved = stats['single_matches'] + stats['multiple_matches_resolved_by_ein'] + stats['multiple_matches_resolved_by_geo']
print(f"TOTAL RESOLVED MATCHES: {total_resolved}")

print(f"\nPrivate colleges with no IPEDS match: {len(stats['no_match_private_colleges'])}")

print(f"\n--- EIN Updates ---")
print(f"EINs filled (blank → value): {stats['ein_filled']}")
print(f"EINs REPLACED (existing → different): {stats['ein_replaced']}")

print(f"\n--- Latitude Updates ---")
print(f"Latitudes filled: {stats['lat_filled']}")
print(f"Latitudes replaced: {stats['lat_replaced']}")

print(f"\n--- Longitude Updates ---")
print(f"Longitudes filled: {stats['long_filled']}")
print(f"Longitudes replaced: {stats['long_replaced']}")

print(f"\n--- Student/Faculty Ratio Updates ---")
print(f"Ratios filled: {stats['ratio_filled']}")
print(f"Ratios replaced: {stats['ratio_replaced']}")

if stats['no_match_private_colleges']:
    print(f"\n--- Private Colleges with No IPEDS Match ---")
    for college in stats['no_match_private_colleges'][:10]:
        print(f"  • {college}")
    if len(stats['no_match_private_colleges']) - 10 > 0:
        print(f"  ... and {len(stats['no_match_private_colleges']) - 10} more")

print("\n" + "="*70)
print("DONE!")
print("="*70)