import pandas as pd
from datetime import datetime

# Configuration
HUMMINGBIRD_FILE = "hv_master_data/data/HummingbirdDataWorking_geocoded.csv"
IPEDS_FILE = "hv_master_data/data/IPEDS.csv"
OUTPUT_FILE = "hv_master_data/data/HummingbirdDataWorking_ipeds_merged.csv"
MANUAL_REVIEW_FILE = "hv_master_data/data/manual_review_multiple_matches.csv"

print("="*70)
print("IPEDS DATA MERGE SCRIPT")
print("="*70)

# Load datasets
print("\n1. Loading datasets...")
hb_df = pd.read_csv(HUMMINGBIRD_FILE)
ipeds_df = pd.read_csv(IPEDS_FILE)

print(f"   Hummingbird rows (before dedup): {len(hb_df)}")

# Remove exact duplicate rows from Hummingbird dataset
# This handles cases where the same institution appears multiple times with identical information
hb_df = hb_df.drop_duplicates()
print(f"   Hummingbird rows (after dedup):  {len(hb_df)}")

print(f"   IPEDS rows: {len(ipeds_df)}")

# Create a lookup dictionary from IPEDS for faster matching
# Key = lowercase institution name, Value = IPEDS row
print("\n2. Building IPEDS lookup index...")
ipeds_lookup = {}
ipeds_alias_lookup = {}

for idx, row in ipeds_df.iterrows():
    inst_name = str(row['institution name']).strip().lower()
    alias_name = str(row['HD2024.Institution name alias']).strip().lower()
    
    # Add to main lookup
    if inst_name and inst_name != 'nan':
        if inst_name not in ipeds_lookup:
            ipeds_lookup[inst_name] = []
        ipeds_lookup[inst_name].append(row)
    
    # Add to alias lookup
    if alias_name and alias_name != 'nan':
        if alias_name not in ipeds_alias_lookup:
            ipeds_alias_lookup[alias_name] = []
        ipeds_alias_lookup[alias_name].append(row)

print(f"   Indexed {len(ipeds_lookup)} unique institution names")
print(f"   Indexed {len(ipeds_alias_lookup)} unique aliases")

# Tracking statistics
stats = {
    'total_processed': 0,
    'matches_found': 0,
    'no_match_private_colleges': [],
    'ein_filled': 0,
    'ein_replaced': 0,
    'lat_filled': 0,
    'lat_replaced': 0,
    'long_filled': 0,
    'long_replaced': 0,
    'ratio_filled': 0,
    'ratio_replaced': 0,
    'multiple_matches': []
}

def is_empty(value):
    """Check if a value is empty, NaN, or #VALUE!"""
    if pd.isna(value):
        return True
    str_val = str(value).strip()
    return str_val == '' or '#VALUE' in str_val.upper()

def find_ipeds_match(institution_name):
    """Find IPEDS match by name or alias. Returns matching row(s) or None."""
    search_name = str(institution_name).strip().lower()
    
    # Try main name first
    if search_name in ipeds_lookup:
        return ipeds_lookup[search_name]
    
    # Try alias
    if search_name in ipeds_alias_lookup:
        return ipeds_alias_lookup[search_name]
    
    return None

print("\n3. Processing matches and updates...")
print("-"*70)

manual_review_cases = []

for idx, hb_row in hb_df.iterrows():
    stats['total_processed'] += 1
    inst_name = hb_row['institution_name']
    inst_type = hb_row.get('institution_type', '')
    
    # Find IPEDS match
    ipeds_matches = find_ipeds_match(inst_name)
    
    if ipeds_matches is None:
        # No match found
        if inst_type == 'Private College':
            stats['no_match_private_colleges'].append(inst_name)
        continue
    
    # Handle multiple matches
    if len(ipeds_matches) > 1:
        stats['multiple_matches'].append({
            'hummingbird_name': inst_name,
            'hummingbird_index': idx,
            'num_ipeds_matches': len(ipeds_matches),
            'ipeds_names': [m['institution name'] for m in ipeds_matches]
        })
        manual_review_cases.append({
            'hummingbird_name': inst_name,
            'hummingbird_ein': hb_row.get('ein_number', ''),
            'ipeds_match_1': ipeds_matches[0]['institution name'],
            'ipeds_match_2': ipeds_matches[1]['institution name'] if len(ipeds_matches) > 1 else '',
            'num_matches': len(ipeds_matches)
        })
        continue
    
    # Single match found
    stats['matches_found'] += 1
    ipeds_row = ipeds_matches[0]
    
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
print(f"IPEDS matches found: {stats['matches_found']}")
print(f"Multiple matches (flagged for review): {len(stats['multiple_matches'])}")
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