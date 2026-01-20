import pandas as pd
import googlemaps
import time
from datetime import datetime
import re

# Configuration
API_KEY = 'AIzaSyC-DEtYVxcUN-JHzAOo4qGreHR8MrMiyMQ'  # Same key as lat_long.py
INPUT_FILE = "hv_master_data/data/HummingbirdDataWorking.csv"
OUTPUT_FILE = "hv_master_data/data/HummingbirdDataWorking_locations_filled.csv"
BACKUP_FILE = f"hv_master_data/data/HummingbirdDataWorking_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# Initialize Google Maps client
gmaps = googlemaps.Client(key=API_KEY)

def extract_city_state_from_address(address_components):
    """
    Extract city and state from Google Maps address components.
    Returns (city, state) or (None, None)
    """
    city = None
    state = None
    
    for component in address_components:
        types = component.get('types', [])
        
        # Look for city (locality or sublocality)
        if 'locality' in types:
            city = component['long_name']
        elif 'sublocality' in types and not city:
            city = component['long_name']
        
        # Look for state (administrative_area_level_1)
        if 'administrative_area_level_1' in types:
            state = component['short_name']  # This gives us "NY" instead of "New York"
    
    return city, state

def find_location_for_institution(institution_name, institution_type=None):
    """
    Use Google Maps Geocoding to find city and state for an institution.
    Returns (city, state) or (None, None) if not found.
    """
    try:
        # Create search query - include type if available for better results
        if institution_type and pd.notna(institution_type):
            query = f"{institution_name} {institution_type}"
        else:
            query = institution_name
        
        # Call geocoding API
        result = gmaps.geocode(query)
        
        if result and len(result) > 0:
            address_components = result[0].get('address_components', [])
            city, state = extract_city_state_from_address(address_components)
            
            if city and state:
                print(f"✓ Found: {institution_name} -> {city}, {state}")
                return city, state
            else:
                print(f"✗ Incomplete: {institution_name} (found location but missing city/state)")
                return None, None
        else:
            print(f"✗ Not found: {institution_name}")
            return None, None
            
    except Exception as e:
        print(f"✗ Error searching {institution_name}: {str(e)}")
        return None, None

def is_missing_location(row):
    """
    Check if a row is missing city or state information.
    """
    city = row['city']
    state = row['state']
    
    # Check for NaN, empty string, or #VALUE!
    city_missing = pd.isna(city) or str(city).strip() == '' or '#VALUE' in str(city).upper()
    state_missing = pd.isna(state) or str(state).strip() == '' or '#VALUE' in str(state).upper()
    
    return city_missing or state_missing

def main():
    print("="*60)
    print("FILL MISSING CITY/STATE DATA")
    print("="*60)
    
    # Read the CSV
    print(f"\n1. Reading CSV from: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    print(f"   Total rows: {len(df)}")
    
    # Create backup
    print(f"\n2. Creating backup: {BACKUP_FILE}")
    df.to_csv(BACKUP_FILE, index=False)
    
    # Find rows that need location data
    needs_location = df.apply(is_missing_location, axis=1)
    rows_to_fill = df[needs_location]
    print(f"\n3. Rows missing city/state: {len(rows_to_fill)}")
    
    if len(rows_to_fill) == 0:
        print("\n✓ All rows already have city and state!")
        return
    
    # Fill missing entries
    print(f"\n4. Starting location search...")
    print("-"*60)
    
    found_count = 0
    failed_count = 0
    
    for idx, row in rows_to_fill.iterrows():
        name = row['institution_name']
        inst_type = row.get('institution_type', None)
        
        # Skip if no institution name
        if pd.isna(name) or str(name).strip() == '':
            print(f"✗ Skipping row {idx}: No institution name")
            failed_count += 1
            continue
        
        # Search for location
        city, state = find_location_for_institution(name, inst_type)
        
        if city and state:
            # Only update if currently missing
            current_city = str(df.at[idx, 'city']).strip()
            current_state = str(df.at[idx, 'state']).strip()
            
            if pd.isna(df.at[idx, 'city']) or current_city == '' or '#VALUE' in current_city.upper():
                df.at[idx, 'city'] = city
            if pd.isna(df.at[idx, 'state']) or current_state == '' or '#VALUE' in current_state.upper():
                df.at[idx, 'state'] = state
            found_count += 1
        else:
            failed_count += 1
        
        # Rate limiting
        time.sleep(0.1)
    
    print("-"*60)
    print(f"\n5. Location search complete!")
    print(f"   Successfully found: {found_count}")
    print(f"   Failed to find: {failed_count}")
    
    # Save updated CSV
    print(f"\n6. Saving updated data to: {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False)
    
    print("\n" + "="*60)
    print("DONE! Now you can run lat_long.py again to geocode these.")
    print("="*60)

if __name__ == "__main__":
    main()