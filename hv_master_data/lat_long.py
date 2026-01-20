### Grabbing Lat/Long Data Make sure you have a googlecloud Geocoding API KEY
### Need to run this in terminal: pip install googlemaps pandas

import pandas as pd
import googlemaps
import time
from datetime import datetime

# Configuration
API_KEY = 'AIzaSyC-DEtYVxcUN-JHzAOo4qGreHR8MrMiyMQ'  # Replace with your actual API key
INPUT_FILE = "hv_master_data/data/HummingbirdDataWorking_locations_filled.csv"
OUTPUT_FILE = "hv_master_data/data/HummingbirdDataWorking_geocoded.csv"
BACKUP_FILE = f"hv_master_data/data/HummingbirdDataWorking_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# Initialize Google Maps client
gmaps = googlemaps.Client(key=API_KEY)

def geocode_institution(name, city, state):
    """
    Geocode an institution using Google Maps Geocoding API.
    Returns (latitude, longitude) or (None, None) if not found.
    """
    try:
        # Create search query
        query = f"{name}, {city}, {state}"
        
        # Call geocoding API
        result = gmaps.geocode(query)
        
        if result and len(result) > 0:
            location = result[0]['geometry']['location']
            lat = location['lat']
            lng = location['lng']
            print(f"✓ Found: {name} -> ({lat}, {lng})")
            return lat, lng
        else:
            print(f"✗ Not found: {name}, {city}, {state}")
            return None, None
            
    except Exception as e:
        print(f"✗ Error geocoding {name}: {str(e)}")
        return None, None

def main():
    print("="*60)
    print("INSTITUTION GEOCODING SCRIPT")
    print("="*60)
    
    # Read the CSV
    print(f"\n1. Reading CSV from: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    print(f"   Total rows: {len(df)}")
    
    # Create backup
    print(f"\n2. Creating backup: {BACKUP_FILE}")
    df.to_csv(BACKUP_FILE, index=False)
    
    # Find rows that need geocoding
    # Check if latitude/longitude are missing or empty
    needs_geocoding = df['latitude'].isna() | df['longitude'].isna() | \
                      (df['latitude'] == '') | (df['longitude'] == '')
    
    rows_to_geocode = df[needs_geocoding]
    print(f"\n3. Rows needing geocoding: {len(rows_to_geocode)}")
    
    if len(rows_to_geocode) == 0:
        print("\n✓ All rows already have coordinates!")
        return
    
    # Geocode missing entries
    print(f"\n4. Starting geocoding process...")
    print("-"*60)
    
    geocoded_count = 0
    failed_count = 0
    
    for idx, row in rows_to_geocode.iterrows():
        name = row['institution_name']
        city = row['city']
        state = row['state']
        
        # Skip if essential data is missing
        if pd.isna(name) or pd.isna(city) or pd.isna(state):
            print(f"✗ Skipping row {idx}: Missing name, city, or state")
            failed_count += 1
            continue
        
        # Geocode
        lat, lng = geocode_institution(name, city, state)
        
        if lat is not None and lng is not None:
            df.at[idx, 'latitude'] = lat
            df.at[idx, 'longitude'] = lng
            geocoded_count += 1
        else:
            failed_count += 1
        
        # Rate limiting: sleep to avoid hitting API limits
        # Google allows 50 requests per second, but being conservative
        time.sleep(0.1)
    
    print("-"*60)
    print(f"\n5. Geocoding complete!")
    print(f"   Successfully geocoded: {geocoded_count}")
    print(f"   Failed to geocode: {failed_count}")
    
    # Save updated CSV
    print(f"\n6. Saving updated data to: {OUTPUT_FILE}")
    df.to_csv(OUTPUT_FILE, index=False)
    
    print("\n" + "="*60)
    print("DONE! Check the output file for results.")
    print("="*60)

if __name__ == "__main__":
    main()