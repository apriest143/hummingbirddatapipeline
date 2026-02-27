#!/usr/bin/env python3
"""
990 Lat/Long Appender
=====================
Pulls 990 filings with lat/long from Hummingbird_Master_Combined_v2.csv
and appends them to full_dataset_prioritized.csv

Usage:
    python append_990_latlong.py
    
Or with custom paths:
    python append_990_latlong.py --master Hummingbird_Master_Combined_v2.csv --dataset full_dataset_prioritized.csv --output full_dataset_updated.csv
"""

import argparse
import pandas as pd
from pathlib import Path

# =============================================================================
# HARDCODED PATHS
# =============================================================================
MASTER_FILE = r"C:\Users\apriest1\Documents\GitHub\hummingbirddatapipeline\hv_master_data\data\Hummingbird_Master_Combined_v2.csv"
DATASET_FILE = r"C:\Users\apriest1\Documents\GitHub\hummingbirddatapipeline\hv_master_data\acreage_scripts\full_dataset_prioritized.csv"
OUTPUT_FILE = r"C:\Users\apriest1\Documents\GitHub\hummingbirddatapipeline\hv_master_data\acreage_scripts\full_dataset_prioritized.csv"  # Overwrites original


def map_to_detected_type(row):
    """Map institution type to detected_type categories for acreage scraping."""
    inst_type = str(row.get('institution_type', '')).lower()
    name = str(row.get('institution_name', '')).lower()
    ntee = str(row.get('ntee_code', '')).upper() if pd.notna(row.get('ntee_code')) else ''
    
    # Camp/Ranch detection
    camp_keywords = ['camp', 'ranch', 'retreat', 'outdoor', 'scout', 'ymca', 'ywca', 'conference center', 'recreation']
    if any(kw in name for kw in camp_keywords) or any(kw in inst_type for kw in camp_keywords):
        return 'camp_ranch'
    if ntee.startswith('N'):
        return 'camp_ranch'
    
    # Educational
    if any(x in inst_type for x in ['school', 'education', 'college', 'university']):
        return 'educational'
    if ntee.startswith('B'):
        return 'educational'
    
    # Religious
    if any(x in inst_type for x in ['church', 'religious', 'ministry', 'faith']):
        return 'religious'
    if ntee.startswith('X'):
        return 'religious'
    
    # Healthcare/Wellness
    if any(x in inst_type for x in ['health', 'hospital', 'medical', 'wellness']):
        return 'healthcare'
    if ntee and ntee[0] in ['E', 'F', 'G', 'H']:
        return 'healthcare'
    
    # Housing
    if any(x in inst_type for x in ['housing', 'residential', 'shelter']):
        return 'housing'
    if ntee.startswith('L'):
        return 'housing'
    
    # Arts/Culture
    if any(x in inst_type for x in ['art', 'museum', 'culture', 'theater']):
        return 'arts_culture'
    if ntee.startswith('A'):
        return 'arts_culture'
    
    return 'other_nonprofit'


def get_priority(distress_score, distress_category):
    """Determine priority based on distress score."""
    # Try distress_score first
    if pd.notna(distress_score):
        try:
            score = float(distress_score)
            if score >= 80:
                return 'CRITICAL'
            elif score >= 60:
                return 'HIGH'
            elif score >= 40:
                return 'MEDIUM'
            else:
                return 'LOW'
        except:
            pass
    
    # Fall back to category
    if pd.notna(distress_category):
        cat = str(distress_category).upper()
        if cat == 'CRITICAL':
            return 'CRITICAL'
        elif cat == 'HIGH':
            return 'HIGH'
        elif cat == 'MODERATE':
            return 'MEDIUM'
        elif cat == 'LOW':
            return 'LOW'
    
    return 'MEDIUM'


def main():
    parser = argparse.ArgumentParser(description='Append 990 filings with lat/long to full dataset')
    parser.add_argument('--master', '-m', default=MASTER_FILE,
                        help='Master combined CSV file')
    parser.add_argument('--dataset', '-d', default=DATASET_FILE,
                        help='Existing dataset to append to')
    parser.add_argument('--output', '-o', default=OUTPUT_FILE,
                        help='Output file')
    parser.add_argument('--min-distress', type=float, default=None,
                        help='Minimum distress score to include (e.g., 40)')
    args = parser.parse_args()
    
    # Use paths directly (already absolute)
    master_path = Path(args.master)
    dataset_path = Path(args.dataset)
    output_path = Path(args.output)
    
    print("="*60)
    print("990 Lat/Long Appender")
    print("="*60)
    print(f"\nMaster file:  {master_path}")
    print(f"Dataset:      {dataset_path}")
    print(f"Output:       {output_path}")
    if args.min_distress:
        print(f"Min distress: {args.min_distress}")
    
    # Load master file
    print(f"\nLoading master file...")
    master_df = pd.read_csv(master_path, low_memory=False)
    print(f"  Total rows: {len(master_df):,}")
    
    # Filter for 990 data source
    print(f"\nFiltering for 990 filings...")
    df_990 = master_df[master_df['data_source'].str.contains('990', case=False, na=False)].copy()
    print(f"  990 filings: {len(df_990):,}")
    
    # Filter for valid lat/long
    print(f"\nFiltering for valid lat/long...")
    df_990 = df_990[
        df_990['latitude'].notna() & 
        df_990['longitude'].notna() &
        (df_990['latitude'] != 0) &
        (df_990['longitude'] != 0)
    ].copy()
    print(f"  With lat/long: {len(df_990):,}")
    
    # Optional: filter by minimum distress score
    if args.min_distress:
        print(f"\nFiltering for distress score >= {args.min_distress}...")
        # Check multiple distress score columns
        distress_col = None
        for col in ['distress_score', 'distress_score_990']:
            if col in df_990.columns:
                distress_col = col
                break
        
        if distress_col:
            df_990 = df_990[
                pd.to_numeric(df_990[distress_col], errors='coerce') >= args.min_distress
            ].copy()
            print(f"  After distress filter: {len(df_990):,}")
    
    if len(df_990) == 0:
        print("\nNo matching rows found. Exiting.")
        return
    
    # Transform to prioritized format
    print(f"\nTransforming to prioritized format...")
    new_rows = []
    
    for _, row in df_990.iterrows():
        detected_type = map_to_detected_type(row)
        
        # Get distress info
        distress_score = row.get('distress_score') or row.get('distress_score_990')
        distress_cat = row.get('distress_category') or row.get('distress_category_990')
        priority = get_priority(distress_score, distress_cat)
        
        # Get acres if available
        acres = row.get('verified_acres') or row.get('acreage_raw') or 0
        if pd.isna(acres):
            acres = 0
        
        new_row = {
            'name': row.get('institution_name', ''),
            'city': row.get('city', ''),
            'state': row.get('state', ''),
            'original_type': row.get('institution_type', ''),
            'detected_type': detected_type,
            'estimated_acres': float(acres) if acres else 0.0,
            'verification_priority': priority,
            'skip_reason': '',
            'verified_acres': '',
            'confidence': '',
            'source': '',
            'status': '',
            'notes': ''
        }
        new_rows.append(new_row)
    
    new_df = pd.DataFrame(new_rows)
    
    print(f"\nType distribution:")
    print(new_df['detected_type'].value_counts().to_string())
    
    print(f"\nPriority distribution:")
    print(new_df['verification_priority'].value_counts().to_string())
    
    # Load existing dataset
    print(f"\nLoading existing dataset...")
    if dataset_path.exists():
        existing_df = pd.read_csv(dataset_path)
        print(f"  Existing rows: {len(existing_df):,}")
    else:
        print(f"  Dataset not found, creating new one")
        existing_df = pd.DataFrame(columns=new_df.columns)
    
    # Deduplicate
    print(f"\nDeduplicating...")
    existing_df['_key'] = (
        existing_df['name'].astype(str).str.lower().str.strip() + '|' + 
        existing_df['city'].astype(str).str.lower().str.strip() + '|' + 
        existing_df['state'].astype(str).str.lower().str.strip()
    )
    new_df['_key'] = (
        new_df['name'].astype(str).str.lower().str.strip() + '|' + 
        new_df['city'].astype(str).str.lower().str.strip() + '|' + 
        new_df['state'].astype(str).str.lower().str.strip()
    )
    
    existing_keys = set(existing_df['_key'].tolist())
    new_df_deduped = new_df[~new_df['_key'].isin(existing_keys)].copy()
    
    duplicates_removed = len(new_df) - len(new_df_deduped)
    print(f"  Duplicates removed: {duplicates_removed:,}")
    print(f"  New unique rows: {len(new_df_deduped):,}")
    
    # Clean up key columns
    new_df_deduped = new_df_deduped.drop(columns=['_key'])
    existing_df = existing_df.drop(columns=['_key'])
    
    # Combine
    combined_df = pd.concat([existing_df, new_df_deduped], ignore_index=True)
    
    # Save
    print(f"\nSaving to {output_path}...")
    combined_df.to_csv(output_path, index=False)
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"  Original dataset:    {len(existing_df):,} rows")
    print(f"  990 with lat/long:   {len(df_990):,} institutions")
    print(f"  Duplicates removed:  {duplicates_removed:,}")
    print(f"  New rows added:      {len(new_df_deduped):,}")
    print(f"  Final total:         {len(combined_df):,} rows")
    print(f"\nSaved to: {output_path}")
    print("="*60)


if __name__ == "__main__":
    main()