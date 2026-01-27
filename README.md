# hummingbirddatapipeline
Building out a data cleaning pipeline for Hummingbird Ventures

This project seeks to create a streamlined and automated way of implementing changes to the
hummingbird ventures dataset.

===========================================================================================

I will try and make a comprehensive overview of which .py files to run and in which order. This may be messy for a time but as I go it should get more clear.

GROUND RULES:
ALL NEW DATA GOES INTO /data
ENSURE ANY NEW DATA HAS HEADERS COMPATIBLE WITH EXISTING CODE/DATA

===========================================================================================

I am going to list files in order for a "from zero" beginning. Important notes included:

1. city_state.py - This fills in missing cities and states (Skip this unless uploading a truly significant amount of new entries)
Input = HummingBirdDataWorking.csv
Output = HummingbirdDataWorking_locations_filled.csv

2. by lat_long.py - This geocodes latitude and longitude using the Google Geocoding API. WILL NEED A KEY (its free). Also skip this as a later .py file can be used for smaller sets of new data entries.
Input = HummingbirdDataWorking_locations_filled.csv
Output = HummingbirdDataWorking_geocoded.csv

3. IPEDS_incorp.py - Matches the IPEDS data with The geocoded hummingbird data. Standardises names, and updates EIN, Lat/Long, and any other desired metrics (Must be added if desired) Financials are handled later. 
Input = HummingbirdDataWorking_geocoded.csv
Outputs = HummingbirdDataWorking_ipeds_merged, manual_review_multiple_matches.csv

Here is the NCES-IPEDS batch Data session information for replicatability:
2024: Guest_488336759694
2023: Guest_687287391029
2022: Guest_804974725436
If udpdating, grabbing more vars, or grabbing more years MAKE SURE TO SAVE A NEW SESSION FOR FUTURE USE. Any new information or years will requre updates to the IPEDS_incorp.py file
ideally the MLV file will be usable for pulling these vars and adding a year if needed. However, the IPEDS website consistently returns errors when trying to upload. so who knows.


4. ein_exact.py - Uses a Non Education based 990 form to link other institution types other set, adding in EIN off of name joins.
Inputs = HummingbirdDataWorking_ipeds_merged.csv, NonEd990.csv
Output = HummingbirdDataWorking_ein_exact.csv

5. ein_fuzzy.py - Same Data, same logic. Just accounts for a bit of "fuzz" in the naming giving some flexibility in terms of matching.
Inputs = HummingbirdDataWorking_ein_exact.csv", NonEd990.csv
Output= HummingbirdDataWorking_ein_exact_fuzzy

===========================================================================================

INCORPORATING 990s These files have different documentation and thus any updated file should be heavily scrutinized as having them line up is crucial They live in /data/990s

6a. 990_trim_combine - Trims down unneeded metrics from 990, standardizes column name and combines into a master sheet
Inputs = 24eoextract990pf.csv, 24eoextract990.csv, /24eoextract990EZ.csv
Outputs = 24eoextract990_core.csv, 24eoextract990EZ_core.csv, 24eoextract990pf_core.csv
990_combined.csv

6b. 990_analysis.py - Takes combined 990 data and produces one row per EIN with most recent filing + historical averages if available
Inputs =  990_combined.csv, HummingbirdDataWorking_ein_exact_fuzzy.csv
Output = 990_analysis.csv

6c. 990_integration.py - Merges 990 financials into Hummingbird data. NOTE - 2025 990s have not yet released. 2024 data is in, and has populated the "previous" financial fields.
Inputs = HummingbirdDataWorking_ein_exact_fuzzy.csv, 990_analysis.csv
Output = HummingbirdDataWorking_990_merged.csv

===========================================================================================

With Small additions of new insititutions this is where one can start. Adding new IPEDS and 990 data should be done in their respective steps, but HummingbirdDataWorking_990_merged.csv is treated as a new master set from here on out

7. geocode_all_missing - (NEEDS API KEY)a file that comprehensively attempts to fill in any missing latitude and longitude values. Anything remaining is likely a special case and should be done manually to ensure accuracy.
Input =  HummingbirdDataWorking_990_merged.csv
Output =  HummingbirdDataWorking_990_merged.csv

8. ein_all_missing - Similar to previous but for EIN. uses multiple strategies to find remaning EINs on propublica database. Currently 250 remaining unfilled EIN
Input = HummingbirdDataWorking_990_merged.csv
Output = HummingbirdDataWorking_990_merged.csv

9. IPEDS_financials.py - uses IPEDS financial information to fill in financial information for colleges that we were unable to find 990 information for. 