import pandas as pd

df = pd.read_csv("hv_master_data/data/HummingbirdDataWorking_ein_exact_fuzzy.csv", dtype=str)  

institution_types = [
    "Private College",
    "Religious Institution",
    "Wellness/Retreat",
    "Tribal Centers"
]
for inst_type in institution_types:
    missing = df[
        (df["institution_type"] == inst_type) &
        (df["ein_number"].isna() | (df["ein_number"].str.strip() == ""))
    ]
    print(f"{inst_type} missing EINs: {len(missing)}")

