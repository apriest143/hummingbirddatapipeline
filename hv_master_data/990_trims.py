### Trimming down unwanted columns in the 990s
import pandas as pd


###Standard 990
df = pd.read_csv("hv_master_data/data/990s/24eoextract990.csv", dtype=str)

keep_cols_990 = [
    "EIN",
    "tax_pd",
    "totrevenue",
    "totfuncexpns",
    "totassetsend",
    "totliabend",
    "totnetassetend"
]

df_990_core = df[keep_cols_990]
df_990_core.to_csv(
    "hv_master_data/data/990s/24eoextract990_core.csv",
    index=False
)


### 990pf
df = pd.read_csv("hv_master_data/data/990s/24eoextract990pf.csv", dtype=str)

keep_cols_990PF = [
    "EIN",
    "TAX_PRD",
    "TAX_YR",
    "TOTRCPTPERBKS",
    "TOTEXPNSPBKS",
    "TOTASSETSEND",
    "TOTLIABEND"
]

df_990PF_core = df[keep_cols_990PF]
df_990PF_core.to_csv(
    "hv_master_data/data/990s/24eoextract990pf_core.csv",
    index=False
)

### 990ez
import pandas as pd

df = pd.read_csv("hv_master_data/data/990s/24eoextract990EZ.csv", dtype=str)

keep_cols_990EZ = [
    "EIN",
    "taxpd",
    "totrevnue",
    "totexpns",
    "totassetsend",
    "totliabend",
    "totnetassetsend"
]

df_990EZ_core = df[keep_cols_990EZ]
df_990EZ_core.to_csv(
    "hv_master_data/data/990s/24eoextract990EZ_core.csv",
    index=False
)
