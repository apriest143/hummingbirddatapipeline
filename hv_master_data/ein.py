##Trying to get some EIN info in here - using propublica for now.
import pandas as pd

# read the Excel file
df = pd.read_excel("HummingbirdDataWorking.xlsx")

# write to CSV
df.to_csv("HummingbirdDataWorking.csv", index=False)

