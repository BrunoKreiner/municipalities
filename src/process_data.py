import pandas as pd

# Load the data
df = pd.read_csv("../data/CompleteListFixed.csv")

# Drop rows where "Website" is NaN
df = df.dropna(subset=['Website'])

# Rename columns
df.columns = ['index', 'municipality', 'country', 'email', 'website', 'population', 'pop_d', 'status']

df = df[df['status'] != "invalid url"]
df = df[df['status'] != "no website found"]
df = df[df['status'] != "no connection to page"]
# Write the DataFrame back to the CSV file
df.to_csv("../data/urls_processed.csv", index=False)
