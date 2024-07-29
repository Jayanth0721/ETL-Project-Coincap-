import requests
import pandas as pd
import duckdb

# =============================================================================
# #Extract
# =============================================================================

url = 'http://api.coincap.io/v2/assets'

headers={"Content-Type": "application/json",
        "Accept-Encoding":"deflate"}

response = requests.get(url, headers=headers)


# Extracting the JSON data from the response
rs_data = response.json()

df = pd.json_normalize(rs_data['data'])

# =============================================================================
# #Transform
# =============================================================================

# Print the original DataFrame
print("Original DataFrame for maxSupply column:")
print(df['maxSupply'])

# Fill None values in the maxSupply column with "NOPE"
df['maxSupply'] = df['maxSupply'].fillna("NA")

# Print the DataFrame with filled values
print("DataFrame with filled NA values (T):")
print(df['maxSupply'])

# =============================================================================
# Load
# =============================================================================

#Load to csv & database
print("Export to csv as output.csv")
df.to_csv('C:\projects_folder\json_etl\output.csv', index=False)

# Specify the full path to the DuckDB database file
database_path = 'C:\Database\crypto.duckdb'  # replace with your desired directory path

# Connect to DuckDB (on-disk database in the specified directory)
con = duckdb.connect(database=database_path)

# Load DataFrame into DuckDB
con.execute('CREATE TABLE Coincap AS SELECT * FROM df')
print("duckdb query data")

# Query the table
result = con.execute('SELECT * FROM Coincap').df()
print(result)

#close 
con.close()
