from pathlib import Path
import pandas as pd
from py_scripts.rds_provision import delete_rds, create_rds, get_rds_conn_info, create_inbound_rule
from py_scripts.fetch_raw_data import get_bikeshare_data, get_weather_data
from py_scripts.db_operations import copy_df_weather, get_conn, create_db_tables, is_table_populated, copy_df_bikeshare, copy_df_weather, create_rouser
from py_scripts.prep_data import normalize_bikeshare_df, daily_weather_columns, hourly_weather_columns

# ----------
# Step 1: Provision a PostgreSQL RDS instance on AWS. This will serve as our read-only analytics database instance.
# Prerequisite: Have your AWS credentials set up in your environtment so that boto client can make calls.
# ----------
# delete_rds("bikesharedb", "us-east-1")
create_rds("bikesharedb", "us-east-1")
create_inbound_rule("bikesharedb", "us-east-1")

# ----------
# Step 2: Create the tables in the aptly named 'bikesharedb' database to keep our data. Two substeps here: first we establish a connection to our previously crated db and then call the create_db_tables() function giving the cursor of the connection we just created as the input.
# ----------
conn_info = get_rds_conn_info(inst_name="bikesharedb", reg_name="us-east-1")
conn = get_conn(conn_info)
cur = conn.cursor()

create_db_tables(cur)

#----------
# Step 3: Fetch the raw data from their publishing sources: I) capital bikeshare trip data from lyft will be streamed to csv files in a created 'bikeshare_csv' folder and II) historical weather data from Open-meteo. Since weather data will not be stored but will directly be written to database as soon as the API call returns, the get_weather_data() function will be called when it's time to write the data to the database
#----------
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "bikeshare_csv"

get_bikeshare_data(PROJECT_ROOT)

#----------
# Step 4: Populate the tables in the database with normalized trips and weather data. Before being written to rides_raw table, the trips data requires some extensive normalization which is handled by the normalize_bikeshare_df() function. The weather data is fetched via an API call and are intermittently stored in data frames, which are ultimately written to their respective daily_weather and horuly_weather tables.
#----------
# populate rides_raw table
#----------
if is_table_populated(cur, "rides_raw"):
    print("rides_raw table already exists and is populated. Skipping CSV loading.")
else:
    for csv_path in DATA_DIR.glob("*.csv"):
        print(f"Loading {csv_path.name}")
        df = pd.read_csv(csv_path, low_memory=False)
        df = normalize_bikeshare_df(df)
        copy_df_bikeshare(cur, df)
#----------
#populate daily_weather and hourly_weather table
#----------
if is_table_populated(cur, "daily_weather") and is_table_populated(cur, "hourly_weather"):
    print("Weather tables already exists and is populated. Skippng data loading.")
else:
    weather_tuple = get_weather_data()
    targets = [
        ("daily_weather", daily_weather_columns),
        ("hourly_weather", hourly_weather_columns)
        ]
    for df, (table, columns) in zip(weather_tuple, targets):
        copy_df_weather(cur, df, table, columns)

#----------
# Step 5: Create a read-only analytics user (rouser) on the database and set privileges and connection limits. More elegant & industry standard solutions to this requirement would be through IAM and roles but for small groups of analytics people, this looks sufficient.
#----------
create_rouser(cur)

cur.close()
conn.close()