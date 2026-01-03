import time
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

from py_scripts.db_operations import get_conn_analytics
from py_scripts.rds_provision import get_rds_conn_info

# ----------------------------
# A connection to the RDS instance using the analytics user
# ----------------------------
conn_info = get_rds_conn_info(inst_name="bikesharedb", reg_name="us-east-1")
conn = get_conn_analytics(conn_info)

# ----------------------------
# Wrap existing psycopg2 connection for Pandas using SQLAlchemy
# ----------------------------
engine = create_engine("postgresql+psycopg2://", creator=lambda: conn)

# ----------------------------
# A sample analytics query
# ----------------------------
dbquery = """
SELECT 
    EXTRACT(YEAR  FROM started_at) AS year,
    EXTRACT(MONTH FROM started_at) AS month,
    EXTRACT(DAY   FROM started_at) AS day,
    EXTRACT(HOUR  FROM started_at) AS hour,
    member_casual,
    COUNT(*) AS cnt
FROM rides_raw
GROUP BY year, month, day, hour, member_casual
ORDER BY year, month, day, hour, member_casual;
"""

print("Executing the database query and assigning returned results to a pandas dataframe...")
start = time.perf_counter()
df = pd.read_sql_query(dbquery, engine)
elapsed = time.perf_counter() - start
print(f"Query returned {len(df):,} rows in {elapsed:.2f} seconds")
# Takes about 3 minutes to run, just a heads up for the first time :)

# ----------------------------
# Plot
# ----------------------------
yearly = (
    df.groupby("year", as_index=False)["cnt"]
      .sum()
)

print("Plotting the results...")
plt.figure(figsize=(8, 5))
plt.bar(yearly["year"], yearly["cnt"])
plt.xlabel("Year")
plt.ylabel("Total ride count")
plt.title("Total bikeshare rides per year")
plt.xticks(yearly["year"])  # ensure every year is shown
plt.tight_layout()
plt.show()
