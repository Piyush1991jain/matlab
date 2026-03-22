from databricks import sql
import os

host = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
token = os.getenv("DATABRICKS_TOKEN")

conn = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=token
)

cursor = conn.cursor()


print("Running Bronze layer...")
cursor.execute("""
CREATE TABLE IF NOT EXISTS bronze.sensor_raw
USING DELTA AS
SELECT * FROM csv.`/Volumes/test/default/data/sample.csv`
""")

print("Running Silver layer...")
cursor.execute("""
CREATE OR REPLACE TABLE silver.sensor_clean AS
SELECT *
FROM bronze.sensor_raw
WHERE value IS NOT NULL
""")

print("Running Gold layer...")
cursor.execute("""
CREATE OR REPLACE TABLE gold.sensor_alerts AS
SELECT *,
CASE 
    WHEN value > 90 THEN 'CRITICAL'
    WHEN value > 70 THEN 'WARNING'
    ELSE 'NORMAL'
END AS status
FROM silver.sensor_clean
""")

print("Pipeline executed successfully!")