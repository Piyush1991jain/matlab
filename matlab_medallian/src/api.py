from fastapi import FastAPI
from databricks import sql
import os

app = FastAPI()

conn = sql.connect(
    server_hostname=os.getenv("DATABRICKS_HOST"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN")
)

@app.get("/alerts")
def get_alerts():
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM gold.sensor_alerts LIMIT 100")
    return cursor.fetchall()

@app.post("/insert")
def insert_record():
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO gold.sensor_alerts VALUES
        ('2024-01-01', 'EQ1', 'temp', 80, 'WARNING')
    """)
    return {"message": "Inserted"}