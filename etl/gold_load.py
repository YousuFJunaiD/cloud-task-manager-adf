import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()

# =====================
# CONFIG
# =====================
PIPELINE_NAME = "gold_task_events"
CONTAINER_SILVER = "processed"
PARQUET_FILE = "task_events_silver.parquet"

SQL_SERVER = "ctm-sql-server-yusuf.database.windows.net"
SQL_DB = "task_events_db"
SQL_USER = os.getenv("AZURE_SQL_USER")
SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")

# =====================
# CONNECT TO SQL
# =====================
engine = create_engine(
    f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}"
    f"@{SQL_SERVER}:1433/{SQL_DB}"
    "?driver=ODBC+Driver+18+for+SQL+Server"
)

# =====================
# READ WATERMARK
# =====================
with engine.connect() as conn:
    result = conn.execute(
        text("""
        SELECT last_event_time
        FROM etl_watermark
        WHERE pipeline_name = :name
        """),
        {"name": PIPELINE_NAME}
    ).fetchone()

last_event_time = result[0]
print("⏱ Last processed event_time:", last_event_time)

# =====================
# READ SILVER DATA
# =====================
blob_service = BlobServiceClient.from_connection_string(
    os.getenv("AZURE_STORAGE_CONNECTION_STRING")
)

container = blob_service.get_container_client(CONTAINER_SILVER)
blob = container.get_blob_client(PARQUET_FILE)

data = blob.download_blob().readall()
df = pd.read_parquet(BytesIO(data))

# =====================
# FILTER INCREMENTAL DATA
# =====================
df["event_time"] = pd.to_datetime(df["event_time"]).dt.tz_convert(None)
last_event_time = pd.to_datetime(last_event_time)
incremental_df = df[df["event_time"] > last_event_time]

if incremental_df.empty:
    print("✅ No new data to load.")
    exit()

print("📥 New rows to load:", len(incremental_df))

# =====================
# LOAD INTO GOLD
# =====================
incremental_df[[
    "task_id",
    "event_type",
    "event_date",
    "event_hour"
]].to_sql(
    "task_events_gold",
    engine,
    if_exists="append",
    index=False
)

# =====================
# UPDATE WATERMARK
# =====================
new_watermark = incremental_df["event_time"].max()

with engine.connect() as conn:
    conn.execute(
        text("""
        UPDATE etl_watermark
        SET last_event_time = :ts
        WHERE pipeline_name = :name
        """),
        {"ts": new_watermark, "name": PIPELINE_NAME}
    )
    conn.commit()

print("✅ Incremental Gold load complete.")
print("🆕 Watermark updated to:", new_watermark)
