import os
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import BytesIO

# ======================
# Load environment vars
# ======================
load_dotenv()

# ======================
# Azure containers
# ======================
CONTAINER_RAW = "raw"
CONTAINER_SILVER = "processed"

# ======================
# Azure Blob client
# ======================
blob_service = BlobServiceClient.from_connection_string(
    os.getenv("AZURE_STORAGE_CONNECTION_STRING")
)

# ======================
# Read raw (Bronze) data
# ======================
def read_raw_events():
    container = blob_service.get_container_client(CONTAINER_RAW)

    # We only read task event exports
    blobs = container.list_blobs(name_starts_with="task_events")

    events = []

    for blob in blobs:
        blob_client = container.get_blob_client(blob.name)
        data = blob_client.download_blob().readall()

        # Each blob contains a JSON array
        events.extend(json.loads(data))

    if not events:
        raise ValueError("❌ No raw events found in bronze layer")

    return pd.DataFrame(events)

# ======================
# Transform → SILVER
# ======================
def transform_to_silver(df):
    # ---- Schema validation (REAL DE work)
    expected_cols = {
        "event_id",
        "task_id",
        "event_type",
        "event_time"
    }

    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"❌ Missing columns in raw data: {missing}")

    # ---- Convert timestamps safely
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")

    # ---- Drop corrupt records
    df = df.dropna(subset=["event_time"])

    # ---- Add analytical columns
    df["event_date"] = df["event_time"].dt.date
    df["event_hour"] = df["event_time"].dt.hour

    # ---- Silver = clean, deduplicated, analytics-ready
    silver_df = df[[
    "task_id",
    "event_type",
    "event_time",   # 👈 KEEP THIS
    "event_date",
    "event_hour"
]].drop_duplicates()

    return silver_df

# ======================
# Write Silver to Azure
# ======================
def write_silver(df):
    container = blob_service.get_container_client(CONTAINER_SILVER)

    # Create container if not exists
    try:
        container.create_container()
    except Exception:
        pass

    output = BytesIO()
    df.to_parquet(output, index=False)
    output.seek(0)

    blob_client = container.get_blob_client(
        "task_events_silver.parquet"
    )

    blob_client.upload_blob(output, overwrite=True)

# ======================
# MAIN
# ======================
if __name__ == "__main__":
    raw_df = read_raw_events()
    silver_df = transform_to_silver(raw_df)
    write_silver(silver_df)

    print("✅ Silver layer created and uploaded successfully")
