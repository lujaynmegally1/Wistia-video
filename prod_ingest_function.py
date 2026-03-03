import azure.functions as func
import logging
import requests
import json
import os
from datetime import datetime, timedelta
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()

# ── Config ────────────────────────────────────────────────────────
MEDIA_IDs       = ["gskhw4w4lm", "v08dlrgr7v"]
KEY_VAULT_URL   = "https://wistia-keyvault-lm.vault.azure.net/"
STORAGE_ACCOUNT = "wistiaadlslm"
CONTAINER       = "raw"
START_DATE      = "2024-12-04"  # fallback if no watermark
WATERMARK_FILE  = "last_ingested.txt"


# ── Timer Trigger — runs daily at 8am UTC ────────────────────────
@app.timer_trigger(schedule="0 0 8 * * *",
                   arg_name="myTimer",
                   run_on_startup=False)
def wistia_ingestion(myTimer: func.TimerRequest) -> None:
    logging.info("🚀 Wistia ingestion pipeline started")

    # ── Get API token from Key Vault ──────────────────────────────
    credential    = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
    API_TOKEN     = secret_client.get_secret("wistia-api-token").value
    HEADERS       = {"Authorization": f"Bearer {API_TOKEN}"}

    # ── ADLS Client ───────────────────────────────────────────────
    adls_client = DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
        credential=credential
    )

    # ── Incremental Watermark Logic ───────────────────────────────
    start_date = get_last_ingested_date()
    end_date   = datetime.today().strftime("%Y-%m-%d")

    if start_date > end_date:
        logging.info("✅ Pipeline already up to date. Nothing to ingest.")
        return

    logging.info(f"📅 Ingesting from {start_date} to {end_date}")

    # ── Process Each Media ID ─────────────────────────────────────
    for media_id in MEDIA_IDs:
        logging.info(f"\n{'='*50}")
        logging.info(f"Processing Media ID: {media_id}")
        logging.info(f"{'='*50}")

        # 1. Media Metadata → dim_media
        logging.info(f"📹 Fetching metadata for {media_id}")
        metadata = call_api(
            f"https://api.wistia.com/v1/medias/{media_id}.json",
            HEADERS
        )
        if metadata:
            save_to_adls(
                adls_client,
                metadata,
                f"metadata/media_id={media_id}/date={end_date}/metadata.json"
            )

        # 2. Stats by Date → fact_media_engagement_daily
        logging.info(f"📊 Fetching stats by date for {media_id} ({start_date} to {end_date})")
        stats = call_api(
            f"https://api.wistia.com/v1/stats/medias/{media_id}/by_date.json",
            HEADERS,
            params={"start_date": start_date, "end_date": end_date}
        )
        if stats:
            save_to_adls(
                adls_client,
                stats,
                f"stats_by_date/media_id={media_id}/date={end_date}/stats.json"
            )

        # 3. Events with Pagination → dim_visitor + fact_visitor_events
        logging.info(f"👥 Fetching events for {media_id} ({start_date} to {end_date})")
        events = fetch_events(media_id, start_date, end_date, HEADERS)
        if events:
            save_to_adls(
                adls_client,
                events,
                f"events/media_id={media_id}/date={end_date}/events.json"
            )

    # ── Update Watermark After Successful Run ─────────────────────
    update_watermark(end_date)
    logging.info("🎉 Ingestion complete!")


# ── API Call Helper ───────────────────────────────────────────────
def call_api(url, headers, params=None):
    """Make API call with error handling. Returns JSON or None."""
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            logging.error("❌ Unauthorized: Check API token in Key Vault.")
        elif response.status_code == 404:
            logging.error(f"❌ Not found: {url}")
        else:
            logging.error(f"⚠️ API error {response.status_code}: {response.text}")
    except Exception as e:
        logging.error(f"❌ Request failed: {e}")
    return None


# ── Pagination Helper ─────────────────────────────────────────────
def fetch_events(media_id, start_date, end_date, headers):
    """
    Fetch all visitor events with pagination.
    Loops through pages of 100 until fewer than 100 results returned.
    """
    all_events = []
    page       = 1
    per_page   = 100

    while True:
        params = {
            "media_id"  : media_id,
            "start_date": start_date,
            "end_date"  : end_date,
            "per_page"  : per_page,
            "page"      : page
        }

        data = call_api("https://api.wistia.com/v1/stats/events.json", headers, params)

        if not data:
            logging.info(f"   No data on page {page} — stopping.")
            break

        all_events.extend(data)
        logging.info(f"   Page {page}: {len(data)} events (total: {len(all_events)})")

        # Fewer than per_page results = last page reached
        if len(data) < per_page:
            logging.info("   Last page reached.")
            break

        page += 1

    logging.info(f"✅ Total events for {media_id}: {len(all_events)}")
    return all_events


# ── ADLS Write Helper ─────────────────────────────────────────────
def save_to_adls(adls_client, data, path):
    """
    Write JSON data to ADLS Gen2.
    Path follows Hive-style partitioning:
    /{data_type}/media_id={id}/date={run_date}/filename.json
    """
    try:
        fs          = adls_client.get_file_system_client(CONTAINER)
        file_client = fs.get_file_client(path)
        file_client.upload_data(json.dumps(data, indent=2), overwrite=True)
        logging.info(f"💾 Saved to ADLS: {path}")
    except Exception as e:
        logging.error(f"❌ ADLS write error for {path}: {e}")


# ── Watermark Helpers ─────────────────────────────────────────────
def get_last_ingested_date():
    """
    Read watermark file and return the day AFTER last ingested date.
    This prevents re-ingesting data from the last successful run.
    Falls back to START_DATE if no watermark exists or file is empty.
    """
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, "r") as f:
            last_date = f.read().strip()

            if not last_date:
                logging.info(f"📅 Watermark empty — using default: {START_DATE}")
                return START_DATE

            next_date = (
                datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")

            logging.info(f"📅 Watermark found: last ingested = {last_date}, starting from = {next_date}")
            return next_date

    logging.info(f"📅 No watermark found — using default start date: {START_DATE}")
    return START_DATE


def update_watermark(date: str):
    """Write today's run date as the new watermark."""
    with open(WATERMARK_FILE, "w") as f:
        f.write(date)
    logging.info(f"✅ Watermark updated to: {date}")

# import requests
# import json
# import os
# from datetime import datetime, timedelta

# # ── Configuration ────────────────────────────────────────────────
# API_TOKEN = "0323ade64e13f79821bdc0f2a9410d9ec3873aa9df01f8a4a54d4e0f3dd2e6b4"
# MEDIA_IDs = ["gskhw4w4lm", "v08dlrgr7v"]
# HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}

# # For testing locally, we use the known active date range
# # In production, START_DATE will come from a watermark file
# START_DATE = "2024-12-04"
# END_DATE   = "2024-12-13"

# # Watermark file path — tracks last successful ingestion date
# WATERMARK_FILE = "last_ingested.txt"

# # Output folder for raw JSON (locally simulates ADLS)
# OUTPUT_DIR = "raw_output"
# os.makedirs(OUTPUT_DIR, exist_ok=True)


# # ── Watermark Helpers ─────────────────────────────────────────────
# def get_last_ingested_date():
#     """Read watermark — returns day AFTER last ingested date to avoid re-ingesting."""
#     if os.path.exists(WATERMARK_FILE):
#         with open(WATERMARK_FILE, "r") as f:
#             last_date = f.read().strip()
#             next_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
#             print(f"📅 Watermark found: last ingested = {last_date}, starting from = {next_date}")
#             return next_date
#     print(f"📅 No watermark found — using default start date: {START_DATE}")
#     return START_DATE

# def update_watermark(date: str):
#     """Write today's date as the new watermark after successful run."""
#     with open(WATERMARK_FILE, "w") as f:
#         f.write(date)
#     print(f"✅ Watermark updated to: {date}")


# # ── Save Raw JSON ─────────────────────────────────────────────────
# def save_json(data, filename):
#     """Save raw API response to local folder (simulates ADLS landing zone)."""
#     path = os.path.join(OUTPUT_DIR, filename)
#     with open(path, "w") as f:
#         json.dump(data, f, indent=2)
#     print(f"💾 Saved: {path}")


# # ── API Call Helper ───────────────────────────────────────────────
# def call_api(url, params=None):
#     """Make API call with basic error handling."""
#     response = requests.get(url, headers=HEADERS, params=params)
#     if response.status_code == 200:
#         return response.json()
#     elif response.status_code == 401:
#         print("❌ Unauthorized: Check your API token.")
#     elif response.status_code == 404:
#         print(f"❌ Not found: {url}")
#     else:
#         print(f"⚠️ Error {response.status_code}: {response.text}")
#     return None


# # ── Fetch Media Metadata ──────────────────────────────────────────
# def fetch_media_metadata(media_id):
#     """Fetch media metadata — feeds dim_media."""
#     print(f"\n📹 Fetching metadata for media ID: {media_id}")
#     url = f"https://api.wistia.com/v1/medias/{media_id}.json"
#     data = call_api(url)
#     if data:
#         print(f"✅ Metadata retrieved for {media_id}")
#         save_json(data, f"metadata_{media_id}.json")
#     return data


# # ── Fetch Stats by Date ───────────────────────────────────────────
# def fetch_stats_by_date(media_id, start_date, end_date):
#     """Fetch daily aggregated stats — feeds fact_media_engagement_daily."""
#     print(f"\n📊 Fetching stats by date for media ID: {media_id} ({start_date} to {end_date})")
#     url = f"https://api.wistia.com/v1/stats/medias/{media_id}/by_date.json"
#     params = {"start_date": start_date, "end_date": end_date}
#     data = call_api(url, params)
#     if data:
#         print(f"✅ Stats by date retrieved for {media_id} — {len(data)} days")
#         save_json(data, f"stats_by_date_{media_id}_{start_date}_to_{end_date}.json")
#     return data


# # ── Fetch Events (with Pagination) ───────────────────────────────
# def fetch_events(media_id, start_date, end_date):
#     """
#     Fetch all visitor events with pagination.
#     Feeds dim_visitor + fact_visitor_events.
    
#     Pagination logic:
#     - Request page 1 with per_page=100
#     - If we get 100 results back, there might be more — request page 2
#     - Keep going until we get fewer than 100 results (we've hit the last page)
#     """
#     print(f"\n👥 Fetching events for media ID: {media_id} ({start_date} to {end_date})")
    
#     all_events = []
#     page = 1
#     per_page = 100

#     while True:
#         params = {
#             "media_id": media_id,
#             "start_date": start_date,
#             "end_date": end_date,
#             "per_page": per_page,
#             "page": page
#         }
        
#         print(f"   Fetching page {page}...")
#         data = call_api("https://api.wistia.com/v1/stats/events.json", params)
        
#         if not data:
#             print(f"   No data returned on page {page} — stopping.")
#             break
        
#         all_events.extend(data)
#         print(f"   Page {page}: {len(data)} events retrieved (total so far: {len(all_events)})")
        
#         # If we got fewer results than per_page, this was the last page
#         if len(data) < per_page:
#             print(f"   Last page reached.")
#             break
        
#         page += 1  # Move to next page

#     print(f"✅ Total events retrieved for {media_id}: {len(all_events)}")
#     save_json(all_events, f"events_{media_id}_{start_date}_to_{end_date}.json")
#     return all_events


# # ── Main Ingestion Function ───────────────────────────────────────
# def run_ingestion():
#     print("🚀 Starting Wistia ingestion pipeline...\n")
#     # Incremental logic — get start date from watermark
#     start_date = get_last_ingested_date()
#     end_date = END_DATE  # In production: datetime.today().strftime("%Y-%m-%d")

#     # Guard: if start is ahead of end, nothing new to ingest
#     if start_date > end_date:
#         print(f"✅ Pipeline up to date. Start date {start_date} is ahead of end date {end_date}. Nothing to ingest.")
#         return

#     print(f"📅 Ingesting from {start_date} to {end_date}")

#     all_metadata = []
#     all_stats    = []
#     all_events   = []

#     for media_id in MEDIA_IDs:
#         print(f"\n{'='*50}")
#         print(f"Processing Media ID: {media_id}")
#         print(f"{'='*50}")

#         # 1. Media metadata — dim_media
#         metadata = fetch_media_metadata(media_id)
#         if metadata:
#             all_metadata.append(metadata)

#         # 2. Stats by date — fact_media_engagement_daily
#         stats = fetch_stats_by_date(media_id, start_date, end_date)
#         if stats:
#             all_stats.extend(stats)

#         # 3. Events with pagination — dim_visitor + fact_visitor_events
#         events = fetch_events(media_id, start_date, end_date)
#         if events:
#             all_events.extend(events)

#     # Update watermark to end_date after successful run
#     update_watermark(end_date)

#     print(f"\n🎉 Ingestion complete!")
#     print(f"   Media records : {len(all_metadata)}")
#     print(f"   Daily stats   : {len(all_stats)}")
#     print(f"   Events        : {len(all_events)}")


# # ── Entry Point ───────────────────────────────────────────────────
# if __name__ == "__main__":
#     run_ingestion()