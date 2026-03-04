import azure.functions as func
import logging
import requests
import json
import os
from datetime import datetime, timedelta
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

# This is required for the V2 model to index your functions correctly
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# ── Config ────────────────────────────────────────────────────────
MEDIA_IDs       = ["gskhw4w4lm", "v08dlrgr7v"]
KEY_VAULT_URL   = "https://wistia-keyvault-lm.vault.azure.net/"
STORAGE_ACCOUNT = "wistiaadls"
CONTAINER       = "raw"
START_DATE      = "2024-12-04"  
WATERMARK_FILE  = "last_ingested.txt"

# ── Timer Trigger — runs daily at 8am UTC ────────────────────────
@app.timer_trigger(schedule="0 0 8 * * *", 
                   arg_name="myTimer", 
                   run_on_startup=False)
def wistia_ingestion(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is running late!')

    logging.info("🚀 Wistia ingestion pipeline started")

    try:
        # ── Get API token from Key Vault ──────────────────────────────
        credential    = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
        # Using .get_secret().value directly
        API_TOKEN     = secret_client.get_secret("wistia-api-token").value
        HEADERS       = {"Authorization": f"Bearer {API_TOKEN}"}

        # ── ADLS Client ───────────────────────────────────────────────
        adls_client = DataLakeServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
            credential=credential
        )

        # ── Incremental Watermark Logic ───────────────────────────────
        start_date = get_last_ingested_date()
        # Fix: For your 7-day run, you might want to hardcode this or use today
        end_date   = datetime.today().strftime("%Y-%m-%d")

        if start_date > end_date:
            logging.info("✅ Pipeline already up to date. Nothing to ingest.")
            return

        for media_id in MEDIA_IDs:
            # 1. Media Metadata
            metadata = call_api(f"https://api.wistia.com/v1/medias/{media_id}.json", HEADERS)
            if metadata:
                save_to_adls(adls_client, metadata, f"metadata/media_id={media_id}/date={end_date}/metadata.json")

            # 2. Stats by Date
            stats = call_api(f"https://api.wistia.com/v1/stats/medias/{media_id}/by_date.json", 
                             HEADERS, params={"start_date": start_date, "end_date": end_date})
            if stats:
                save_to_adls(adls_client, stats, f"stats_by_date/media_id={media_id}/date={end_date}/stats.json")

            # 3. Events
            events = fetch_events(media_id, start_date, end_date, HEADERS)
            if events:
                save_to_adls(adls_client, events, f"events/media_id={media_id}/date={end_date}/events.json")

        update_watermark(end_date)
        logging.info("🎉 Ingestion complete!")

    except Exception as e:
        logging.error(f"❌ Global Pipeline Error: {e}")

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



