import azure.functions as func
import logging
import requests
import json
from datetime import datetime, timedelta
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()

# ── Config ────────────────────────────────────────────────────────
MEDIA_IDs       = ["gskhw4w4lm", "v08dlrgr7v"]
KEY_VAULT_URL   = "https://wistia-keyvault-lm.vault.azure.net/"
STORAGE_ACCOUNT = "wistiaadls"
CONTAINER       = "raw"
START_DATE      = "2026-03-06"
WATERMARK_BLOB  = "watermark/last_ingested.txt"


# ── Timer Trigger ─────────────────────────────────────────────────
@app.timer_trigger(schedule="0 0 8 * * *",
                   arg_name="myTimer",
                   run_on_startup=False)
def wistia_ingestion(myTimer: func.TimerRequest) -> None:
    logging.info("🚀 Wistia ingestion pipeline started")
    try:
        credential    = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
        try:
            API_TOKEN = secret_client.get_secret("wistia-api-token").value
            logging.info("✅ API fetched from Key Vault successfully")
        except Exception as e:
            logging.warning(f"⚠️ Key Vault api key fetching failed") 

        HEADERS     = {"Authorization": f"Bearer {API_TOKEN}"}
        adls_client = DataLakeServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
            credential=credential
        )

        start_date = get_last_ingested_date(adls_client)
        end_date   = datetime.today().strftime("%Y-%m-%d")

        if start_date > end_date:
            logging.info("✅ Pipeline already up to date.")
            return

        logging.info(f"📅 Ingesting from {start_date} to {end_date}")

        for media_id in MEDIA_IDs:
            logging.info(f"{'='*50}")
            logging.info(f"Processing Media ID: {media_id}")
            run_ingestion_for_media(media_id, start_date, end_date, HEADERS, adls_client, logging.info)

        update_watermark(adls_client, end_date)
        logging.info("🎉 Ingestion complete!")

    except Exception as e:
        logging.error(f"❌ Execution Error: {e}")
        raise


# ── Test HTTP Trigger ─────────────────────────────────────────────
@app.route(route="test", auth_level=func.AuthLevel.ANONYMOUS)
def test_trigger(req: func.HttpRequest) -> func.HttpResponse:
    results = []

    def log(msg):
        logging.info(msg)
        results.append(msg)

    log("🧪 Test trigger fired")

    try:
        credential = DefaultAzureCredential()

        # ── Test 1: Key Vault ─────────────────────────────────────
        try:
            secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
            API_TOKEN = secret_client.get_secret("wistia-api-token").value
            log("✅ Key Vault OK")
        except Exception as e:
            log(f"❌ Key Vault Error: {e}")
            log("⚠️ Falling back to hardcoded token")
            API_TOKEN = "0323ade64e13f79821bdc0f2a9410d9ec3873aa9df01f8a4a54d4e0f3dd2e6b4"

        # ── Test 2: ADLS Connection ───────────────────────────────
        try:
            adls_client = DataLakeServiceClient(
                account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
                credential=credential
            )
            fs = adls_client.get_file_system_client(CONTAINER)
            fs.get_file_system_properties()
            log("✅ ADLS Connection OK")
        except Exception as e:
            log(f"❌ ADLS Connection Error: {e}")
            return func.HttpResponse("\n".join(results), status_code=500)

        # ── Test 3: ADLS Write ────────────────────────────────────
        try:
            file_client = fs.get_file_client("test/test.json")
            file_client.upload_data(json.dumps({"test": "ok"}), overwrite=True)
            log("✅ ADLS Write OK")
        except Exception as e:
            log(f"❌ ADLS Write Error: {e}")
            return func.HttpResponse("\n".join(results), status_code=500)

        # ── Test 4: Wistia API ────────────────────────────────────
        try:
            HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}
            data = call_api("https://api.wistia.com/v1/medias/v08dlrgr7v.json", HEADERS)
            if data:
                log(f"✅ Wistia API OK — Media: {data.get('name')}")
            else:
                log("❌ Wistia API returned no data")
        except Exception as e:
            log(f"❌ Wistia API Error: {e}")
            return func.HttpResponse("\n".join(results), status_code=500)

        # ── Test 5: Full Ingestion ────────────────────────────────
        log("🚀 Starting full ingestion...")
        start_date = get_last_ingested_date(adls_client)
        end_date   = datetime.today().strftime("%Y-%m-%d")
        log(f"📅 Ingesting from {start_date} to {end_date}")

        if start_date > end_date:
            log("✅ Already up to date — nothing to ingest")
        else:
            for media_id in MEDIA_IDs:
                log(f"\n{'='*40}")
                log(f"📹 Processing {media_id}")
                run_ingestion_for_media(media_id, start_date, end_date, HEADERS, adls_client, log)

            update_watermark(adls_client, end_date)
            log("🎉 Ingestion complete!")

        return func.HttpResponse("\n".join(results), status_code=200)

    except Exception as e:
        results.append(f"❌ Fatal Error: {e}")
        logging.error(f"❌ Fatal Error: {e}")
        return func.HttpResponse("\n".join(results), status_code=500)


# ── Core Ingestion Logic ──────────────────────────────────────────
def run_ingestion_for_media(media_id, start_date, end_date, headers, adls_client, log):
    """
    Runs full ingestion for one media ID.
    - Metadata: one file, partitioned by actual created_at date
    - Stats: one file per day, partitioned by actual data date
    - Events: one file per day, partitioned by actual event date
    """

    # 1. Metadata — one file, use created date as partition
    metadata = call_api(f"https://api.wistia.com/v1/medias/{media_id}.json", headers)
    if metadata:
        created_date = metadata.get("created", end_date)[:10]  # extract YYYY-MM-DD
        success = save_to_adls(
            adls_client, metadata,
            f"metadata/media_id={media_id}/date={created_date}/metadata.json"
        )
        log(f"{'✅' if success else '❌'} Metadata {'saved' if success else 'FAILED'} for {media_id} (date={created_date})")

    # 2. Stats by date — split each day into its own partition
    stats = call_api(
        f"https://api.wistia.com/v1/stats/medias/{media_id}/by_date.json",
        headers,
        params={"start_date": start_date, "end_date": end_date}
    )
    if stats:
        stats_saved = 0
        for day_stat in stats:
            data_date = day_stat.get("date")  # already YYYY-MM-DD
            success = save_to_adls(
                adls_client,
                day_stat,
                f"stats_by_date/media_id={media_id}/date={data_date}/stats.json"
            )
            if success:
                stats_saved += 1
        log(f"✅ Stats saved for {media_id} — {stats_saved} day partitions")

    # 3. Events — fetch all, then split by event date into separate partitions
    events = fetch_events(media_id, start_date, end_date, headers, log)
    if events:
        # Group events by date
        events_by_date = {}
        for event in events:
            event_date = event.get("received_at", "")[:10]  # extract YYYY-MM-DD
            if event_date not in events_by_date:
                events_by_date[event_date] = []
            events_by_date[event_date].append(event)

        # Save one file per date partition
        for event_date, day_events in events_by_date.items():
            success = save_to_adls(
                adls_client,
                day_events,
                f"events/media_id={media_id}/date={event_date}/events.json"
            )
            log(f"{'✅' if success else '❌'} Events {'saved' if success else 'FAILED'} for {media_id} date={event_date} — {len(day_events)} events")


# ── API Call Helper ───────────────────────────────────────────────
def call_api(url, headers, params=None):
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        logging.error(f"⚠️ API error {response.status_code}: {response.text}")
    except Exception as e:
        logging.error(f"❌ Request failed: {e}")
    return None


# ── Pagination Helper ─────────────────────────────────────────────
def fetch_events(media_id, start_date, end_date, headers, log=logging.info):
    all_events = []
    page = 1
    while True:
        params = {
            "media_id"  : media_id,
            "start_date": start_date,
            "end_date"  : end_date,
            "per_page"  : 100,
            "page"      : page
        }
        data = call_api("https://api.wistia.com/v1/stats/events.json", headers, params)
        if not data:
            break
        all_events.extend(data)
        log(f"   Page {page}: {len(data)} events (total: {len(all_events)})")
        if len(data) < 100:
            break
        page += 1
    return all_events


# ── ADLS Write Helper ─────────────────────────────────────────────
def save_to_adls(adls_client, data, path):
    try:
        fs          = adls_client.get_file_system_client(CONTAINER)
        file_client = fs.get_file_client(path)
        file_client.upload_data(json.dumps(data, indent=2), overwrite=True)
        logging.info(f"✅ Saved to ADLS: {path}")
        return True
    except Exception as e:
        logging.error(f"❌ ADLS Error saving {path}: {e}")
        return False


# ── Watermark Helpers ─────────────────────────────────────────────
def get_last_ingested_date(adls_client):
    try:
        fs          = adls_client.get_file_system_client(CONTAINER)
        file_client = fs.get_file_client(WATERMARK_BLOB)
        download    = file_client.download_file()
        last_date   = download.readall().decode("utf-8").strip()
        if last_date:
            next_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            logging.info(f"📌 Watermark found: {last_date}, starting from {next_date}")
            return next_date
    except Exception:
        logging.info(f"📌 No watermark found, using START_DATE: {START_DATE}")
    return START_DATE


def update_watermark(adls_client, date: str):
    try:
        fs          = adls_client.get_file_system_client(CONTAINER)
        file_client = fs.get_file_client(WATERMARK_BLOB)
        file_client.upload_data(date.encode("utf-8"), overwrite=True)
        logging.info(f"📌 Watermark updated to {date}")
    except Exception as e:
        logging.error(f"❌ Failed to update watermark: {e}")