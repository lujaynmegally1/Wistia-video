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

# ── Retry Config ──────────────────────────────────────────────────
MAX_RETRIES   = 3
RETRY_BACKOFF = 2  # exponential backoff: 2s, 4s, 8s

# ── ADF Production Ingest Trigger ────────────────────────────────
@app.route(route="test", auth_level=func.AuthLevel.ANONYMOUS)
def ingest_trigger(req: func.HttpRequest) -> func.HttpResponse:
    pipeline_start = datetime.utcnow()
    logging.info("=" * 60)
    logging.info(f"🚀 ADF triggered ingestion started at {pipeline_start} UTC")

    try:
        credential = DefaultAzureCredential()

        # ── Key Vault ─────────────────────────────────────────────
        try:
            secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
            API_TOKEN     = secret_client.get_secret("wistia-api-token").value
            logging.info("✅ Key Vault: secret retrieved successfully")
        except Exception as e:
            # No fallback — fail loudly so the issue is visible and fixed
            logging.error(f"❌ Key Vault failed — cannot proceed without API token: {e}")
            return func.HttpResponse(
                f"❌ Key Vault error: {e}",
                status_code=500
            )

        HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}

        # ── ADLS ──────────────────────────────────────────────────
        try:
            adls_client = DataLakeServiceClient(
                account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
                credential=credential
            )
            logging.info("✅ ADLS: client initialized successfully")
        except Exception as e:
            logging.error(f"❌ ADLS: failed to initialize client: {e}")
            return func.HttpResponse(f"❌ ADLS connection error: {e}", status_code=500)

        # ── Watermark ─────────────────────────────────────────────
        start_date = get_last_ingested_date(adls_client)
        end_date   = datetime.today().strftime("%Y-%m-%d")

        if start_date > end_date:
            logging.info("✅ Already up to date — nothing to ingest")
            return func.HttpResponse("✅ Already up to date", status_code=200)

        logging.info(f"📅 Ingestion window: {start_date} → {end_date}")

        # ── Process Each Media ID ─────────────────────────────────
        pipeline_success = True
        for media_id in MEDIA_IDs:
            logging.info("=" * 40)
            logging.info(f"📹 Processing media_id: {media_id}")
            media_start = datetime.utcnow()
            try:
                run_ingestion_for_media(
                    media_id, start_date, end_date, HEADERS, adls_client, logging.info
                )
                elapsed = (datetime.utcnow() - media_start).seconds
                logging.info(f"✅ {media_id} completed in {elapsed}s")
            except Exception as e:
                logging.error(f"❌ {media_id} failed: {e}")
                pipeline_success = False
                continue  # process next media ID even if this one fails

        # ── Watermark Update ──────────────────────────────────────
        if pipeline_success:
            update_watermark(adls_client, end_date)
            elapsed_total = (datetime.utcnow() - pipeline_start).seconds
            logging.info("=" * 60)
            logging.info(f"🎉 Ingestion complete in {elapsed_total}s")
            return func.HttpResponse("✅ Ingestion complete", status_code=200)
        else:
            logging.warning("⚠️ Watermark NOT updated — one or more media IDs failed")
            return func.HttpResponse(
                "⚠️ Ingestion completed with errors — watermark not updated",
                status_code=500
            )

    except Exception as e:
        logging.error(f"❌ Fatal Error: {e}")
        return func.HttpResponse(f"❌ Fatal Error: {e}", status_code=500)


# ── Core Ingestion Logic ──────────────────────────────────────────
def run_ingestion_for_media(media_id, start_date, end_date, headers, adls_client, log):
    """
    Runs full ingestion for one media ID.
    - Metadata : one file, partitioned by actual created_at date
    - Stats    : one file per day, partitioned by actual data date
    - Events   : one file per day, partitioned by actual event date
    """

    # 1. Metadata
    log(f"  📄 Fetching metadata for {media_id}...")
    metadata = call_api(f"https://api.wistia.com/v1/medias/{media_id}.json", headers)
    if metadata:
        created_date = metadata.get("created", end_date)[:10]
        success      = save_to_adls(
            adls_client, metadata,
            f"metadata/media_id={media_id}/date={created_date}/metadata.json"
        )
        log(f"  {'✅' if success else '❌'} Metadata {'saved' if success else 'FAILED'} for {media_id} (date={created_date})")
    else:
        log(f"  ⚠️ Metadata: no data returned for {media_id}")

    # 2. Stats by date
    log(f"  📊 Fetching stats for {media_id} ({start_date} → {end_date})...")
    stats = call_api(
        f"https://api.wistia.com/v1/stats/medias/{media_id}/by_date.json",
        headers,
        params={"start_date": start_date, "end_date": end_date}
    )
    if stats:
        stats_saved  = 0
        stats_failed = 0
        for day_stat in stats:
            data_date = day_stat.get("date")
            success   = save_to_adls(
                adls_client, day_stat,
                f"stats_by_date/media_id={media_id}/date={data_date}/stats.json"
            )
            if success:
                stats_saved += 1
            else:
                stats_failed += 1
        log(f"  ✅ Stats: {stats_saved} days saved, {stats_failed} failed")
    else:
        log(f"  ⚠️ Stats: no data returned for {media_id}")

    # 3. Events
    log(f"  📋 Fetching events for {media_id}...")
    events = fetch_events(media_id, start_date, end_date, headers, log)
    if events:
        events_by_date = {}
        for event in events:
            event_date = event.get("received_at", "")[:10]
            if event_date not in events_by_date:
                events_by_date[event_date] = []
            events_by_date[event_date].append(event)

        events_saved  = 0
        events_failed = 0
        for event_date, day_events in events_by_date.items():
            success = save_to_adls(
                adls_client, day_events,
                f"events/media_id={media_id}/date={event_date}/events.json"
            )
            if success:
                events_saved += 1
            else:
                events_failed += 1
        log(f"  ✅ Events: {len(events)} total across {events_saved} date partitions, {events_failed} failed")
    else:
        log(f"  ⚠️ Events: no data returned for {media_id} (may be no views in this period)")


# ── API Call with Retry + Timeout ─────────────────────────────────
def call_api(url, headers, params=None):
    """
    Calls Wistia API with exponential backoff retry.
    - Timeout    : 30s per request
    - Retries on : 429 (rate limit), 500/502/503/504 (server errors), timeouts, connection errors
    - Fails hard : on 4xx client errors (except 429) — no point retrying bad requests
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 429:
                wait = RETRY_BACKOFF ** attempt
                logging.warning(f"⚠️ Rate limited (429) — retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)

            elif response.status_code in [500, 502, 503, 504]:
                wait = RETRY_BACKOFF ** attempt
                logging.warning(f"⚠️ Server error {response.status_code} — retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)

            else:
                logging.error(f"❌ API error {response.status_code}: {response.text}")
                return None  # don't retry on 4xx client errors

        except requests.exceptions.Timeout:
            wait = RETRY_BACKOFF ** attempt
            logging.warning(f"⚠️ Request timed out — retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)

        except requests.exceptions.ConnectionError as e:
            wait = RETRY_BACKOFF ** attempt
            logging.warning(f"⚠️ Connection error: {e} — retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)

        except Exception as e:
            logging.error(f"❌ Unexpected error calling {url}: {e}")
            return None

    logging.error(f"❌ All {MAX_RETRIES} retries exhausted for {url}")
    return None


# ── Pagination Helper ─────────────────────────────────────────────
def fetch_events(media_id, start_date, end_date, headers, log=logging.info):
    all_events = []
    page       = 1
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
            log(f"  ⚠️ No data on page {page} — stopping pagination")
            break
        all_events.extend(data)
        log(f"  📄 Page {page}: {len(data)} events (total: {len(all_events)})")
        if len(data) < 100:
            break
        page += 1
    return all_events


# ── ADLS Write with Retry ─────────────────────────────────────────
def save_to_adls(adls_client, data, path):
    """Writes JSON data to ADLS with exponential backoff retry."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            fs          = adls_client.get_file_system_client(CONTAINER)
            file_client = fs.get_file_client(path)
            file_client.upload_data(json.dumps(data, indent=2), overwrite=True)
            logging.info(f"✅ Saved: {path}")
            return True
        except Exception as e:
            wait = RETRY_BACKOFF ** attempt
            logging.warning(f"⚠️ ADLS write failed (attempt {attempt}/{MAX_RETRIES}): {e} — retrying in {wait}s")
            time.sleep(wait)
    logging.error(f"❌ ADLS write failed after {MAX_RETRIES} retries: {path}")
    return False


# ── Watermark Helpers ─────────────────────────────────────────────
def get_last_ingested_date(adls_client):
    try:
        fs          = adls_client.get_file_system_client(CONTAINER)
        file_client = fs.get_file_client(WATERMARK_BLOB)
        download    = file_client.download_file()
        last_date   = download.readall().decode("utf-8").strip()
        if last_date:
            next_date = (
                datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
            logging.info(f"📌 Watermark found: {last_date} → starting from {next_date}")
            return next_date
    except Exception as e:
        logging.info(f"📌 No watermark found ({e}) → using START_DATE: {START_DATE}")
    return START_DATE


def update_watermark(adls_client, date: str):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            fs          = adls_client.get_file_system_client(CONTAINER)
            file_client = fs.get_file_client(WATERMARK_BLOB)
            file_client.upload_data(date.encode("utf-8"), overwrite=True)
            logging.info(f"📌 Watermark updated to {date}")
            return
        except Exception as e:
            wait = RETRY_BACKOFF ** attempt
            logging.warning(f"⚠️ Watermark update failed (attempt {attempt}/{MAX_RETRIES}): {e} — retrying in {wait}s")
            time.sleep(wait)
    logging.error(f"❌ Watermark update failed after {MAX_RETRIES} retries")
