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
STORAGE_ACCOUNT = "wistiaadls"
CONTAINER       = "raw"
START_DATE      = "2024-12-04"
WATERMARK_BLOB  = "watermark/last_ingested.txt"

@app.timer_trigger(schedule="0 0 8 * * *",
                   arg_name="myTimer",
                   run_on_startup=False)
def wistia_ingestion(myTimer: func.TimerRequest) -> None:
    logging.info("🚀 Wistia ingestion pipeline started")

    try:
        credential    = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
        # API_TOKEN     = secret_client.get_secret("wistia-api-token").value
        API_TOKEN = "0323ade64e13f79821bdc0f2a9410d9ec3873aa9df01f8a4a54d4e0f3dd2e6b4"
        HEADERS       = {"Authorization": f"Bearer {API_TOKEN}"}

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
            # 1. Metadata
            metadata = call_api(f"https://api.wistia.com/v1/medias/{media_id}.json", HEADERS)
            if metadata:
                save_to_adls(adls_client, metadata, f"metadata/media_id={media_id}/date={end_date}/metadata.json")

            # 2. Stats
            stats = call_api(f"https://api.wistia.com/v1/stats/medias/{media_id}/by_date.json",
                             HEADERS, params={"start_date": start_date, "end_date": end_date})
            if stats:
                save_to_adls(adls_client, stats, f"stats_by_date/media_id={media_id}/date={end_date}/stats.json")

            # 3. Events (Pagination)
            events = fetch_events(media_id, start_date, end_date, HEADERS)
            if events:
                save_to_adls(adls_client, events, f"events/media_id={media_id}/date={end_date}/events.json")

        update_watermark(adls_client, end_date)
        logging.info("🎉 Ingestion complete!")

    except Exception as e:
        logging.error(f"❌ Execution Error: {e}")
        raise

# ── Helpers ───────────────────────────────────────────────────────

def call_api(url, headers, params=None):
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        logging.error(f"⚠️ API error {response.status_code}: {response.text}")
    except Exception as e:
        logging.error(f"❌ Request failed: {e}")
    return None

def fetch_events(media_id, start_date, end_date, headers):
    all_events = []
    page = 1
    while True:
        params = {"media_id": media_id, "start_date": start_date, "end_date": end_date, "per_page": 100, "page": page}
        data = call_api("https://api.wistia.com/v1/stats/events.json", headers, params)
        if not data:
            break
        all_events.extend(data)
        if len(data) < 100:
            break
        page += 1
    return all_events

def save_to_adls(adls_client, data, path):
    try:
        fs = adls_client.get_file_system_client(CONTAINER)
        file_client = fs.get_file_client(path)
        file_client.upload_data(json.dumps(data, indent=2), overwrite=True)
        logging.info(f"✅ Saved to ADLS: {path}")
    except Exception as e:
        logging.error(f"❌ ADLS Error saving {path}: {e}")

def get_last_ingested_date(adls_client):
    """Read watermark from ADLS so it persists across function restarts."""
    try:
        fs = adls_client.get_file_system_client(CONTAINER)
        file_client = fs.get_file_client(WATERMARK_BLOB)
        download = file_client.download_file()
        last_date = download.readall().decode("utf-8").strip()
        if last_date:
            next_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            logging.info(f"📌 Watermark found: {last_date}, starting from {next_date}")
            return next_date
    except Exception:
        logging.info(f"📌 No watermark found, using START_DATE: {START_DATE}")
    return START_DATE

def update_watermark(adls_client, date: str):
    """Persist watermark to ADLS."""
    try:
        fs = adls_client.get_file_system_client(CONTAINER)
        file_client = fs.get_file_client(WATERMARK_BLOB)
        file_client.upload_data(date.encode("utf-8"), overwrite=True)
        logging.info(f"📌 Watermark updated to {date}")
    except Exception as e:
        logging.error(f"❌ Failed to update watermark: {e}")



@app.route(route="test", auth_level=func.AuthLevel.ANONYMOUS)
def test_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🧪 Test trigger fired")
    try:
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
        API_TOKEN = secret_client.get_secret("wistia-api-token").value
        HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}

        adls_client = DataLakeServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
            credential=credential
        )

        start_date = get_last_ingested_date(adls_client)
        end_date = datetime.today().strftime("%Y-%m-%d")

        if start_date > end_date:
            return func.HttpResponse("✅ Already up to date", status_code=200)

        for media_id in MEDIA_IDs:
            metadata = call_api(f"https://api.wistia.com/v1/medias/{media_id}.json", HEADERS)
            if metadata:
                save_to_adls(adls_client, metadata, f"metadata/media_id={media_id}/date={end_date}/metadata.json")

            stats = call_api(f"https://api.wistia.com/v1/stats/medias/{media_id}/by_date.json",
                             HEADERS, params={"start_date": start_date, "end_date": end_date})
            if stats:
                save_to_adls(adls_client, stats, f"stats_by_date/media_id={media_id}/date={end_date}/stats.json")

            events = fetch_events(media_id, start_date, end_date, HEADERS)
            if events:
                save_to_adls(adls_client, events, f"events/media_id={media_id}/date={end_date}/events.json")

        update_watermark(adls_client, end_date)
        return func.HttpResponse("🎉 Ingestion complete!", status_code=200)

    except Exception as e:
        logging.error(f"❌ Error: {e}")
        return func.HttpResponse(f"❌ Error: {str(e)}", status_code=500)
```

Deploy via VS Code then hit this URL in your browser:
```
https://wistia-ingestion-lm2-a4chezbde9hkdxc7.canadacentral-01.azurewebsites.net/api/test