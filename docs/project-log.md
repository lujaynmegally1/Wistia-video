# Project Log

A running log of decisions made, issues encountered, and how they were resolved.

---

## Step 1 — API Exploration & Authentication

### Endpoint Mapping
Mapped Wistia API endpoints to the data model:
- `GET /v1/medias/{mediaId}.json` → dim_media
- `GET /v1/stats/medias/{mediaId}/by_date.json` → fact_media_engagement_daily
- `GET /v1/stats/events.json` → dim_visitor + fact_visitor_events

### Data Model Decision
The original project brief had a single `fact_media_engagement` table mixing
daily aggregates with visitor-level metrics. This creates a granularity clash —
you can't store both daily totals and per-visitor percentages in the same row.

**Decision:** Split into two fact tables:
- `fact_media_engagement_daily` — daily aggregates (from `/by_date` endpoint)
- `fact_visitor_events` — one row per play event (from `/events` endpoint)

This enables independent analysis at both levels and avoids null-heavy rows.

### Channel Extraction
The Wistia API has no explicit "channel" field. The marketing team uses the
internal video name to identify the ad platform:
- `"Chris Face VSL The Gap Method Facebook Paid Ads"` → Facebook
- `"Chris Face VSL The Gap Method Youtube Paid Ads"` → YouTube

**Decision:** Parse channel from title using string matching on "Facebook"
and "YouTube".

---

## Step 2 — Azure Setup & Ingestion

### Azure Resources Created
- Resource Group: `Wistia-analytics`
- Key Vault: `wistia-keyvault-lm` (stores Wistia API token)
- ADLS Gen2: `wistiaadls` (raw, silver, gold containers)
- Azure Functions: `wistia-ingestion-lm2`

### CI/CD Issue — Azure Functions GitHub Actions Deployment
**Problem:** GitHub Actions deployment kept setting `WEBSITE_RUN_FROM_PACKAGE=1`
automatically, causing the function app to run in read-only mode. The Python
worker crashed silently on startup with 0 functions found in the portal.

Things tried:
- Toggling `ENABLE_ORYX_BUILD`, `SCM_DO_BUILD_DURING_DEPLOYMENT`
- Bundling deps via `--target=.python_packages/lib/site-packages`
- Setting `PYTHONPATH` and `AzureWebJobsFeatureFlags=EnableWorkerIndexing`
- Creating a new Function App entirely

**Resolution:** Switched to VS Code Azure Functions extension for deployment,
and use GitHub purely for version control. This is a valid production pattern for this project - 
CI/CD for the function code is tracked in GitHub, and CI/CD for the Databricks notebook uses
full GitHub Actions automation to deploy when a change is made. 

### Watermark Design
**Decision:** Store watermark as a plain text file in ADLS
(`raw/watermark/last_ingested.txt`) rather than a database table.
- No extra Azure service needed
- Survives function app restarts
- Easy to inspect and manually reset if needed
- Watermark only advances if ALL media IDs succeed

### ADLS Partitioning Decision
**Problem:** Original code partitioned by ingestion date (run date), not
data date. This meant all historical data (for testing) landed in one folder named today.

**Decision:** Partition by actual data date extracted from API response fields:
- Metadata → `date=` from `created` field
- Stats → `date=` from `date` field in response
- Events → `date=` from `received_at` field

This enables partition pruning and makes each partition meaningful.

### Error Handling Additions
Added three layers of resilience to the Azure Function:
1. API-level retries with exponential backoff (handles 429, 5xx, timeouts)
2. ADLS write retries (3 attempts)
3. Watermark update retries (3 attempts)
4. Pipeline continues to next media ID on failure
5. Watermark only advances on full success

---

## Step 3 — Databricks & Transformation

### Unity Catalog Issue
**Problem:** `input_file_name()` function blocked by Unity Catalog security policy.

**Resolution:** Replaced with `_metadata.file_path` which is the Unity Catalog
approved equivalent for extracting file path metadata.

### Silver vs Gold Decision
**Decision:** Skipped a separate silver container and wrote transformation
output directly to gold. For this project's data volume and complexity,
the silver layer added no practical value — the transformation from raw JSON
to typed dimensional tables is a single clean step.

---

## Step 4 — Synapse & Serving Layer

### Synapse Deployment Issue
**Problem:** Synapse workspace deployment failed with
`CustomerSubscriptionNotRegisteredWithSqlRp`.

**Resolution:** Registered `Microsoft.Sql` and `Microsoft.Synapse` resource
providers via Azure CLI:
```bash
az provider register --namespace Microsoft.Sql
az provider register --namespace Microsoft.Synapse
```

### Serverless vs Dedicated Pool
**Decision:** Used Synapse Serverless SQL pool instead of dedicated.
Data volume (tens of thousands of rows) does not justify a dedicated pool.
Serverless reads Parquet directly from ADLS via `OPENROWSET` with no
data movement, and costs nothing when not queried.

### Views in Wrong Database
**Problem:** `CREATE OR ALTER VIEW` ran against `master` database instead
of `wistia_gold` because the Connect To dropdown wasn't switched.

**Resolution:** Each view must be created as a separate SQL script with
the Connect To dropdown explicitly set to `wistia_gold` before running.

---

## Step 5 — Orchestration & CI/CD

### ADF vs Timer Trigger 
During development, an Azure Function timer trigger was used to run 
ingestion daily at 8am UTC. This allowed the pipeline to run in 
production for 7 consecutive days while the rest of the stack 
(Databricks, Synapse, ADF) was being built out in parallel. The timer 
trigger proved the incremental ingestion logic worked correctly — 
watermark advancing daily, both media IDs processing, data landing in 
ADLS partitions as expected.

Once ADF was set up and the full end-to-end pipeline (ingestion → 
transformation → serving) was complete, the timer trigger was replaced 
with an ADF schedule trigger for production orchestration.

**Decision:** Replace Azure Function timer trigger with ADF schedule trigger.

Reasons:
- ADF provides visual monitoring of every pipeline run with per-activity 
  status, duration, and error details — the timer trigger only exposed 
  logs via Azure Function invocations tab
- Built-in retry policies at activity level (2 retries per activity) 
  without writing retry logic into the function itself
- Activity-level timeouts configurable independently — ingestion capped 
  at 10 minutes, Databricks notebook at 1 hour
- Single place to see the full pipeline — ingestion + transformation 
  visible and manageable in one ADF pipeline view
- ADF schedule trigger supports tumbling window configuration with a 
  defined start/end date — better suited for the 7-day production 
  requirement than a simple cron expression

The timer trigger code is kept in `function_app.py` as a commented-out 
reference, documenting the development approach and providing a fallback 
option if ADF is ever unavailable.
### Databricks CI/CD
Notebooks exported as `.py` source files and stored in `notebooks/` folder.
GitHub Actions workflow uses Databricks CLI to deploy on push to `main`:
```yaml
databricks workspace import_dir notebooks /Workspace/Wistia --overwrite
```

