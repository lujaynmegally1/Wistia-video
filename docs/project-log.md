# Project Log

A running log of decisions made, issues encountered, and how they were resolved.

---

## Week 1 — API Exploration & Authentication

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

## Week 2 — Azure Setup & Ingestion

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
and use GitHub purely for version control. This is a valid production pattern —
CI/CD for the function code is tracked in git, Databricks notebooks use
full GitHub Actions automation.

### Watermark Design
**Decision:** Store watermark as a plain text file in ADLS
(`raw/watermark/last_ingested.txt`) rather than a database table.
- No extra Azure service needed
- Survives function app restarts
- Easy to inspect and manually reset if needed
- Watermark only advances if ALL media IDs succeed

### ADLS Partitioning Decision
**Problem:** Original code partitioned by ingestion date (run date), not
data date. This meant all historical data landed in one folder named today.

**Decision:** Partition by actual data date extracted from API response fields:
- Metadata → `date=` from `created` field
- Stats → `date=` from `date` field in response
- Events → `date=` from `received_at` field

This enables partition pruning and makes each partition meaningful.

---

## Week 3 — Databricks & Transformation

### Unity Catalog Issue
**Problem:** `input_file_name()` function blocked by Unity Catalog security policy.

**Resolution:** Replaced with `_metadata.file_path` which is the Unity Catalog
approved equivalent for extracting file path metadata.

### Silver vs Gold Decision
**Decision:** Skipped a separate silver container and wrote transformation
output directly to gold. For this project's data volume and complexity,
the silver layer added no practical value — the transformation from raw JSON
to typed dimensional tables is a single clean step.

### stats_by_date Partitioning Fix
**Problem:** Stats API returns an array of daily objects. Original code saved
the entire array as one file under today's date.

**Resolution:** Loop through the array and save each day as its own partition
file. This keeps partitioning consistent with the events data.

---

## Week 4 — Synapse & Serving Layer

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

## Week 5 — Orchestration & CI/CD

### ADF vs Timer Trigger
**Decision:** Replaced Azure Function timer trigger with ADF schedule trigger.

Reasons:
- ADF provides visual monitoring of every pipeline run
- Built-in retry policies at activity level (2 retries per activity)
- Activity-level timeouts configurable independently
- Single place to see ingestion + transformation pipeline
- Timer trigger kept in code as commented-out reference

### Databricks CI/CD
Notebooks exported as `.py` source files and stored in `notebooks/` folder.
GitHub Actions workflow uses Databricks CLI to deploy on push to `main`:
```yaml
databricks workspace import_dir notebooks /Workspace/Wistia --overwrite
```

### Error Handling Additions
Added three layers of resilience to the Azure Function:
1. API-level retries with exponential backoff (handles 429, 5xx, timeouts)
2. ADLS write retries (3 attempts)
3. Watermark update retries (3 attempts)
4. Pipeline continues to next media ID on failure
5. Watermark only advances on full success

### API Token Security
**Decision:** Removed hardcoded API token fallback from production code.
If Key Vault is unavailable the function returns 500 immediately.
Rationale: a silent fallback to a hardcoded token masks a real
infrastructure problem and is a security risk.

---

## Media IDs

| Media ID | Channel | Active Period |
|---|---|---|
| gskhw4w4lm | YouTube | Jan 2025 — present |
| v08dlrgr7v | Facebook | Dec 2024 (limited activity) |

**Note:** v08dlrgr7v had very limited activity after Dec 2024.
The pipeline correctly ingests zero-event days for this media ID —
the absence of events is valid data showing the video is no longer
being actively promoted on Facebook.