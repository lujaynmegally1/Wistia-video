# Architecture & Design Rationale

## Pipeline Overview

![Architecture Diagram](docs/images/architecture-diagram.png)


## Component Breakdown

### Source — Wistia Stats API
Two videos tracked across Facebook and YouTube paid ads:
- `gskhw4w4lm` — YouTube
- `v08dlrgr7v` — Facebook

Endpoints used:
- `GET /v1/medias/{mediaId}.json` — media metadata
- `GET /v1/stats/medias/{mediaId}/by_date.json` — daily aggregates
- `GET /v1/stats/events.json` — visitor-level events (paginated)

---

### Ingestion — Azure Functions (Python)

The Azure Function handles all ingestion logic:

**Pagination**
The function loops through API pages of 100 events until an empty
response is returned, ensuring no records are missed.

**Incremental ingestion**
A watermark file (`raw/watermark/last_ingested.txt`) is stored in ADLS.
On each run the function reads the watermark, sets
`start_date = last_ingested + 1 day`, and only fetches records in that
window. The watermark is only updated if ALL media IDs ingest successfully,
preventing partial advances.

**Secret management**
The Wistia API token is stored in Azure Key Vault and retrieved via
Managed Identity at runtime. No credentials appear in source code or
config files. If Key Vault is unavailable the function fails loudly
(returns 500) rather than falling back to a hardcoded value.

**Error handling & retries**
- API calls retry up to 3 times with exponential backoff (2s, 4s, 8s)
- Handles: 429 rate limits, 5xx server errors, timeouts, connection errors
- ADLS writes retry up to 3 times
- Pipeline continues to next media ID if one fails
- Watermark not advanced on partial failure

**Why Azure Functions over alternatives?**
- Serverless — no infrastructure to manage, spins up on trigger
- Native Python support matches project requirement
- Directly invokable by ADF as a web activity
- Consumption tier means no cost when not running

---

### Orchestration — Azure Data Factory

ADF runs the full pipeline daily at 8am UTC via a schedule trigger.

Pipeline activities:
1. **Web Activity** → calls Azure Function `/api/test` (ingestion)
2. **Databricks Notebook Activity** → runs transformation (depends on step 1)

Each activity has 2 retries configured. Synapse does not need an ADF
activity since its views are virtual — they read directly from ADLS on
every query.

**Why ADF over Azure Logic Apps?**
Logic Apps is designed for event-driven workflow automation, not data
pipeline orchestration. It lacks native Databricks and Synapse integration,
has no data-focused monitoring, and becomes difficult to manage as pipeline
complexity grows. ADF keeps the entire pipeline — ingestion, transformation,
and loading — visible and manageable in one place.

**Why ADF over keeping the Azure Function timer trigger?**
ADF provides visual monitoring, built-in retry policies, activity-level
timeouts, and a single view of the entire pipeline. The Azure Function
timer trigger was used during development and testing but replaced by ADF
for production orchestration.

---

### Storage — ADLS Gen2

Raw JSON and gold Parquet both land in the same ADLS Gen2 account
(`wistiaadls`), separated by container:
```
wistiaadls/
├── raw/      ← JSON from Azure Function
└── gold/     ← Parquet from Databricks
```

**Hive-style partitioning in raw layer**
```
raw/events/media_id=gskhw4w4lm/date=2026-03-10/events.json
```
Each day's data lands in its own partition. This keeps daily runs
isolated, makes it easy to reprocess a specific date, and allows
Databricks to use partition pruning for faster reads.

**Why ADLS Gen2 over Azure Blob Storage?**

| Feature | ADLS Gen2 | Blob Storage |
|---|---|---|
| Directory operations | Atomic (rename/delete instant) | Copy + delete every file |
| Directory listing | Single efficient call | Scans flat key namespace |
| Spark driver | `abfss://` — optimised for big data | `wasbs://` — slower |
| Databricks support | Native | Works but not optimised |

---

### Transformation — Databricks (PySpark)

A single PySpark notebook (`wistia-video-gold.py`) reads raw JSON from
ADLS, builds the dimensional model, and writes gold Parquet back to ADLS.

Transformations applied:
- Cast timestamps and dates to correct types
- Extract `channel` from video title string (Facebook / YouTube)
- Extract `media_id` from Hive partition path using `_metadata.file_path`
- Join stats with event aggregates for `fact_media_engagement_daily`
- Calculate derived fields: `play_rate`, `total_watch_time_hrs_visitor`,
  `avg_watched_percent_daily`
- Deduplicate `dim_visitor` by `visitor_id`

**Why Databricks over Synapse Spark Pools?**
- More mature, stable PySpark environment
- Better library support and notebook tooling
- Superior job scheduling and cluster management
- ADLS Gen2 integration is more reliable on Databricks
- Native CI/CD via Databricks CLI

---

### Serving — Azure Synapse Analytics (Serverless SQL)

Synapse serverless SQL views sit on top of the gold Parquet files in ADLS.
When Databricks writes updated Parquet, the views automatically reflect
the new data on the next query — no refresh or loading step needed.
```sql
CREATE OR ALTER VIEW fact_media_engagement_daily AS
SELECT * FROM OPENROWSET(
    BULK 'https://wistiaadls.dfs.core.windows.net/gold/fact_media_engagement_daily/**',
    FORMAT = 'PARQUET'
) AS fmed;
```

**Why Synapse Serverless over Dedicated Pool?**
Data volume doesn't justify a dedicated pool. Serverless SQL reads
Parquet directly from ADLS with no data movement or loading required,
and costs nothing when not queried.

**Why not connect Power BI directly to ADLS or Databricks?**

Connecting directly to ADLS:
- Power BI is not designed to query raw Parquet files
- No query engine — Power BI scans files itself on every refresh
- No table-level access control

Connecting directly to Databricks:
- Every dashboard refresh spins up a Databricks cluster
- Cluster startup latency on every report load
- Databricks is optimised for transformation, not repeated BI queries

Synapse provides a stable, low-latency SQL layer purpose-built for BI tools.

---

### CI/CD

**Azure Functions**
Deployed manually via Azure Functions Core Tools:
```bash
func azure functionapp publish wistia-ingestion-lm2 --python
```
Code is committed to GitHub for version control. A GitHub Actions
workflow for automatic deployment was attempted but encountered issues
with `WEBSITE_RUN_FROM_PACKAGE` causing read-only mode. VS Code
deployment was used as the reliable alternative.

**Databricks Notebooks**
`.py` notebook files are stored in `notebooks/` in GitHub.
A GitHub Actions workflow uses the Databricks CLI to automatically
sync and deploy updated notebooks to `/Workspace/Wistia` on every
push to `main`.

---

## Azure Resources

| Resource | Name | Purpose |
|---|---|---|
| Resource Group | Wistia-analytics | Container for all resources |
| Storage Account (ADLS Gen2) | wistiaadls | Raw and gold data lake |
| Azure Functions | wistia-ingestion-lm2 | Wistia API ingestion |
| Key Vault | wistia-keyvault-lm | Secure secret management |
| Databricks | wistia-databricks | PySpark transformation |
| Synapse Analytics | wistia-synapse-lm | Serverless SQL serving layer |
| Data Factory | wistia-adf | Pipeline orchestration |