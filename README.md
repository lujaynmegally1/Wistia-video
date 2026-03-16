# Wistia Video Analytics — End-to-End Data Engineering Pipeline

## Overview
An end-to-end data engineering pipeline that ingests video engagement analytics 
from the Wistia Stats API, transforms the data through a medallion architecture, 
and serves it via Azure Synapse Analytics for reporting.

Built on Azure using Python, PySpark, and GitHub Actions CI/CD.

---

## Architecture
```
Wistia Stats API
      ↓
Azure Functions (Python) — HTTP trigger orchestrated by ADF
      ↓
ADLS Gen2 — raw layer (Hive-style partitioning: media_id= / date=)
      ↓
Databricks (PySpark) — transformation notebook
      ↓
ADLS Gen2 — gold layer (Parquet)
      ↓
Azure Synapse Analytics — serverless SQL views
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Azure Functions (Python) |
| Orchestration | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 |
| Transformation | Databricks (PySpark) |
| Serving | Azure Synapse Analytics (Serverless SQL) |
| Secrets | Azure Key Vault |
| CI/CD | GitHub Actions |
| Version Control | GitHub |

---

## Data Model

### dim_media
| Column | Source | Description |
|---|---|---|
| media_id | hashed_id | Unique Wistia media identifier |
| title | name | Internal Wistia video name |
| channel | derived from name | Facebook or YouTube |
| duration_secs | duration | Video length in seconds |
| created_at | created | When video was uploaded to Wistia |
| embed_url | embed_url | Page the video is embedded on |

### dim_visitor
| Column | Source | Description |
|---|---|---|
| visitor_id | visitor_key | Unique visitor identifier |
| ip_address | ip | Visitor IP address |
| country | country | Visitor country |
| region | region | Visitor region/state |
| city | city | Visitor city |
| browser | user_agent_details.browser | Browser used |
| platform | user_agent_details.platform | OS platform |
| is_mobile | user_agent_details.mobile | Mobile device flag |

### fact_media_engagement_daily
| Column | Source | Description |
|---|---|---|
| media_id | API param | Foreign key to dim_media |
| date | date | Date of engagement |
| load_count_daily | load_count | Times video was rendered on page |
| play_count_daily | play_count | Times video was played |
| visitor_count_daily | calculated | Distinct visitors per day from events |
| play_rate | calculated | play_count / load_count |
| total_watch_time_hrs_daily | hours_watched | Total hours watched |
| avg_watched_percent_daily | calculated | Avg % of video watched across events |

### fact_visitor_events
| Column | Source | Description |
|---|---|---|
| event_key | event_key | Unique event identifier |
| media_id | media_id | Foreign key to dim_media |
| visitor_id | visitor_key | Foreign key to dim_visitor |
| received_at | received_at | Full event timestamp |
| date | derived | Date portion of received_at |
| watched_percent_visitor | percent_viewed | % of video watched this event |
| total_watch_time_hrs_visitor | calculated | (percent_viewed * duration_secs) / 3600 |
| url | embed_url | Page the video was watched from |

---

## ADLS Folder Structure
```
wistiaadls/
├── raw/
│   ├── metadata/
│   │   └── media_id={id}/date={created_date}/metadata.json
│   ├── stats_by_date/
│   │   └── media_id={id}/date={data_date}/stats.json
│   ├── events/
│   │   └── media_id={id}/date={event_date}/events.json
│   └── watermark/
│       └── last_ingested.txt
└── gold/
    ├── dim_media/
    ├── dim_visitor/
    ├── fact_visitor_events/
    └── fact_media_engagement_daily/
```

---

## Incremental Ingestion & Watermark

The pipeline uses a watermark file stored in ADLS (`raw/watermark/last_ingested.txt`)
to track the last successfully ingested date. On each run:

1. Read watermark → set `start_date = last_ingested + 1 day`
2. Set `end_date = today`
3. If `start_date > end_date` → skip (already up to date)
4. Ingest data for window `[start_date, end_date]`
5. Only update watermark if ALL media IDs ingested successfully

---

## Error Handling & Retries

The pipeline implements three layers of resilience:

**1. Function-level retries (Azure Functions)**
- API calls retry up to 3 times with exponential backoff (2s, 4s, 8s)
- Handles: 429 rate limits, 5xx server errors, timeouts, connection errors
- ADLS writes retry up to 3 times with exponential backoff
- Watermark only advances on full pipeline success

**2. Activity-level retries (Azure Data Factory)**
- Each ADF activity retries 2 times on failure
- Web activity timeout: 10 minutes
- Databricks notebook timeout: 1 hour

**3. Partial failure handling**
- If one media ID fails, pipeline continues to next
- Failed media IDs are logged with full error details
- Watermark not advanced if any media ID fails

---

## CI/CD

### Azure Functions
Deployed via Azure Functions Core Tools from terminal:
```bash
func azure functionapp publish wistia-ingestion-lm2 --python
```

### Databricks Notebooks
Automatically deployed via GitHub Actions on every push to `main`
when files in `notebooks/` are changed.

Workflow: `.github/workflows/deploy_notebooks.yml`

Required GitHub Secrets:
| Secret | Description |
|---|---|
| `DATABRICKS_HOST` | Databricks workspace URL |
| `DATABRICKS_TOKEN` | Databricks personal access token |

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

---

## Repository Structure
```
Wistia-video/
├── .github/
│   └── workflows/
│       └── deploy_notebooks.yml   # CI/CD for Databricks notebooks
├── notebooks/
│   └── wistia-video-gold.py       # PySpark transformation (raw → gold)
├── .funcignore                    # Azure Functions deployment exclusions
├── .gitignore                     # Git exclusions
├── function_app.py                # Azure Functions ingestion code
├── host.json                      # Azure Functions runtime config
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

---

## Setup Instructions

### Prerequisites
- Azure subscription
- Python 3.11+
- Azure Functions Core Tools v4
- Databricks CLI

### 1. Clone the repo
```bash
git clone https://github.com/lujaynmegally1/Wistia-video.git
cd Wistia-video
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Key Vault
Add the following secret to Azure Key Vault `wistia-keyvault-lm`:
- `wistia-api-token` — your Wistia API token

### 4. Deploy Azure Function
```bash
func azure functionapp publish wistia-ingestion-lm2 --python
```

### 5. Trigger pipeline manually
```
GET https://wistia-ingestion-lm2-a4chezbde9hkdxc7.canadacentral-01.azurewebsites.net/api/test
```

### 6. Run Databricks transformation
Open Databricks workspace → Workspace → Wistia → run `wistia-video-gold`

### 7. Query results in Synapse
Connect to:
```
wistia-synapse-lm-ondemand.sql.azuresynapse.net
```
Database: `wistia_gold`

---

## Media IDs

| Media ID | Channel | Description |
|---|---|---|
| gskhw4w4lm | YouTube | Chris Face VSL — YouTube Paid Ads |
| v08dlrgr7v | Facebook | Chris Face VSL — Facebook Paid Ads |

---

## Pipeline Schedule

The pipeline runs daily at **8:00 AM UTC** via ADF trigger `daily_8am_trigger`.

Each run:
1. ADF triggers Azure Function → ingests new data incrementally from Wistia API
2. ADF triggers Databricks notebook → transforms raw JSON to gold Parquet
3. Synapse serverless views automatically reflect updated gold data

---

## Design Decisions

**Why Azure Functions over ADF for ingestion?**
Azure Functions gives full control over pagination, watermark logic, and 
error handling in Python. ADF orchestrates the trigger but the ingestion 
logic lives in the function.

**Why Hive-style partitioning in ADLS?**
`media_id=x/date=y` partitioning allows Databricks and Synapse to use 
partition pruning — only reading relevant partitions rather than full scans.

**Why no DBT?**
Project requirements specified no DBT. PySpark in Databricks handles 
all transformation logic directly.

**Why Synapse Serverless over Dedicated Pool?**
Data volume doesn't justify a dedicated pool. Serverless SQL reads 
Parquet directly from ADLS with no data movement or loading required.