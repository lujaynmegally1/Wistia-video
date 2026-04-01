# Wistia Video Analytics — End-to-End Azure Data Pipeline

**[Watch Video Project Walkthrough](https://youtu.be/njy4wowDJUc)**


An end-to-end data engineering pipeline that ingests video engagement analytics
from the Wistia Stats API, transforms the data through a medallion architecture,
and serves it via Azure Synapse Analytics.

Built entirely on Azure using Python, PySpark, and GitHub Actions CI/CD.

---
## Edited Architecture (after feedback from SMEs)

![Architecture Diagram](docs/images/architecture-diagram.png)

## Architecture (project docs currently reflect this architecture) 

![Architecture Diagram](docs/images/architecture-diagram-2.png)

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


→ Full Component Breakdown & Design Rationale: [`docs/architecture.md`](docs/architecture.md)

---

## Data Model

| Table | Description |
|---|---|
| `dim_media` | One row per video — title, channel, duration, embed URL |
| `dim_visitor` | One row per unique visitor — location, browser, device |
| `fact_media_engagement_daily` | Daily aggregates per video — plays, loads, watch time, play rate |
| `fact_visitor_events` | One row per play event — visitor, timestamp, % watched |

→ Full data model with source mappings: [`docs/data-model.md`](docs/data-model.md)

---

## Quick Start

→ Full documentation of my project's process, issues encountered, decisions, etc.: [`docs/project-log.md`](docs/project-log.md)

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
Add to Azure Key Vault `wistia-keyvault-lm`:
- `wistia-api-token` → your Wistia API token

### 4. Deploy Azure Function
```bash
func azure functionapp publish wistia-ingestion-lm2 --python
```

### 5. Trigger pipeline manually
```
GET /api/test
```

### 6. Run Databricks transformation
Databricks → Workspace → Wistia → run `wistia-video-gold`

### 7. Set up Synapse views
Connect to Synapse serverless endpoint and run the setup script:
- Server: `wistia-synapse-lm-ondemand.sql.azuresynapse.net`
- Script: [`docs/synapse-setup.sql`](docs/synapse-setup.sql)

Note: Run `CREATE DATABASE` connected to `master`, then switch 
to `wistia_gold` for all view creation statements.

---

## CI/CD

| Component | Method |
|---|---|
| Azure Functions | `func azure functionapp publish` via terminal. (Commit to GituHub for version control) |
| Databricks Notebooks | GitHub Actions → Databricks CLI on push to `main` |

Required GitHub Secrets:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

---

## Docs

- [Architecture & Design Rationale](docs/architecture.md)
- [Data Model](docs/data-model.md)
- [Project Log](docs/project-log.md) 
