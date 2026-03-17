# Data Model

## Overview

The dimensional model consists of two dimension tables and two fact tables,
designed to support both daily aggregate analysis and visitor-level analysis.

The original project brief combined daily and visitor-level metrics into a
single fact table. This was separated into two tables to avoid granularity
clashes and enable richer analysis at both levels independently.
```
dim_media ←── fact_media_engagement_daily
    ↑
dim_visitor ←── fact_visitor_events
```

---

## dim_media

Source: `GET /v1/medias/{mediaId}.json`

| Column | Type | Source Field | Description |
|---|---|---|---|
| media_id | string | hashed_id | Unique Wistia media identifier |
| title | string | name | Internal Wistia video name |
| channel | string | derived from name | Facebook or YouTube |
| duration_secs | double | duration | Video length in seconds |
| created_at | timestamp | created | When video was uploaded |
| embed_url | string | embed_url from events | Page video is embedded on |

**Notes:**
- `channel` is extracted by checking if the internal video name contains
  "Facebook" or "YouTube". The marketing team uses this naming convention
  to identify which ad platform each video belongs to.
  e.g. `"Chris Face VSL The Gap Method Facebook Paid Ads"` → `Facebook`
- `embed_url` is derived from the events endpoint as the most frequently
  occurring embed URL for that media_id

---

## dim_visitor

Source: `GET /v1/stats/events.json`

| Column | Type | Source Field | Description |
|---|---|---|---|
| visitor_id | string | visitor_key | Unique visitor identifier |
| ip_address | string | ip | Visitor IP address |
| country | string | country | Visitor country |
| region | string | region | Visitor region/state |
| city | string | city | Visitor city |
| browser | string | user_agent_details.browser | Browser used |
| platform | string | user_agent_details.platform | OS platform |
| is_mobile | boolean | user_agent_details.mobile | Mobile device flag |

**Notes:**
- Deduplicated by `visitor_id` — one row per unique visitor
- A visitor may appear across multiple events and multiple days

---

## fact_media_engagement_daily

Sources:
- `GET /v1/stats/medias/{mediaId}/by_date.json` — load/play counts, hours watched
- `GET /v1/stats/events.json` — aggregated for visitor count and avg watch %

| Column | Type | Source | Description |
|---|---|---|---|
| media_id | string | API param | Foreign key to dim_media |
| date | date | date | Date of engagement |
| load_count_daily | long | load_count | Times video rendered on page |
| play_count_daily | long | play_count | Times video was played |
| visitor_count_daily | long | calculated | Distinct visitor_keys per day |
| play_rate | double | calculated | play_count / load_count |
| total_watch_time_hrs_daily | double | hours_watched | Total hours watched |
| avg_watched_percent_daily | double | calculated | avg(percent_viewed) across events |

**Calculated fields:**
```
play_rate = play_count_daily / load_count_daily
visitor_count_daily = COUNT(DISTINCT visitor_key) per media_id + date
avg_watched_percent_daily = AVG(percent_viewed) across events that day
```

**Notes:**
- Zero rows (load_count = 0) are kept intentionally — days with no activity
  are valid data points for play rate trend analysis
- `visitor_count_daily` may equal `play_count_daily` for videos with
  one play per visitor

---

## fact_visitor_events

Source: `GET /v1/stats/events.json`

| Column | Type | Source Field | Description |
|---|---|---|---|
| event_key | string | event_key | Unique event identifier |
| media_id | string | media_id | Foreign key to dim_media |
| visitor_id | string | visitor_key | Foreign key to dim_visitor |
| received_at | timestamp | received_at | Full event timestamp |
| date | date | derived | Date portion of received_at |
| watched_percent_visitor | double | percent_viewed | % of video watched |
| total_watch_time_hrs_visitor | double | calculated | Watch time in hours |
| url | string | embed_url | Page video was watched from |

**Calculated fields:**
```
total_watch_time_hrs_visitor = (percent_viewed * duration_secs) / 3600
date = received_at::date
```

**Notes:**
- One row per play event — a single visitor can have multiple events
- `date` column is added for easy joining to `fact_media_engagement_daily`
- Events with `percent_viewed = 0` are kept in gold layer for completeness

---

## Raw Layer Partitioning

Data lands in ADLS raw layer with Hive-style partitioning by actual data
date (not ingestion date):
```
raw/
├── metadata/media_id={id}/date={created_date}/metadata.json
├── stats_by_date/media_id={id}/date={data_date}/stats.json
├── events/media_id={id}/date={event_date}/events.json
└── watermark/last_ingested.txt
```

Each day's data is in its own partition file, enabling:
- Partition pruning in Databricks (faster reads)
- Easy reprocessing of a specific date
- Clear isolation between daily runs