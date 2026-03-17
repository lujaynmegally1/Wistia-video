-- ─────────────────────────────────────────────────────────────
-- Wistia Analytics — Synapse Serverless SQL Setup
-- Database: wistia_gold
-- Run each statement separately connected to wistia_gold
-- ─────────────────────────────────────────────────────────────


-- Step 1: Create database (run connected to master)
-- ─────────────────────────────────────────────────────────────
CREATE DATABASE wistia_gold


-- Step 2: Switch connection to wistia_gold, then run each view
-- ─────────────────────────────────────────────────────────────

CREATE OR ALTER VIEW dim_media AS
SELECT *
FROM OPENROWSET(
    BULK 'https://wistiaadls.dfs.core.windows.net/gold/dim_media/**',
    FORMAT = 'PARQUET'
) AS dm;


CREATE OR ALTER VIEW dim_visitor AS
SELECT *
FROM OPENROWSET(
    BULK 'https://wistiaadls.dfs.core.windows.net/gold/dim_visitor/**',
    FORMAT = 'PARQUET'
) AS dv;


CREATE OR ALTER VIEW fact_visitor_events AS
SELECT *
FROM OPENROWSET(
    BULK 'https://wistiaadls.dfs.core.windows.net/gold/fact_visitor_events/**',
    FORMAT = 'PARQUET'
) AS fve;


CREATE OR ALTER VIEW fact_media_engagement_daily AS
SELECT *
FROM OPENROWSET(
    BULK 'https://wistiaadls.dfs.core.windows.net/gold/fact_media_engagement_daily/**',
    FORMAT = 'PARQUET'
) AS fmed;


-- ─────────────────────────────────────────────────────────────
-- Verification Queries
-- ─────────────────────────────────────────────────────────────

-- Check all views created
SELECT * FROM INFORMATION_SCHEMA.VIEWS;

-- Preview each table
SELECT TOP 10 * FROM dbo.dim_media;
SELECT TOP 10 * FROM dbo.dim_visitor;
SELECT TOP 10 * FROM dbo.fact_media_engagement_daily;
SELECT TOP 10 * FROM dbo.fact_visitor_events;

-- Summary by channel
SELECT
    m.channel,
    COUNT(DISTINCT f.date)          AS active_days,
    SUM(f.play_count_daily)         AS total_plays,
    SUM(f.load_count_daily)         AS total_loads,
    ROUND(AVG(f.play_rate), 4)      AS avg_play_rate,
    ROUND(SUM(f.total_watch_time_hrs_daily), 2) AS total_watch_hrs
FROM fact_media_engagement_daily f
JOIN dim_media m ON f.media_id = m.media_id
GROUP BY m.channel;
