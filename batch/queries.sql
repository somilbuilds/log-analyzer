-- ============================================================
-- Analytical Queries for Web Log Analysis
-- ============================================================
-- Run these in Hive (beeline) after creating the external table
-- and repairing partitions.
-- ============================================================

USE web_analytics;

-- ─── Query 1: Peak Hour by Request Count ───────────────────
-- Finds the hour with the highest number of requests
SELECT
    date,
    hour_val,
    COUNT(*) AS request_count
FROM web_logs
GROUP BY date, hour_val
ORDER BY request_count DESC
LIMIT 10;


-- ─── Query 2: Error Rate by Hour ───────────────────────────
-- Computes the percentage of 4xx/5xx responses per hour
SELECT
    date,
    hour_val,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) AS error_count,
    ROUND(
        SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS error_rate_pct
FROM web_logs
GROUP BY date, hour_val
ORDER BY date, hour_val;


-- ─── Query 3: Top 10 Hosts by Request Count ────────────────
-- Identifies the most active clients
SELECT
    host,
    COUNT(*) AS request_count
FROM web_logs
GROUP BY host
ORDER BY request_count DESC
LIMIT 10;


-- ─── Query 4: Top 10 Most Requested Paths ──────────────────
-- Shows the most popular URLs
SELECT
    path,
    COUNT(*) AS request_count
FROM web_logs
GROUP BY path
ORDER BY request_count DESC
LIMIT 10;


-- ─── Query 5: Status Code Distribution ─────────────────────
-- Overall breakdown of HTTP status codes
SELECT
    status,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM web_logs), 2) AS pct
FROM web_logs
GROUP BY status
ORDER BY count DESC;
