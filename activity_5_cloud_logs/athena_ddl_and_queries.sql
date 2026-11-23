-- ==============================================================================
-- AWS ATHENA SERVERLESS LOG LAKE DDL & ANALYTICS QUERIES
-- CLOUD-BASED BIG DATA ANALYTICS (ACTIVITY 5)
-- ==============================================================================

-- 1. CREATE LOG LAKE DATABASE
CREATE DATABASE IF NOT EXISTS cloud_log_lake
COMMENT 'Enterprise Serverless Log Lake'
LOCATION 's3://cloud-ops-telemetry-bucket-prod/database/';

-- 2. CREATE EXTERNAL TABLE OVER S3 LOGS (REGEX SERDE FOR APACHE/COMBINED LOG FORMAT)
CREATE EXTERNAL TABLE IF NOT EXISTS cloud_log_lake.s3_web_server_logs_raw (
    client_ip       STRING,
    ident           STRING,
    auth_user       STRING,
    request_time    STRING,
    http_method     STRING,
    uri_path        STRING,
    http_version    STRING,
    status_code     INT,
    bytes_sent      BIGINT,
    referrer        STRING,
    user_agent      STRING,
    latency_ms      INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] "(\\S+) (\\S+) (\\S+)" (\\d{3}) (\\d+|-) "([^"]*)" "([^"]*)" (\\d+)$'
)
LOCATION 's3://cloud-ops-telemetry-bucket-prod/logs/raw_web_server/';

-- 3. CREATE OPTIMIZED PARQUET TABLE PARTITIONED BY YEAR, MONTH, DAY
CREATE EXTERNAL TABLE IF NOT EXISTS cloud_log_lake.web_server_logs_parquet (
    request_id      STRING,
    timestamp_utc   TIMESTAMP,
    client_ip       STRING,
    http_method     STRING,
    uri_path        STRING,
    status_code     INT,
    status_category STRING,
    bytes_sent      BIGINT,
    latency_ms      INT,
    is_error        INT,
    user_agent      STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION 's3://cloud-ops-telemetry-bucket-prod/logs/curated_parquet/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Load new partitions automatically discovered by AWS Glue Crawler:
MSCK REPAIR TABLE cloud_log_lake.web_server_logs_parquet;

-- 4. ANALYTICAL QUERIES
-- Query A: System Health & Status Code Distribution
SELECT 
    status_category, 
    status_code, 
    COUNT(*) AS total_requests,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS traffic_percentage,
    ROUND(AVG(latency_ms), 1) AS avg_latency_ms
FROM cloud_log_lake.web_server_logs_parquet
WHERE year = '2024' AND month = '05'
GROUP BY status_category, status_code
ORDER BY status_category, total_requests DESC;

-- Query B: Outage Detection (Hourly Spike in 5xx Gateway & Server Errors)
SELECT 
    DATE_TRUNC('hour', timestamp_utc) AS log_hour,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS server_errors,
    ROUND(SUM(CASE WHEN status_code >= 500 THEN 1.0 ELSE 0.0 END) * 100.0 / COUNT(*), 2) AS server_error_rate_pct,
    ROUND(AVG(latency_ms), 1) AS avg_latency_ms
FROM cloud_log_lake.web_server_logs_parquet
WHERE year = '2024' AND month = '05' AND day = '20'
GROUP BY DATE_TRUNC('hour', timestamp_utc)
ORDER BY log_hour ASC;

-- Query C: Problematic API Routes & Degraded Latency (P95 / P99)
SELECT 
    uri_path,
    http_method,
    COUNT(*) AS request_volume,
    SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS count_5xx_errors,
    ROUND(AVG(latency_ms), 1) AS avg_latency_ms,
    ROUND(APPROX_PERCENTILE(latency_ms, 0.95), 1) AS p95_latency_ms,
    ROUND(APPROX_PERCENTILE(latency_ms, 0.99), 1) AS p99_latency_ms
FROM cloud_log_lake.web_server_logs_parquet
WHERE year = '2024' AND month = '05'
GROUP BY uri_path, http_method
HAVING COUNT(*) > 100
ORDER BY count_5xx_errors DESC, p95_latency_ms DESC;
