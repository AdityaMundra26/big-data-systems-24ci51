-- ==============================================================================
-- BIG DATA SYSTEMS (24CI51) - SELF LEARNING ACTIVITY 1
-- TECHNOLOGY: APACHE HIVE
-- SCENARIO: SUPERMARKET SALES TRANSACTION ANALYSIS & INVENTORY OPTIMIZATION
-- ==============================================================================

-- 1. DATABASE CREATION AND INITIALIZATION
CREATE DATABASE IF NOT EXISTS retail_dw
COMMENT 'Enterprise Supermarket Sales Data Warehouse'
LOCATION '/user/hive/warehouse/retail_dw.db';

USE retail_dw;

-- Set execution engine and optimization properties
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.autogather=true;

-- 2. STAGING EXTERNAL TABLE (Raw Ingestion Layer)
DROP TABLE IF EXISTS raw_sales_transactions;

CREATE EXTERNAL TABLE raw_sales_transactions (
    transaction_id        STRING,
    transaction_timestamp STRING,
    sale_date             STRING,
    sale_hour             INT,
    branch_id             STRING,
    branch_name           STRING,
    product_id            STRING,
    product_name          STRING,
    category              STRING,
    quantity              INT,
    unit_price            DECIMAL(10,2),
    total_amount          DECIMAL(10,2),
    payment_method        STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/retail/raw_transactions'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 3. MANAGED OPTIMIZED WAREHOUSE TABLE (Partitioned & Bucketed, Columnar ORC)
DROP TABLE IF EXISTS sales_transactions_orc;

CREATE TABLE sales_transactions_orc (
    transaction_id        STRING,
    transaction_timestamp TIMESTAMP,
    sale_hour             INT,
    branch_name           STRING,
    product_id            STRING,
    product_name          STRING,
    category              STRING,
    quantity              INT,
    unit_price            DECIMAL(10,2),
    total_amount          DECIMAL(10,2),
    payment_method        STRING
)
COMMENT 'Optimized Sales Fact Table Partitioned by Date and Branch'
PARTITIONED BY (sale_date STRING, branch_id STRING)
CLUSTERED BY (transaction_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES (
    "orc.compress"="SNAPPY",
    "orc.create.index"="true",
    "transactional"="false"
);

-- Dynamic Partitioning Configuration
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing=true;

-- Insert into optimized partitioned table from staging
INSERT OVERWRITE TABLE sales_transactions_orc 
PARTITION (sale_date, branch_id)
SELECT 
    transaction_id,
    CAST(transaction_timestamp AS TIMESTAMP),
    sale_hour,
    branch_name,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    sale_date,
    branch_id
FROM raw_sales_transactions;


-- ==============================================================================
-- ANALYTICAL QUERIES (HIVEQL)
-- ==============================================================================

-- QUERY 1: Overall Revenue and Branch-wise Sales Performance
-- Identifies highest grossing supermarket branches and average transaction values
SELECT 
    branch_id,
    branch_name,
    COUNT(DISTINCT transaction_id) AS total_orders,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_item_spend,
    ROUND((SUM(total_amount) / SUM(SUM(total_amount)) OVER ()) * 100, 2) AS revenue_share_pct
FROM sales_transactions_orc
GROUP BY branch_id, branch_name
ORDER BY total_revenue DESC;


-- QUERY 2: Top 10 Best-Selling Products by Revenue and Units Sold
-- Crucial for stock replenishment priority and supplier contracts
SELECT 
    product_id,
    product_name,
    category,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(unit_price), 2) AS unit_price,
    DENSE_RANK() OVER (ORDER BY SUM(total_amount) DESC) AS revenue_rank
FROM sales_transactions_orc
GROUP BY product_id, product_name, category
ORDER BY total_revenue DESC
LIMIT 10;


-- QUERY 3: Peak Sales Hours Analysis (Hourly Distribution)
-- Enables intelligent staff scheduling and fresh shelf restocking prior to surges
SELECT 
    sale_hour,
    COUNT(DISTINCT transaction_id) AS total_transactions,
    SUM(quantity) AS total_items_purchased,
    ROUND(SUM(total_amount), 2) AS hourly_revenue,
    CASE 
        WHEN sale_hour BETWEEN 7 AND 10 THEN 'Morning Rush'
        WHEN sale_hour BETWEEN 11 AND 14 THEN 'Mid-Day Peak'
        WHEN sale_hour BETWEEN 17 AND 20 THEN 'Evening Rush'
        ELSE 'Regular / Off-Peak'
    END AS operational_window
FROM sales_transactions_orc
GROUP BY sale_hour
ORDER BY sale_hour ASC;


-- QUERY 4: Monthly Sales Trends and Growth Trajectory
-- Tracks revenue and volume changes across Q1 months
SELECT 
    SUBSTR(sale_date, 1, 7) AS sales_month,
    COUNT(DISTINCT transaction_id) AS monthly_transactions,
    SUM(quantity) AS total_volume_sold,
    ROUND(SUM(total_amount), 2) AS monthly_revenue,
    ROUND(SUM(total_amount) - LAG(SUM(total_amount), 1, 0.0) OVER (ORDER BY SUBSTR(sale_date, 1, 7)), 2) AS mom_revenue_growth
FROM sales_transactions_orc
GROUP BY SUBSTR(sale_date, 1, 7)
ORDER BY sales_month ASC;


-- QUERY 5: Category-Wise Revenue and Product Diversity Breakdown
-- Guides aisle shelf-space allocation and category margin management
SELECT 
    category,
    COUNT(DISTINCT product_id) AS unique_products,
    SUM(quantity) AS units_sold,
    ROUND(SUM(total_amount), 2) AS category_revenue,
    ROUND((SUM(total_amount) / SUM(SUM(total_amount)) OVER ()) * 100, 2) AS category_share_pct
FROM sales_transactions_orc
GROUP BY category
ORDER BY category_revenue DESC;
