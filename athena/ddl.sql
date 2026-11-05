CREATE DATABASE IF NOT EXISTS sales_lake;

CREATE EXTERNAL TABLE IF NOT EXISTS sales_lake.curated_sales (
  transaction_id string,
  customer_id string,
  product_id string,
  quantity int,
  unit_price double,
  currency string,
  revenue double
)
PARTITIONED BY (sale_date date)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/YOUR_PREFIX/curated/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- Discover Hive-style sale_date=YYYY-MM-DD prefixes after uploading new data.
MSCK REPAIR TABLE sales_lake.curated_sales;

