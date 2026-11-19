CREATE DATABASE IF NOT EXISTS tlc_lake;

CREATE EXTERNAL TABLE IF NOT EXISTS tlc_lake.yellow_trips (
  tpep_pickup_datetime timestamp,
  tpep_dropoff_datetime timestamp,
  pulocationid int,
  dolocationid int,
  trip_distance double
)
PARTITIONED BY (release_month string)
STORED AS PARQUET
LOCATION 's3://BUCKET/TLC_PREFIX/curated/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.release_month.type' = 'date',
  'projection.release_month.range' = '2009-01,NOW',
  'projection.release_month.format' = 'yyyy-MM',
  'projection.release_month.interval' = '1',
  'projection.release_month.interval.unit' = 'MONTHS',
  'storage.location.template' = 's3://BUCKET/TLC_PREFIX/curated/release_month=${release_month}/'
);
