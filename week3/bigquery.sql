-- Question 1
-- Create an external table using the Yellow Taxi Trip Records.

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi_external`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dezoomcamp2026/nyc-tlc/yellow/yellow_trips_2024-*.parquet'], -- 6 files
  skip_leading_rows = 1,
  autodetect = TRUE
);

-- Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).
CREATE OR REPLACE TABLE `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi` 
as (
    select * from `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi_external`
);


-- Count the number of records in the external table to verify it was created successfully.

SELECT COUNT(*) AS total_records
FROM `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi_external`;

-- Question 2
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

SELECT COUNT(DISTINCT(PULocationID)) AS count_distinct_pulocation_ids
FROM  `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi_external`;

SELECT COUNT(DISTINCT(PULocationID)) AS count_distinct_pulocation_ids
FROM  `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi`;

-- Question 3
-- Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery
SELECT PULocationID FROM `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi`;

--Write a query to retrieve the PULocationID and DOLocationID on the same table

SELECT PULocationID, DOLocationID FROM `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi`;

-- Question 4
-- How many records have a fare_amount of 0?
SELECT COUNT(*) AS fare_amount_zero_count FROM `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi` WHERE fare_amount = 0;

-- Question 5
--Create a new partitioned table

CREATE OR REPLACE TABLE 'de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi_partitioned'
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID
AS 
SELECT *
FROM `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi`

-- Question 6
-- Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

SELECT DISTINCT(VendorID) AS distinct_vendor_id
FROM 'de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi'
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
ORDER BY distinct_vendor_id;

SELECT DISTINCT(VendorID) AS distinct_vendor_id
FROM 'de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi_partitioned'
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
ORDER BY distinct_vendor_id;

