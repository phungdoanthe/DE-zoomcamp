-- Question 1
-- Create an external table using the Yellow Taxi Trip Records.

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi_external`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dezoomcamp2026/nyc-tlc/yellow/yellow_tripdata_2024-*.parquet'] -- 6 files

);

-- Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).
CREATE OR REPLACE TABLE `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi` 
as (
    select * from `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi_external`
);


-- Count the number of records in the external table to verify it was created successfully.

SELECT COUNT(*) AS total_records
FROM `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi_external`;
--  => result: 20,332,093 

-- Question 2
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

SELECT COUNT(DISTINCT(PULocationID)) AS count_distinct_pulocation_ids
FROM  `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi_external`;

-- => This query will process 0 B when run.

SELECT COUNT(DISTINCT(PULocationID)) AS count_distinct_pulocation_ids
FROM  `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi`;

-- => This script will process 155.12 MB when run.


-- Question 3
-- Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery
SELECT PULocationID FROM `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi`;
-- => This query will process 155.12 MB when run.


--Write a query to retrieve the PULocationID and DOLocationID on the same table

SELECT PULocationID, DOLocationID FROM `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi`;
-- => This query will process 310.24 MB when run.

-- => BigQuery is a columnar database, and it only scans the specific columns requested in the query. 
-- Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), 
-- leading to a higher estimated number of bytes processed.

-- Question 4
-- How many records have a fare_amount of 0?
SELECT COUNT(*) AS fare_amount_zero_count FROM `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi` WHERE fare_amount = 0;
-- => result: 8,333

-- Question 5
--Create a new partitioned table

CREATE OR REPLACE TABLE `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi_partitioned`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID
AS 
SELECT *
FROM `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi`

-- => Partition by tpep_dropoff_datetime and Cluster on VendorID

-- Question 6
-- Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

SELECT DISTINCT(VendorID) AS distinct_vendor_id
FROM `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
ORDER BY distinct_vendor_id;
-- => This query will process 310.24 MB when run.

SELECT DISTINCT(VendorID) AS distinct_vendor_id
FROM `de-zoomcamp-2026-485014.nyc_yellow.yellow_taxi_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
ORDER BY distinct_vendor_id;
-- => This query will process 26.86 MB when run.

-- => 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

-- Question 7:
-- Where is the data stored in the External Table you created?
-- => GCP Bucket

-- Question 8:
-- It is best practice in Big Query to always cluster your data:
-- => False

-- Question 9:
-- Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
-- This query will process 0 B when run. Because of the metadata stored in BigQuery for materialized tables,
-- it knows the total number of records without needing to scan the actual data along with other statistics(like size, number of columns, categories etc.)