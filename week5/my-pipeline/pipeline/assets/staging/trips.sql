/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null

custom_checks:
  - name: row_count_greater_than_zero
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips
    value: 1
@bruin */

WITH data_source AS (

    SELECT
        -- Normalize column names
        tpep_pickup_datetime  AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,

        PULocationID AS pickup_location_id,
        DOLocationID AS dropoff_location_id,

        VendorID      AS vendor_id,
        passenger_count,
        trip_distance,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount

    FROM ingestion.trips
    WHERE taxi_type = 'yellow'

)

SELECT
    d.pickup_datetime,
    d.dropoff_datetime,
    d.pickup_location_id,
    d.dropoff_location_id,
    d.fare_amount,
    d.taxi_type,
    
    coalesce(p.payment_type_name, "unknown") AS payment_type_name
FROM data_source d
LEFT JOIN ingestion.payment_lookup p
    ON d.payment_type = p.payment_type_id
WHERE d.pickup_datetime >= '{{ start_datetime }}'
  AND d.pickup_datetime < '{{ end_datetime }}'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.pickup_datetime, d.dropoff_datetime,
                 d.pickup_location_id, d.dropoff_location_id, d.fare_amount
    ORDER BY d.pickup_datetime
) = 1