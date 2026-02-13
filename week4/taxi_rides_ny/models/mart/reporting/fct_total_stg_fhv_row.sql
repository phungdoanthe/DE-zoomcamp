select count(*) as total_records
from {{ ref("stg_fhv_tripdata") }}