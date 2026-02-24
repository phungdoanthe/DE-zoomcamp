"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: taxi_type
    type: string
    description: "NYC taxi service type (yellow or green)"
  - name: tpep_pickup_datetime
    type: timestamp
    description: "Raw pickup datetime for yellow taxi trips"
  - name: lpep_pickup_datetime
    type: timestamp
    description: "Raw pickup datetime for green taxi trips"
  - name: tpep_dropoff_datetime
    type: timestamp
    description: "Raw dropoff datetime for yellow taxi trips"
  - name: lpep_dropoff_datetime
    type: timestamp
    description: "Raw dropoff datetime for green taxi trips"
  - name: PULocationID
    type: integer
    description: "Raw TLC pickup location ID"
  - name: DOLocationID
    type: integer
    description: "Raw TLC dropoff location ID"
  - name: passenger_count
    type: integer
    description: "Number of passengers as reported by the driver"
  - name: trip_distance
    type: float
    description: "Trip distance in miles as reported by the taximeter"
  - name: payment_type
    type: integer
    description: "Numeric payment type code from TLC"
  - name: fare_amount
    type: float
    description: "Base fare amount in USD"
  - name: total_amount
    type: float
    description: "Total charged amount in USD"
  - name: extracted_at
    type: timestamp
    description: "UTC timestamp when record was ingested"

@bruin"""

import json
import os
from datetime import date, datetime
from io import BytesIO
from typing import Iterable, List

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def _month_starts(start: date, end: date) -> Iterable[date]:
    """
    Generate the first day of each month between start (inclusive) and end (exclusive).
    """
    current = start.replace(day=1)
    while current < end:
        yield current
        current += relativedelta(months=1)


def _get_taxi_types() -> List[str]:
    """
    Read taxi_types from BRUIN_VARS, defaulting to yellow if unset.
    """
    raw_vars = os.environ.get("BRUIN_VARS", "") or "{}"
    vars_dict = json.loads(raw_vars)
    taxi_types = vars_dict.get("taxi_types") or ["yellow"]
    return taxi_types


def materialize():
    """
    Ingest raw NYC taxi trip data from TLC parquet files.

    - Uses BRUIN_START_DATE / BRUIN_END_DATE to determine the run window.
    - Uses the taxi_types pipeline variable to select which taxi services to ingest.
    - Fetches one parquet file per taxi type per month and keeps the schema as raw as possible.
    - Adds taxi_type and extracted_at columns for lineage and debugging.
    - Returns a pandas DataFrame for Bruin to materialize into DuckDB.
    """
    start_str = os.environ["BRUIN_START_DATE"]
    end_str = os.environ["BRUIN_END_DATE"]
    start_date = date.fromisoformat(start_str)
    end_date = date.fromisoformat(end_str)

    taxi_types = _get_taxi_types()
    extracted_at = datetime.utcnow()

    frames: List[pd.DataFrame] = []

    for taxi_type in taxi_types:
        for month_start in _month_starts(start_date, end_date):
            year = month_start.year
            month = month_start.month
            url = f"{BASE_URL}{taxi_type}_tripdata_{year}-{month:02d}.parquet"

            response = requests.get(url, stream=True)
            if response.status_code != 200:
                # Skip months without data or inaccessible files.
                continue

            buffer = BytesIO(response.content)
            df = pd.read_parquet(buffer)
            df["taxi_type"] = taxi_type
            df["extracted_at"] = extracted_at
            frames.append(df)

    if not frames:
        # No data for the requested window; return an empty DataFrame with no rows.
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True, sort=False)


