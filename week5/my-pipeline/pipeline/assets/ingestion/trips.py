"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd
import pyarrow as pa
import tzdata

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def generate_months_to_ingest(start_date, end_date):
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    months = pd.period_range(start=start, end=end, freq="M")

    for period in months:
        yield str(period.year), f"{period.month:02d}"

def build_trip_url(taxi_type, year, month):
  return f"{base_url}/{taxi_type}_tripdata_{year}-{month}.parquet"

def fetch_trip_data(url):
  print(f"Fetching data from {url}")
  df = pd.read_parquet(url)
  return df

def materialize():
  start_date = os.environ["BRUIN_START_DATE"]
  end_date = os.environ["BRUIN_END_DATE"]
  taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

  # Generate list of months between start and end dates
  # Fetch parquet files from:
  # https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet

  year_months = generate_months_to_ingest(start_date, end_date)
  final_df_list = []
  for year, month in year_months:
    for taxi_type in taxi_types:
      url = build_trip_url(taxi_type, year, month)
      df = fetch_trip_data(url)
      final_df_list.append(df)
  final_df = pd.concat(final_df_list, ignore_index=True)

  return final_df