from dataclasses import dataclass
import dataclasses
import json

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from model import TaxiRide, trips_serializer, row_to_taxi_ride


df = pd.read_parquet(
    'green_tripdata_2025-10.parquet',
    columns=[
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'tip_amount'
    ]
)

datetime_cols = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]
for c in datetime_cols:
    df[c] = df[c].astype(str)


server = 'localhost:9092'
topic_name = 'green_trips'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=trips_serializer,
    batch_size=32768, 
    linger_ms=10,
    compression_type='gzip'
)

producer.bootstrap_connected()

import time

t0 = time.time()

for i, row in enumerate(df.to_dict(orient='records')):
    trip = row_to_taxi_ride(row)
    producer.send(topic_name, value=trip)
    if i % 5000 == 0:
        print(f"Sent {i} trips")

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')

