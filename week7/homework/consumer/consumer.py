import sys
import time
from time import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from model import trips_deserializer

topic_name = 'green_trips'
server = 'localhost:9092'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-console-2',
    value_deserializer=trips_deserializer,
    consumer_timeout_ms=10000
)

trip_gt_5 = 0
for message in consumer:
    trip = message.value
    if trip.trip_distance > 5:
        trip_gt_5 += 1

print(f"Number of trips with distance greater than 5 miles: {trip_gt_5}")