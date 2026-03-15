import dataclasses
import json
from dataclasses import dataclass


@dataclass
class TaxiRide:
    pickup_datetime: int
    dropoff_datetime: int
    pickup_location_id: int
    dropoff_location_id: int
    passenger_count: int
    trip_distance: float
    tip_amount: float


def trips_serializer(trip):
    trip_dict = dataclasses.asdict(trip)
    return json.dumps(trip_dict).encode('utf-8')

def row_to_taxi_ride(row):
    return TaxiRide(
        pickup_datetime=row['lpep_pickup_datetime'],
        dropoff_datetime=row['lpep_dropoff_datetime'],
        pickup_location_id=row['PULocationID'],
        dropoff_location_id=row['DOLocationID'],
        passenger_count=row['passenger_count'],
        trip_distance=row['trip_distance'],
        tip_amount=row['tip_amount']
    )


def trips_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return TaxiRide(**ride_dict)