from confluent_kafka import Producer
import json
import time
import random

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(**conf)

def get_location(driver_id):
    return {
        "driver_id": driver_id,
        "latitude": random.uniform(-90, 90),
        "longitude": random.uniform(-180, 180),
        "timestamp": time.time()
    }

driver_id = "driver123"

while True:
    location_data = get_location(driver_id)
    print(str(location_data))
    producer.produce('driver-location-updates', key=driver_id, value=json.dumps(location_data))
    producer.flush()
    time.sleep(2)
