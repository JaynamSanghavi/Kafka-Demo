from confluent_kafka import Producer
import json
import time
import random

# Kafka Producer Configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(**conf)

def get_location(driver_id):
    # Mocking a location update
    return {
        "driver_id": driver_id,
        "latitude": random.uniform(-90, 90),
        "longitude": random.uniform(-180, 180),
        "timestamp": time.time()
    }

driver_id = "driver1234"

while True:
    location_data = get_location(driver_id)
    print(str(location_data))
    producer.produce('driver-location-updates', key=driver_id, value=json.dumps(location_data))
    producer.flush()  # Ensure the message is sent
    time.sleep(2)
