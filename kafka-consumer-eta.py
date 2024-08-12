from confluent_kafka import Consumer, Producer
import json
import time
import math

# Kafka Consumer Configuration
conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'location-group', 'auto.offset.reset': 'earliest'}
consumer = Consumer(**conf)
consumer.subscribe(['driver-location-updates'])

# Kafka Producer Configuration
producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(**producer_conf)

destination = {"latitude": 40.748817, "longitude": -73.985428}

average_speed = 15

def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance

def calculate_eta(current_location, destination):
    distance = calculate_distance(
        current_location['latitude'], current_location['longitude'],
        destination['latitude'], destination['longitude']
    )
    eta_seconds = distance / average_speed
    eta_minutes = eta_seconds / 60
    return eta_minutes

while True:
    msg = consumer.poll(2.0)
    if msg is None:
        continue

    location_data = json.loads(msg.value().decode('utf-8'))
    eta = calculate_eta(location_data, destination)
    print(str(eta))
    eta_update = {
        "driver_id": location_data['driver_id'],
        "eta": f"{eta:.2f} minutes",
        "timestamp": time.time()
    }
    producer.produce(f'driver-{location_data["driver_id"]}-eta-updates', key=location_data['driver_id'], value=json.dumps(eta_update))
    producer.flush()
