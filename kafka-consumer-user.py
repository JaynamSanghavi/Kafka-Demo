from confluent_kafka import Consumer
import json

assigned_driver_id = "driver123"

# Kafka Consumer Configuration
conf = {'bootstrap.servers': 'localhost:9092', 'group.id': f'user-group-{assigned_driver_id}', 'auto.offset.reset': 'earliest'}
consumer = Consumer(**conf)
consumer.subscribe([f'driver-{assigned_driver_id}-eta-updates'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue

    eta_update = json.loads(msg.value().decode('utf-8'))
    print(f"Driver {eta_update['driver_id']} will arrive in {eta_update['eta']}")
