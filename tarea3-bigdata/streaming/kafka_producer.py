# -*- coding: utf-8 -*-
"""
Productor de Kafka: genera datos simulados de sensores y los envía al topic
'sensor_data' una vez por segundo.

Ejecución:  python3 kafka_producer.py     (detener con Ctrl+C)
"""
import time
import json
import random
from kafka import KafkaProducer


def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": int(time.time()),
    }


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

try:
    while True:
        sensor_data = generate_sensor_data()
        producer.send("sensor_data", value=sensor_data)
        print(f"Sent: {sensor_data}")
        time.sleep(1)
except KeyboardInterrupt:
    print("\nProductor detenido.")
finally:
    producer.flush()
    producer.close()
