#producer.py generates a supplier performance event every 3 seconds and publishes it to a Kafka topic called supplier-events. The consumer independently reads from that topic — the producer never communicates with the consumer directly

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import random
import uuid
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

def create_producer():
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8')
            )
            print("Kafka producer connected successfully")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready — retrying in 5 seconds ({i+1}/{retries})")
            time.sleep(5)
    raise Exception("Could not connect to Kafka after multiple retries")

def generate_supplier_event():
    suppliers = [
        {"id": "S001", "name": "AsiaTech Components", "country": "China", "region": "Asia Pacific"},
        {"id": "S002", "name": "EuroMfg GmbH", "country": "Germany", "region": "Western Europe"},
        {"id": "S003", "name": "AmeriParts Co", "country": "USA", "region": "North America"},
        {"id": "S004", "name": "IndoPrecision", "country": "India", "region": "South Asia"},
    ]
    
    supplier = random.choice(suppliers)
    
    # Simulate delivery performance metrics
    on_time_rate = round(random.uniform(0.55, 0.98), 2)
    avg_delay_days = round(random.uniform(0, 12), 1)
    defect_rate = round(random.uniform(0.01, 0.25), 2)
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "SUPPLIER_PERFORMANCE_UPDATE",
        "timestamp": datetime.now().isoformat(),
        "supplier_id": supplier["id"],
        "supplier_name": supplier["name"],
        "country": supplier["country"],
        "region": supplier["region"],
        "metrics": {
            "on_time_delivery_rate": on_time_rate,
            "avg_delay_days": avg_delay_days,
            "defect_rate": defect_rate,
            "orders_this_week": random.randint(5, 50)
        },
        "risk_flag": on_time_rate < 0.75 or defect_rate > 0.15
    }
    
    return event

def run_producer():
    producer = create_producer()
    print("Starting supplier event stream...")
    
    try:
        while True:
            event = generate_supplier_event()
            
            # Publish to supplier-events topic
            producer.send(
                topic="supplier-events",
                key=event["supplier_id"],
                value=event
            )
            
            print(f"Published event for {event['supplier_name']} — "
                  f"On-time: {event['metrics']['on_time_delivery_rate']*100:.0f}% — "
                  f"Risk: {'🔴 HIGH' if event['risk_flag'] else '🟢 OK'}")
            
            # Publish every 3 seconds
            time.sleep(3)
            
    except KeyboardInterrupt:
        print("Producer stopped")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()