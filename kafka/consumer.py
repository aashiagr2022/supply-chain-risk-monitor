#The consumer reads events from the supplier-events topic, processes them, detects anomalies, and writes results to PostgreSQL.

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time
import uuid
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.database import SessionLocal, engine
from api.models import Base, RiskAlert

load_dotenv()

# Create tables if they don't exist
Base.metadata.create_all(bind=engine)

def create_consumer():
    retries = 5
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                'supplier-events',
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                group_id='supply-chain-risk-group',
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            print("Kafka consumer connected successfully")
            return consumer
        except NoBrokersAvailable:
            print(f"Kafka not ready — retrying in 5 seconds ({i+1}/{retries})")
            time.sleep(5)
    raise Exception("Could not connect to Kafka after multiple retries")

def detect_anomaly(event: dict) -> dict:
    metrics = event.get('metrics', {})
    on_time_rate = metrics.get('on_time_delivery_rate', 1.0)
    defect_rate = metrics.get('defect_rate', 0.0)
    avg_delay_days = metrics.get('avg_delay_days', 0)

    anomalies = []
    severity = "LOW"

    # Check on-time delivery rate
    if on_time_rate < 0.60:
        anomalies.append(f"Critical delivery failure — only {on_time_rate*100:.0f}% on time")
        severity = "CRITICAL"
    elif on_time_rate < 0.75:
        anomalies.append(f"Poor delivery performance — {on_time_rate*100:.0f}% on time")
        severity = "HIGH"
    elif on_time_rate < 0.85:
        anomalies.append(f"Below average delivery rate — {on_time_rate*100:.0f}% on time")
        if severity == "LOW":
            severity = "MEDIUM"

    # Check defect rate
    if defect_rate > 0.20:
        anomalies.append(f"Critical defect rate — {defect_rate*100:.0f}% defective")
        severity = "CRITICAL"
    elif defect_rate > 0.15:
        anomalies.append(f"High defect rate — {defect_rate*100:.0f}% defective")
        if severity in ["LOW", "MEDIUM"]:
            severity = "HIGH"

    # Check average delay
    if avg_delay_days > 10:
        anomalies.append(f"Severe delays — average {avg_delay_days} days late")
        if severity == "LOW":
            severity = "MEDIUM"
    elif avg_delay_days > 7:
        anomalies.append(f"Significant delays — average {avg_delay_days} days late")

    return {
        "has_anomaly": len(anomalies) > 0,
        "anomalies": anomalies,
        "severity": severity
    }

def save_risk_alert(event: dict, anomaly_result: dict, db):
    if not anomaly_result["has_anomaly"]:
        return None

    description = " | ".join(anomaly_result["anomalies"])

    alert = RiskAlert(
        alert_id=str(uuid.uuid4()),
        supplier_id=event["supplier_id"],
        alert_type="PERFORMANCE_DEGRADATION",
        severity=anomaly_result["severity"],
        description=description,
        ai_analysis=None  # Will be filled by AI layer later
    )

    db.add(alert)
    db.commit()
    print(f"  ⚠️  Risk alert saved — {anomaly_result['severity']} severity for {event['supplier_name']}")
    return alert

def process_event(event: dict, db):
    print(f"\n📨 Processing event for {event['supplier_name']} ({event['region']})")
    print(f"   On-time: {event['metrics']['on_time_delivery_rate']*100:.0f}% | "
          f"Defect rate: {event['metrics']['defect_rate']*100:.0f}% | "
          f"Avg delay: {event['metrics']['avg_delay_days']} days")

    # Detect anomalies
    anomaly_result = detect_anomaly(event)

    if anomaly_result["has_anomaly"]:
        print(f"  🚨 Anomaly detected: {' | '.join(anomaly_result['anomalies'])}")
        save_risk_alert(event, anomaly_result, db)
    else:
        print(f"  ✅ Performance normal — no action needed")

def run_consumer():
    consumer = create_consumer()
    db = SessionLocal()

    print("Starting supply chain risk consumer...")
    print("Listening for supplier events on topic: supplier-events")
    print("-" * 60)

    try:
        for message in consumer:
            event = message.value
            process_event(event, db)

    except KeyboardInterrupt:
        print("\nConsumer stopped")
    finally:
        consumer.close()
        db.close()

if __name__ == "__main__":
    run_consumer()