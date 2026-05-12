from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time
import uuid
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.database import SessionLocal, engine
from api.models import Base, RiskAlert
from ai.rag_retriever import retrieve_supplier_context, format_context_for_prompt
from ai.llm_analyzer import analyze_supplier_risk

load_dotenv()

Base.metadata.create_all(bind=engine)

def create_consumer():
    retries = 5
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                'order-events',
                'weather-events',
                'commodity-events',
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
            print(f"Kafka not ready — retrying ({i+1}/5)")
            time.sleep(5)
    raise Exception("Could not connect to Kafka")

def detect_anomaly(event: dict) -> dict:
    metrics = event.get('metrics', {})
    on_time_rate = metrics.get('on_time_delivery_rate', 1.0)
    defect_rate = metrics.get('defect_rate', 0.0)
    avg_delay_days = metrics.get('avg_delay_days', 0)

    anomalies = []
    severity = "LOW"

    if on_time_rate < 0.60:
        anomalies.append(f"Critical delivery failure — only {on_time_rate*100:.0f}% on time")
        severity = "CRITICAL"
    elif on_time_rate < 0.75:
        anomalies.append(f"Poor delivery performance — {on_time_rate*100:.0f}% on time")
        severity = "HIGH"
    elif on_time_rate < 0.85:
        anomalies.append(f"Below average delivery — {on_time_rate*100:.0f}% on time")
        if severity == "LOW":
            severity = "MEDIUM"

    if defect_rate > 0.20:
        anomalies.append(f"Critical defect rate — {defect_rate*100:.0f}% defective")
        severity = "CRITICAL"
    elif defect_rate > 0.15:
        anomalies.append(f"High defect rate — {defect_rate*100:.0f}% defective")
        if severity in ["LOW", "MEDIUM"]:
            severity = "HIGH"

    if avg_delay_days > 10:
        anomalies.append(f"Severe delays — average {avg_delay_days} days late")
    elif avg_delay_days > 7:
        anomalies.append(f"Significant delays — average {avg_delay_days} days late")

    return {
        "has_anomaly": len(anomalies) > 0,
        "anomalies": anomalies,
        "severity": severity
    }

def save_risk_alert_with_ai(event: dict, anomaly_result: dict, db):
    """
    RAG + AI enhanced alert saving.
    1. Retrieve context from PostgreSQL (RAG)
    2. Format context for LLM prompt
    3. Call OpenAI for specific analysis
    4. Save complete alert with ai_analysis filled
    """
    if not anomaly_result["has_anomaly"]:
        return None

    supplier_id = event["supplier_id"]
    supplier_name = event.get("supplier_name", "Unknown")
    description = " | ".join(anomaly_result["anomalies"])

    print(f"  🔍 RAG: Retrieving context for {supplier_name}...")

    # Step 1 — RAG: retrieve relevant context
    context = retrieve_supplier_context(supplier_id, db)

    # Step 2 — RAG: format context for prompt
    rag_context = format_context_for_prompt(
        context,
        event.get("metrics", {})
    )

    # Step 3 — AI: generate specific analysis
    print(f"  🤖 Calling OpenAI for {supplier_name} analysis...")
    ai_analysis = analyze_supplier_risk(
        supplier_name=supplier_name,
        anomaly_description=description,
        severity=anomaly_result["severity"],
        rag_context=rag_context
    )

    # Step 4 — Save complete alert with AI analysis
    alert = RiskAlert(
        alert_id=str(uuid.uuid4()),
        supplier_id=supplier_id,
        alert_type="PERFORMANCE_DEGRADATION",
        severity=anomaly_result["severity"],
        description=description,
        ai_analysis=ai_analysis
    )

    db.add(alert)
    db.commit()

    print(f"  ✅ Alert saved with AI analysis — {anomaly_result['severity']} for {supplier_name}")
    print(f"  📋 Analysis: {ai_analysis[:100]}...")
    return alert

def process_event(event: dict, db):
    event_type = event.get("event_type", "")

    if event_type in ["SUPPLIER_PERFORMANCE_UPDATE", "ORDER_DELAYED"]:
        print(f"\n📨 Processing: {event_type} for {event.get('supplier_name', 'Unknown')}")

        metrics = event.get("metrics", {})
        if metrics:
            print(f"   On-time: {metrics.get('on_time_delivery_rate', 1)*100:.0f}% | "
                  f"Defects: {metrics.get('defect_rate', 0)*100:.0f}% | "
                  f"Delay: {metrics.get('avg_delay_days', 0)} days")

        anomaly_result = detect_anomaly(event)

        if anomaly_result["has_anomaly"]:
            print(f"  🚨 Anomaly: {' | '.join(anomaly_result['anomalies'])}")
            save_risk_alert_with_ai(event, anomaly_result, db)
        else:
            print(f"  ✅ Normal — no action needed")

    elif event_type == "WEATHER_ALERT":
        from api.models import WeatherEvent
        print(f"\n🌤️  Weather alert: {event.get('region')} — {event.get('severity')}")

        weather = WeatherEvent(
            event_id=str(uuid.uuid4()),
            location=event.get("city", "Unknown"),
            country=event.get("country", "Unknown"),
            event_type=event.get("weather_condition", "Unknown"),
            severity=event.get("severity", "NORMAL"),
            description=event.get("description", "")
        )
        db.add(weather)
        db.commit()
        print(f"  💾 Weather event saved: {event.get('city')} — {event.get('description')}")

    elif event_type == "COMMODITY_PRICE_CHANGE":
        print(f"\n📈 Commodity: {event.get('commodity_name')} "
              f"{event.get('direction')} {abs(event.get('price_change_pct', 0)):.1f}% — "
              f"Severity: {event.get('severity')}")

def run_consumer():
    consumer = create_consumer()
    db = SessionLocal()

    print("\n🚀 Supply Chain Risk Consumer started")
    print("Listening on: order-events | weather-events | commodity-events")
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