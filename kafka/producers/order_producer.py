#This simulates realistic order lifecycle events. Every 8 seconds a random order moves to its next status — exactly like a real logistics tracking system
import json
import time
import uuid
import random
import os
import sys
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from api.database import SessionLocal
from api.models import Order, Supplier

load_dotenv()

# Order lifecycle stages in sequence
ORDER_LIFECYCLE = [
    "ORDER_PLACED",
    "WAREHOUSE_PROCESSING",
    "DEPARTED_WAREHOUSE",
    "IN_TRANSIT",
    "ARRIVED_AT_PORT",
    "CUSTOMS_CLEARANCE",
    "OUT_FOR_DELIVERY",
    "DELIVERED"
]

# Delay scenarios that can happen at any stage
DELAY_SCENARIOS = [
    {"type": "CUSTOMS_DELAY", "description": "Shipment held at customs for additional inspection", "delay_days": random.randint(2, 7)},
    {"type": "WEATHER_DELAY", "description": "Severe weather conditions delaying transit", "delay_days": random.randint(1, 5)},
    {"type": "PORT_CONGESTION", "description": "Port congestion causing loading delays", "delay_days": random.randint(1, 4)},
    {"type": "DOCUMENTATION_ISSUE", "description": "Missing or incorrect shipping documentation", "delay_days": random.randint(1, 3)},
    {"type": "VEHICLE_BREAKDOWN", "description": "Delivery vehicle breakdown requiring replacement", "delay_days": random.randint(1, 2)},
]

def create_producer():
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8')
            )
            print("Order producer connected to Kafka")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready — retrying ({i+1}/5)")
            time.sleep(5)
    raise Exception("Could not connect to Kafka")

def get_active_orders():
    db = SessionLocal()
    try:
        orders = db.query(Order, Supplier).join(
            Supplier, Order.supplier_id == Supplier.supplier_id
        ).filter(
            Order.status.in_(["IN_TRANSIT", "PROCESSING", "PENDING"])
        ).all()
        return orders
    finally:
        db.close()

def generate_order_event(order, supplier):
    # Determine next status
    current_index = 0
    if order.status in ORDER_LIFECYCLE:
        current_index = ORDER_LIFECYCLE.index(order.status)

    # 15% chance of a delay event
    if random.random() < 0.15:
        delay = random.choice(DELAY_SCENARIOS)
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "ORDER_DELAYED",
            "timestamp": datetime.now().isoformat(),
            "order_id": order.order_id,
            "supplier_id": order.supplier_id,
            "supplier_name": supplier.name,
            "region": supplier.region,
            "country": supplier.country,
            "product_id": order.product_id,
            "quantity": order.quantity,
            "delay_type": delay["type"],
            "delay_description": delay["description"],
            "additional_delay_days": delay["delay_days"],
            "current_status": order.status,
            "risk_flag": True
        }
        return event

    # Normal progression to next stage
    next_status = ORDER_LIFECYCLE[min(current_index + 1, len(ORDER_LIFECYCLE) - 1)]

    # Calculate if delivered on time
    on_time = None
    if next_status == "DELIVERED":
        on_time = datetime.now() <= order.expected_delivery

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_STATUS_UPDATE",
        "timestamp": datetime.now().isoformat(),
        "order_id": order.order_id,
        "supplier_id": order.supplier_id,
        "supplier_name": supplier.name,
        "region": supplier.region,
        "country": supplier.country,
        "product_id": order.product_id,
        "quantity": order.quantity,
        "previous_status": order.status,
        "new_status": next_status,
        "expected_delivery": order.expected_delivery.isoformat() if order.expected_delivery else None,
        "on_time": on_time,
        "risk_flag": False
    }
    return event

def simulate_new_order(suppliers):
    supplier = random.choice(suppliers)
    products = ["P001", "P002", "P003", "P004", "P005"]
    product = random.choice(products)
    lead_time = random.randint(5, 20)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_PLACED",
        "timestamp": datetime.now().isoformat(),
        "order_id": f"O{str(uuid.uuid4())[:8].upper()}",
        "supplier_id": supplier.supplier_id,
        "supplier_name": supplier.name,
        "region": supplier.region,
        "country": supplier.country,
        "product_id": product,
        "quantity": random.randint(100, 2000),
        "order_date": datetime.now().isoformat(),
        "expected_delivery": (datetime.now() + timedelta(days=lead_time)).isoformat(),
        "unit_price": round(random.uniform(5, 60), 2),
        "risk_flag": False
    }
    return event

def run_order_producer():
    producer = create_producer()
    db = SessionLocal()

    print("\n🚚 Order Events Producer started")
    print("Publishing order lifecycle events every 8 seconds...")
    print("-" * 60)

    try:
        tick = 0
        while True:
            tick += 1

            # Every 5 ticks — simulate a new order being placed
            if tick % 5 == 0:
                suppliers = db.query(Supplier).all()
                if suppliers:
                    event = simulate_new_order(suppliers)
                    producer.send(
                        topic='order-events',
                        key=event['supplier_id'],
                        value=event
                    )
                    print(f"📦 NEW ORDER: {event['order_id']} placed with "
                          f"{event['supplier_name']} — "
                          f"{event['quantity']} units of {event['product_id']}")

            # Regular order status updates
            orders_with_suppliers = get_active_orders()

            if orders_with_suppliers:
                # Pick a random active order to update
                order, supplier = random.choice(orders_with_suppliers)
                event = generate_order_event(order, supplier)

                producer.send(
                    topic='order-events',
                    key=event['supplier_id'],
                    value=event
                )

                if event['event_type'] == 'ORDER_DELAYED':
                    print(f"⚠️  DELAY: {event['order_id']} — "
                          f"{event['supplier_name']} — "
                          f"{event['delay_type']} — "
                          f"+{event['additional_delay_days']} days")
                else:
                    print(f"📍 STATUS: {event['order_id']} — "
                          f"{event['supplier_name']} — "
                          f"{event.get('previous_status', '?')} → "
                          f"{event.get('new_status', '?')}")
            else:
                print("No active orders found — waiting for new orders...")

            time.sleep(8)

    except KeyboardInterrupt:
        print("\nOrder producer stopped")
    finally:
        producer.close()
        db.close()

if __name__ == "__main__":
    run_order_producer()