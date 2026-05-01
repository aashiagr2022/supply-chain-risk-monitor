import random
import uuid
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from api.database import engine, SessionLocal, Base
from api.models import Supplier, Order

# Create all tables
Base.metadata.create_all(bind=engine)

SUPPLIERS = [
    {"name": "AsiaTech Components", "country": "China", "region": "Asia Pacific"},
    {"name": "EuroMfg GmbH", "country": "Germany", "region": "Western Europe"},
    {"name": "AmeriParts Co", "country": "USA", "region": "North America"},
    {"name": "IndoPrecision", "country": "India", "region": "South Asia"},
    {"name": "BrazilFab", "country": "Brazil", "region": "Latin America"},
    {"name": "JapanTech Ltd", "country": "Japan", "region": "Asia Pacific"},
    {"name": "MexicoMfg", "country": "Mexico", "region": "North America"},
    {"name": "UKComponents", "country": "UK", "region": "Western Europe"},
]

PRODUCTS = [
    {"product_id": "P001", "name": "Microprocessor", "unit_price": 45.00},
    {"product_id": "P002", "name": "Steel Rod", "unit_price": 12.00},
    {"product_id": "P003", "name": "Circuit Board", "unit_price": 28.00},
    {"product_id": "P004", "name": "Aluminum Sheet", "unit_price": 8.00},
    {"product_id": "P005", "name": "Rubber Seal", "unit_price": 3.50},
]

def seed_suppliers(db: Session):
    for s in SUPPLIERS:
        supplier = Supplier(
            supplier_id=f"S{str(uuid.uuid4())[:8].upper()}",
            name=s["name"],
            country=s["country"],
            region=s["region"],
            reliability_score=round(random.uniform(5.0, 9.5), 1),
            contract_value=random.choice([150000, 250000, 420000, 680000, 920000])
        )
        db.add(supplier)
    db.commit()
    print(f"Seeded {len(SUPPLIERS)} suppliers")

def generate_order(supplier_id: str, db: Session):
    product = random.choice(PRODUCTS)
    order_date = datetime.now() - timedelta(days=random.randint(1, 30))
    lead_time = random.randint(5, 20)
    expected_delivery = order_date + timedelta(days=lead_time)
    
    # Simulate some delays
    delay_days = 0
    actual_delivery = None
    status = "IN_TRANSIT"
    
    if random.random() < 0.3:  # 30% chance of delay
        delay_days = random.randint(1, 15)
        actual_delivery = expected_delivery + timedelta(days=delay_days)
        status = "DELAYED"
    elif random.random() < 0.6:  # some delivered on time
        actual_delivery = expected_delivery - timedelta(days=random.randint(0, 2))
        status = "DELIVERED"

    order = Order(
        order_id=f"O{str(uuid.uuid4())[:8].upper()}",
        supplier_id=supplier_id,
        product_id=product["product_id"],
        quantity=random.randint(100, 2000),
        order_date=order_date,
        expected_delivery=expected_delivery,
        actual_delivery=actual_delivery,
        status=status,
        delay_days=delay_days,
        unit_price=product["unit_price"]
    )
    db.add(order)
    db.commit()
    return order

if __name__ == "__main__":
    db = SessionLocal()
    
    # Check if suppliers already exist
    existing = db.query(Supplier).count()
    if existing == 0:
        seed_suppliers(db)
    
    # Get all supplier IDs
    suppliers = db.query(Supplier).all()
    
    # Generate 50 orders across all suppliers
    for _ in range(50):
        supplier = random.choice(suppliers)
        order = generate_order(supplier.supplier_id, db)
        print(f"Generated order {order.order_id} for {supplier.name}")
    
    db.close()
    print("Data generation complete")