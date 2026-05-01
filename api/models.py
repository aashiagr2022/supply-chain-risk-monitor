from sqlalchemy import Column, String, Float, Integer, DateTime, Boolean, Text
from sqlalchemy.sql import func
from api.database import Base

class Supplier(Base):
    __tablename__ = "suppliers"
    supplier_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    country = Column(String, nullable=False)
    region = Column(String, nullable=False)
    reliability_score = Column(Float, nullable=False)
    contract_value = Column(Float, nullable=False)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())

class Order(Base):
    __tablename__ = "orders"
    order_id = Column(String, primary_key=True)
    supplier_id = Column(String, nullable=False)
    product_id = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    order_date = Column(DateTime, nullable=False)
    expected_delivery = Column(DateTime, nullable=False)
    actual_delivery = Column(DateTime, nullable=True)
    status = Column(String, nullable=False)
    delay_days = Column(Integer, default=0)
    unit_price = Column(Float, nullable=False)

class RiskAlert(Base):
    __tablename__ = "risk_alerts"
    alert_id = Column(String, primary_key=True)
    supplier_id = Column(String, nullable=False)
    alert_type = Column(String, nullable=False)
    severity = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    ai_analysis = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    resolved = Column(Boolean, default=False)

class WeatherEvent(Base):
    __tablename__ = "weather_events"
    event_id = Column(String, primary_key=True)
    location = Column(String, nullable=False)
    country = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    severity = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())