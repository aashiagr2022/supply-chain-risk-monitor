from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import redis
import json
import os
from pydantic import BaseModel
from sqlalchemy import text
from ai.llm_analyzer import text_to_sql
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from api.database import get_db, engine
from api.models import Base, Supplier, Order, RiskAlert, WeatherEvent
from dotenv import load_dotenv
load_dotenv()

class QueryRequest(BaseModel):
    question: str

# Create all tables on startup
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Supply Chain Risk Monitor API",
    description="Real-time supply chain risk monitoring with AI-powered analysis",
    version="1.0.0"
)

# Mount static files
app.mount("/static", StaticFiles(directory="dashboard"), name="static")

@app.get("/dashboard")
def serve_dashboard():
    return FileResponse("dashboard/index.html")

# CORS middleware — allows dashboard to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Key authentication
API_KEY = os.getenv("API_KEY", "supply-chain-secret-key-2024")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def get_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return api_key

# Redis client
redis_client = redis.Redis.from_url(
    os.getenv("REDIS_URL", "redis://localhost:6379"),
    decode_responses=True
)

def get_redis():
    return redis_client

# Cache helper functions
def get_cached(key: str, redis_client) -> dict:
    cached = redis_client.get(key)
    if cached:
        return json.loads(cached)
    return None

def set_cached(key: str, data: dict, redis_client, ttl: int = 300):
    redis_client.setex(key, ttl, json.dumps(data, default=str))

# ==================== HEALTH CHECK ====================

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "Supply Chain Risk Monitor",
        "version": "1.0.0"
    }

# ==================== SUPPLIERS ====================

@app.get("/suppliers")
def get_suppliers(
    db: Session = Depends(get_db),
    redis: redis.Redis = Depends(get_redis),
    api_key: str = Depends(get_api_key)
):
    # Check Redis cache first
    cached = get_cached("suppliers:all", redis)
    if cached:
        return {"source": "cache", "data": cached}

    # Query PostgreSQL
    suppliers = db.query(Supplier).filter(Supplier.active == True).all()
    data = [
        {
            "supplier_id": s.supplier_id,
            "name": s.name,
            "country": s.country,
            "region": s.region,
            "reliability_score": s.reliability_score,
            "contract_value": s.contract_value,
            "active": s.active
        }
        for s in suppliers
    ]

    # Cache for 5 minutes
    set_cached("suppliers:all", data, redis, ttl=300)
    return {"source": "database", "count": len(data), "data": data}

@app.get("/suppliers/{supplier_id}")
def get_supplier(
    supplier_id: str,
    db: Session = Depends(get_db),
    redis: redis.Redis = Depends(get_redis),
    api_key: str = Depends(get_api_key)
):
    # Check cache
    cache_key = f"supplier:{supplier_id}"
    cached = get_cached(cache_key, redis)
    if cached:
        return {"source": "cache", "data": cached}

    supplier = db.query(Supplier).filter(
        Supplier.supplier_id == supplier_id
    ).first()

    if not supplier:
        raise HTTPException(status_code=404, detail="Supplier not found")

    data = {
        "supplier_id": supplier.supplier_id,
        "name": supplier.name,
        "country": supplier.country,
        "region": supplier.region,
        "reliability_score": supplier.reliability_score,
        "contract_value": supplier.contract_value
    }

    set_cached(cache_key, data, redis, ttl=300)
    return {"source": "database", "data": data}

# ==================== ORDERS ====================

@app.get("/orders")
def get_orders(
    status: str = None,
    supplier_id: str = None,
    limit: int = 50,
    db: Session = Depends(get_db),
    redis: redis.Redis = Depends(get_redis),
    api_key: str = Depends(get_api_key)
):
    cache_key = f"orders:{status}:{supplier_id}:{limit}"
    cached = get_cached(cache_key, redis)
    if cached:
        return {"source": "cache", "data": cached}

    query = db.query(Order)
    if status:
        query = query.filter(Order.status == status)
    if supplier_id:
        query = query.filter(Order.supplier_id == supplier_id)

    orders = query.limit(limit).all()
    data = [
        {
            "order_id": o.order_id,
            "supplier_id": o.supplier_id,
            "product_id": o.product_id,
            "quantity": o.quantity,
            "status": o.status,
            "delay_days": o.delay_days,
            "order_date": str(o.order_date),
            "expected_delivery": str(o.expected_delivery),
            "actual_delivery": str(o.actual_delivery) if o.actual_delivery else None,
            "unit_price": o.unit_price
        }
        for o in orders
    ]

    set_cached(cache_key, data, redis, ttl=60)
    return {"source": "database", "count": len(data), "data": data}

# ==================== RISK ALERTS ====================

@app.get("/alerts")
def get_alerts(
    severity: str = None,
    resolved: bool = False,
    limit: int = 20,
    db: Session = Depends(get_db),
    redis: redis.Redis = Depends(get_redis),
    api_key: str = Depends(get_api_key)
):
    cache_key = f"alerts:{severity}:{resolved}:{limit}"
    cached = get_cached(cache_key, redis)
    if cached:
        return {"source": "cache", "data": cached}

    query = db.query(RiskAlert).filter(RiskAlert.resolved == resolved)
    if severity:
        query = query.filter(RiskAlert.severity == severity)

    alerts = query.order_by(RiskAlert.created_at.desc()).limit(limit).all()
    data = [
        {
            "alert_id": a.alert_id,
            "supplier_id": a.supplier_id,
            "alert_type": a.alert_type,
            "severity": a.severity,
            "description": a.description,
            "ai_analysis": a.ai_analysis,
            "created_at": str(a.created_at),
            "resolved": a.resolved
        }
        for a in alerts
    ]

    set_cached(cache_key, data, redis, ttl=60)
    return {"source": "database", "count": len(data), "data": data}

@app.get("/alerts/critical")
def get_critical_alerts(
    db: Session = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    alerts = db.query(RiskAlert).filter(
        RiskAlert.severity == "CRITICAL",
        RiskAlert.resolved == False
    ).order_by(RiskAlert.created_at.desc()).all()

    return {
        "count": len(alerts),
        "data": [
            {
                "alert_id": a.alert_id,
                "supplier_id": a.supplier_id,
                "severity": a.severity,
                "description": a.description,
                "ai_analysis": a.ai_analysis,
                "created_at": str(a.created_at)
            }
            for a in alerts
        ]
    }

# ==================== WEATHER ====================

@app.get("/weather")
def get_weather_events(
    severity: str = None,
    db: Session = Depends(get_db),
    redis: redis.Redis = Depends(get_redis),
    api_key: str = Depends(get_api_key)
):
    cache_key = f"weather:{severity}"
    cached = get_cached(cache_key, redis)
    if cached:
        return {"source": "cache", "data": cached}

    query = db.query(WeatherEvent)
    if severity:
        query = query.filter(WeatherEvent.severity == severity)

    events = query.order_by(WeatherEvent.created_at.desc()).limit(20).all()
    data = [
        {
            "event_id": e.event_id,
            "location": e.location,
            "country": e.country,
            "event_type": e.event_type,
            "severity": e.severity,
            "description": e.description,
            "created_at": str(e.created_at)
        }
        for e in events
    ]

    set_cached(cache_key, data, redis, ttl=120)
    return {"source": "database", "count": len(data), "data": data}

# ==================== DASHBOARD SUMMARY ====================

@app.get("/dashboard/summary")
def get_dashboard_summary(
    db: Session = Depends(get_db),
    redis: redis.Redis = Depends(get_redis),
    api_key: str = Depends(get_api_key)
):
    cache_key = "dashboard:summary"
    cached = get_cached(cache_key, redis)
    if cached:
        return {"source": "cache", "data": cached}

    # Count key metrics
    total_suppliers = db.query(Supplier).filter(Supplier.active == True).count()
    total_orders = db.query(Order).count()
    delayed_orders = db.query(Order).filter(Order.status == "DELAYED").count()
    active_alerts = db.query(RiskAlert).filter(RiskAlert.resolved == False).count()
    critical_alerts = db.query(RiskAlert).filter(
        RiskAlert.severity == "CRITICAL",
        RiskAlert.resolved == False
    ).count()

    data = {
        "total_suppliers": total_suppliers,
        "total_orders": total_orders,
        "delayed_orders": delayed_orders,
        "delay_rate": round(delayed_orders / total_orders * 100, 1) if total_orders > 0 else 0,
        "active_alerts": active_alerts,
        "critical_alerts": critical_alerts
    }

    set_cached(cache_key, data, redis, ttl=60)
    return {"source": "database", "data": data}

# Schema context for Text-to-SQL
SCHEMA_CONTEXT = """
Tables:
- suppliers(supplier_id, name, country, region, reliability_score, contract_value, active)
- orders(order_id, supplier_id, product_id, quantity, status, delay_days, order_date, expected_delivery, actual_delivery, unit_price)
- risk_alerts(alert_id, supplier_id, alert_type, severity, description, ai_analysis, created_at, resolved)
- weather_events(event_id, location, country, event_type, severity, description, created_at)

Common values:
- orders.status: DELIVERED, DELAYED, IN_TRANSIT, PROCESSING, PENDING
- risk_alerts.severity: LOW, MEDIUM, HIGH, CRITICAL
- weather_events.severity: NORMAL, MEDIUM, HIGH, CRITICAL
"""

    # class QueryRequest(BaseModel):
    #     question: str

@app.post("/query")
def natural_language_query(
    request: QueryRequest,
    db: Session = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    # Generate SQL from natural language
    sql_query = text_to_sql(request.question, SCHEMA_CONTEXT)

    if sql_query.startswith("Error"):
        raise HTTPException(status_code=500, detail=sql_query)

    try:
        result = db.execute(text(sql_query))
        rows = result.fetchall()
        columns = result.keys()

        data = [dict(zip(columns, row)) for row in rows]

        return {
            "question": request.question,
            "sql_generated": sql_query,
            "results": data,
            "row_count": len(data)
        }

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"SQL execution error: {str(e)}. Generated SQL: {sql_query}"
        )