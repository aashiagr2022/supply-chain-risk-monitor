import os
import sys
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.database import SessionLocal
from api.models import Supplier, Order, RiskAlert, WeatherEvent

load_dotenv()

def retrieve_supplier_context(supplier_id: str, db: Session) -> dict:
    """
    RAG Step 1 — Retrieve all relevant context for a supplier
    before sending to the LLM. This is what makes the AI analysis
    specific and actionable rather than generic.
    """

    # Get supplier details
    supplier = db.query(Supplier).filter(
        Supplier.supplier_id == supplier_id
    ).first()

    if not supplier:
        return {}

    # Get last 30 days of orders for this supplier
    thirty_days_ago = datetime.now() - timedelta(days=30)
    recent_orders = db.query(Order).filter(
        Order.supplier_id == supplier_id,
        Order.order_date >= thirty_days_ago
    ).all()

    # Calculate performance metrics from recent orders
    total_orders = len(recent_orders)
    delayed_orders = [o for o in recent_orders if o.delay_days and o.delay_days > 0]
    on_time_orders = total_orders - len(delayed_orders)
    on_time_rate = (on_time_orders / total_orders * 100) if total_orders > 0 else 0
    avg_delay = sum(o.delay_days for o in delayed_orders if o.delay_days) / len(delayed_orders) if delayed_orders else 0

    # Get historical alerts for this supplier
    past_alerts = db.query(RiskAlert).filter(
        RiskAlert.supplier_id == supplier_id,
        RiskAlert.resolved == False
    ).order_by(RiskAlert.created_at.desc()).limit(5).all()

    # Get current weather in supplier region
    recent_weather = db.query(WeatherEvent).filter(
        WeatherEvent.country == supplier.country
    ).order_by(WeatherEvent.created_at.desc()).first()

    # Get available backup suppliers in same region
    backup_suppliers = db.query(Supplier).filter(
        Supplier.region == supplier.region,
        Supplier.supplier_id != supplier_id,
        Supplier.reliability_score >= 8.0,
        Supplier.active == True
    ).order_by(Supplier.reliability_score.desc()).limit(3).all()

    # Build rich context package
    context = {
        "supplier": {
            "name": supplier.name,
            "country": supplier.country,
            "region": supplier.region,
            "reliability_score": supplier.reliability_score,
            "contract_value": supplier.contract_value
        },
        "recent_performance": {
            "total_orders_30_days": total_orders,
            "delayed_orders": len(delayed_orders),
            "on_time_rate_pct": round(on_time_rate, 1),
            "avg_delay_days": round(avg_delay, 1),
            "orders_in_transit": len([o for o in recent_orders if o.status == "IN_TRANSIT"])
        },
        "historical_alerts": [
            {
                "severity": a.severity,
                "description": a.description,
                "date": str(a.created_at)
            }
            for a in past_alerts
        ],
        "current_weather": {
            "location": recent_weather.location if recent_weather else "Unknown",
            "severity": recent_weather.severity if recent_weather else "NORMAL",
            "description": recent_weather.description if recent_weather else "No weather data"
        } if recent_weather else None,
        "backup_suppliers": [
            {
                "name": b.name,
                "country": b.country,
                "reliability_score": b.reliability_score
            }
            for b in backup_suppliers
        ]
    }

    return context


def format_context_for_prompt(context: dict, current_metrics: dict) -> str:
    """
    RAG Step 2 — Format the retrieved context into a
    structured prompt section that the LLM can reason over.
    """

    if not context:
        return "No historical context available."

    supplier = context.get("supplier", {})
    performance = context.get("recent_performance", {})
    weather = context.get("current_weather")
    backups = context.get("backup_suppliers", [])
    past_alerts = context.get("historical_alerts", [])

    prompt_context = f"""
SUPPLIER PROFILE:
- Name: {supplier.get('name', 'Unknown')}
- Country: {supplier.get('country', 'Unknown')}
- Region: {supplier.get('region', 'Unknown')}
- Reliability Score: {supplier.get('reliability_score', 'N/A')}/10
- Contract Value: ${supplier.get('contract_value', 0):,.0f}/year

CURRENT PERFORMANCE ALERT:
- On-time delivery rate: {current_metrics.get('on_time_delivery_rate', 0)*100:.0f}%
- Defect rate: {current_metrics.get('defect_rate', 0)*100:.0f}%
- Average delay: {current_metrics.get('avg_delay_days', 0)} days

HISTORICAL CONTEXT (Last 30 days):
- Total orders placed: {performance.get('total_orders_30_days', 0)}
- Orders delayed: {performance.get('delayed_orders', 0)}
- Historical on-time rate: {performance.get('on_time_rate_pct', 0)}%
- Average delay when late: {performance.get('avg_delay_days', 0)} days
- Orders currently in transit: {performance.get('orders_in_transit', 0)}

PAST ALERTS:
{chr(10).join([f"- {a['severity']}: {a['description']} ({a['date'][:10]})" for a in past_alerts]) if past_alerts else "- No previous alerts"}

CURRENT WEATHER IN {supplier.get('country', 'region').upper()}:
{f"- {weather['description']} (Severity: {weather['severity']})" if weather else "- No weather data available"}

AVAILABLE BACKUP SUPPLIERS IN {supplier.get('region', 'region').upper()}:
{chr(10).join([f"- {b['name']} ({b['country']}) — reliability {b['reliability_score']}/10" for b in backups]) if backups else "- No backup suppliers identified"}
"""

    return prompt_context.strip()