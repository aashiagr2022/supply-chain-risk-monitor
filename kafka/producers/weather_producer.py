#This fetches REAL weather data from OpenWeather every 60 seconds and only publishes when conditions are significant.
import json
import time
import uuid
import os
import sys
import requests
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

load_dotenv()

WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Primary cities for each supplier region
REGION_CITIES = {
    "Asia Pacific": {"city": "Shanghai", "country": "CN", "suppliers": ["AsiaTech Components", "JapanTech Ltd"]},
    "Western Europe": {"city": "Frankfurt", "country": "DE", "suppliers": ["EuroMfg GmbH", "UKComponents"]},
    "North America": {"city": "Chicago", "country": "US", "suppliers": ["AmeriParts Co", "MexicoMfg"]},
    "South Asia": {"city": "Mumbai", "country": "IN", "suppliers": ["IndoPrecision"]},
    "Latin America": {"city": "Sao Paulo", "country": "BR", "suppliers": ["BrazilFab"]}
}

def create_producer():
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8')
            )
            print("Weather producer connected to Kafka")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready — retrying ({i+1}/5)")
            time.sleep(5)
    raise Exception("Could not connect to Kafka")

def fetch_weather(city: str, country: str) -> dict:
    try:
        url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": f"{city},{country}",
            "appid": WEATHER_API_KEY,
            "units": "metric"
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Weather API error for {city}: {e}")
        return None

def assess_severity(weather_data: dict) -> tuple:
    if not weather_data:
        return "UNKNOWN", "Could not fetch weather data"

    condition = weather_data.get("weather", [{}])[0].get("main", "Clear")
    description = weather_data.get("weather", [{}])[0].get("description", "")
    wind_speed = weather_data.get("wind", {}).get("speed", 0)
    temp = weather_data.get("main", {}).get("temp", 20)
    visibility = weather_data.get("visibility", 10000)

    if condition in ["Thunderstorm", "Tornado"]:
        return "CRITICAL", f"Severe storm: {description} — major disruption expected"
    elif condition == "Snow" and temp < -10:
        return "CRITICAL", f"Extreme winter conditions: {description} — operations halted"
    elif condition in ["Rain", "Snow"] and wind_speed > 15:
        return "HIGH", f"Heavy {condition.lower()} with strong winds ({wind_speed}m/s) — significant delays"
    elif wind_speed > 20:
        return "HIGH", f"Dangerous wind speeds: {wind_speed}m/s — port and air operations affected"
    elif visibility < 1000:
        return "HIGH", f"Very low visibility: {visibility}m — transport disruptions"
    elif condition in ["Rain", "Snow", "Drizzle"]:
        return "MEDIUM", f"Weather disruption: {description} — minor delays possible"
    elif wind_speed > 10:
        return "MEDIUM", f"Moderate winds: {wind_speed}m/s — some delays possible"
    else:
        return "NORMAL", f"Clear conditions: {description}"

def run_weather_producer():
    producer = create_producer()

    print("\n🌤️  Weather Producer started")
    print("Fetching real weather data every 60 seconds...")
    print("Only publishing MEDIUM severity or above events to Kafka")
    print("-" * 60)

    try:
        while True:
            for region, info in REGION_CITIES.items():
                city = info["city"]
                country = info["country"]
                suppliers = info["suppliers"]

                weather_data = fetch_weather(city, country)
                severity, description = assess_severity(weather_data)

                temp = weather_data.get("main", {}).get("temp", "N/A") if weather_data else "N/A"
                wind = weather_data.get("wind", {}).get("speed", 0) if weather_data else 0

                status_icon = "🔴" if severity in ["CRITICAL", "HIGH"] else "🟡" if severity == "MEDIUM" else "🟢"
                print(f"{status_icon} {region} ({city}): {description} | "
                      f"Temp: {temp}°C | Wind: {wind}m/s | Severity: {severity}")

                # Only publish to Kafka if weather is significant
                if severity in ["CRITICAL", "HIGH", "MEDIUM"]:
                    event = {
                        "event_id": str(uuid.uuid4()),
                        "event_type": "WEATHER_ALERT",
                        "timestamp": datetime.now().isoformat(),
                        "region": region,
                        "city": city,
                        "country": country,
                        "affected_suppliers": suppliers,
                        "weather_condition": weather_data.get("weather", [{}])[0].get("main", "Unknown") if weather_data else "Unknown",
                        "description": description,
                        "severity": severity,
                        "temperature_celsius": temp,
                        "wind_speed_ms": wind,
                        "supply_chain_impact": f"Suppliers in {region} may experience delays: {', '.join(suppliers)}"
                    }

                    producer.send(
                        topic='weather-events',
                        key=region,
                        value=event
                    )
                    print(f"  📡 Published weather alert to Kafka for {region}")

            print(f"\n⏰ Next weather check in 60 seconds...\n")
            time.sleep(60)

    except KeyboardInterrupt:
        print("\nWeather producer stopped")
    finally:
        producer.close()

if __name__ == "__main__":
    run_weather_producer()