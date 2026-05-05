#Setup to fetch the data from weather API

import requests
import uuid
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.database import SessionLocal
from api.models import WeatherEvent

load_dotenv()

WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# Map supplier regions to major cities for weather lookup
REGION_CITIES = {
    "Asia Pacific": ["Shanghai", "Beijing", "Tokyo"],
    "Western Europe": ["Berlin", "Frankfurt", "London"],
    "North America": ["Chicago", "Detroit", "Houston"],
    "South Asia": ["Mumbai", "Delhi", "Bangalore"],
    "Latin America": ["Sao Paulo", "Mexico City", "Buenos Aires"]
}

def get_weather(city: str) -> dict:
    try:
        params = {
            "q": city,
            "appid": WEATHER_API_KEY,
            "units": "metric"
        }
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Weather API error for {city}: {e}")
        return None

def assess_weather_severity(weather_data: dict) -> tuple:
    if not weather_data:
        return "NORMAL", "No weather data available"

    weather_main = weather_data.get("weather", [{}])[0]
    condition = weather_main.get("main", "Clear")
    description = weather_main.get("description", "clear sky")
    wind_speed = weather_data.get("wind", {}).get("speed", 0)
    temp = weather_data.get("main", {}).get("temp", 20)

    # Assess severity based on conditions
    if condition in ["Thunderstorm", "Tornado", "Hurricane"]:
        return "CRITICAL", f"Severe weather: {description}"
    elif condition in ["Snow", "Blizzard"] and temp < -5:
        return "HIGH", f"Severe winter conditions: {description}"
    elif condition == "Rain" and wind_speed > 15:
        return "HIGH", f"Heavy rain with strong winds: {description}"
    elif condition in ["Rain", "Drizzle", "Snow"]:
        return "MEDIUM", f"Weather disruption: {description}"
    elif wind_speed > 20:
        return "HIGH", f"Strong winds: {wind_speed}m/s"
    else:
        return "NORMAL", f"Normal conditions: {description}"

def fetch_and_store_weather(db):
    print("\n🌤️  Fetching weather data for all supplier regions...")

    for region, cities in REGION_CITIES.items():
        city = cities[0]  # Use primary city for each region
        weather_data = get_weather(city)

        if not weather_data:
            continue

        severity, description = assess_weather_severity(weather_data)
        country = weather_data.get("sys", {}).get("country", "")

        weather_event = WeatherEvent(
            event_id=str(uuid.uuid4()),
            location=city,
            country=country,
            event_type=weather_data.get("weather", [{}])[0].get("main", "Clear"),
            severity=severity,
            description=description
        )

        db.add(weather_event)

        status_icon = "🔴" if severity == "CRITICAL" else "🟡" if severity in ["HIGH", "MEDIUM"] else "🟢"
        print(f"  {status_icon} {region} ({city}): {description} — Severity: {severity}")

    db.commit()
    print("Weather data stored successfully")

if __name__ == "__main__":
    db = SessionLocal()
    fetch_and_store_weather(db)
    db.close()