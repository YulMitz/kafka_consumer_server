import json
import time
import requests
import math
from enum import Enum

class FeaturedWeatherCondition(Enum):
    # Rain series
    LightRain = "小雨"
    ModerateRain = "中雨"
    HeavyRain = "大雨"
    VeryHeavyRain = "豪雨"
    ExtremeRain = "強烈豪雨"
    FreezingRain = "凍雨"
    LightShowerRain = "小陣雨"
    ShowerRain = "陣雨"
    HeavyShowerRain = "大陣雨"
    RaggedShowerRain = "不規則性陣雨"
    # Thunderstorm series
    ThunderstormLightRain = "有小雨的雷陣雨"
    ThunderstormRain = "有中雨的雷陣雨"
    ThunderstormHeavyRain = "有大雨的雷陣雨"
    LightThunderstorm = "小雷雨"
    Thunderstorm = "雷雨"
    HeavyThunderstorm = "強雷雨"
    RaggedThunderstorm = "不穩定雷雨"
    ThunderstormLightDrizzle = "有毛毛雨的雷陣雨"
    ThunderstormDrizzle = "有小雨的雷陣雨"
    ThunderstormHeavyDrizzle = "有大雨的雷陣雨"

HEAVY_RAIN_OR_ABOVE = {
    FeaturedWeatherCondition.HeavyRain.value,
    FeaturedWeatherCondition.VeryHeavyRain.value,
    FeaturedWeatherCondition.ExtremeRain.value,
    FeaturedWeatherCondition.FreezingRain.value,
    FeaturedWeatherCondition.HeavyShowerRain.value,
    FeaturedWeatherCondition.ThunderstormHeavyRain.value,
    FeaturedWeatherCondition.ThunderstormRain.value,
    FeaturedWeatherCondition.Thunderstorm.value,
    FeaturedWeatherCondition.HeavyThunderstorm.value,
    FeaturedWeatherCondition.ThunderstormDrizzle.value,
    FeaturedWeatherCondition.ThunderstormHeavyDrizzle.value
}

REGION = ["台北市北投區", "台北市士林區", "台北市中山區", "台北市大同區", "台北市萬華區", "台北市信義區", "台北市中正區", "台北市大安區", "台北市文山區", "台北市南港區", "台北市內湖區", "台北市松山區"]

def calculate_price(base_price=30, client_data=None):
    """
    Calculate the final price after applying tax and discount.
    """
    if base_price < 0:
        raise ValueError("Base price cannot be negative.")

    # Get traffic data
    traffic_url = 'http://localhost:5050/traffic-batch'
    try:
        response = requests.get(traffic_url, timeout=0.5)
        traffic_data = response.json()
        print(f"Latest traffic data batch: {traffic_data}")
    except requests.exceptions.RequestException as e:
        print("Failed to fetch traffic data batch.")

    # Get current driver locations
    driver_location_url = 'http://localhost:5000/driver-locations-batch'
    try:
        response = requests.get(driver_location_url, timeout=0.5)
        latest_batch = response.json()
        print(f"Latest driver locations batch: {latest_batch}")
    except requests.exceptions.RequestException as e:
        print("Failed to fetch driver locations batch.")

    # Calculate distance between client and drivers
    distances = []
    for driver in latest_batch:
        distance = haversine_distance(
            client_data['pickup_latitude'],
            client_data['pickup_longitude'],
            driver['latitude'],
            driver['longitude']
        )
        distances.append(distance)

    print(f"Distances from client to drivers: {distances}")
    valid_driver_distances = sorted(distances)[:10]  # Get the 10 closest drivers
    print(f"Valid driver distances: {valid_driver_distances}")

    driver_base_prices = [distance * 2 for distance in valid_driver_distances]

    # Calculate distance between pickup and dropoff locations
    travel_distance = haversine_distance(
        client_data['pickup_latitude'],
        client_data['pickup_longitude'],
        client_data['dropoff_latitude'],
        client_data['dropoff_longitude']
    )
    print(f"Travel distance: {travel_distance} km")

    travel_base_price = 0  # Initialize travel base price
    if travel_distance > 1.5:
        travel_base_price = (travel_distance - 1.5) * 10

    # Get weather data
    weather_url = 'http://localhost:5050/weather-batch'
    try:
        response = requests.get(weather_url, timeout=0.5)
        weather_data = response.json()
        print(f"Latest weather data batch: {weather_data}")
    except requests.exceptions.RequestException as e:
        print("Failed to fetch weather data batch.")

    weather_condition_multiplier = 1.0 # Default multiplier

    if weather_data and (weather_data['weather_condition'] in [item.value for item in FeaturedWeatherCondition]):
        weather_condition = weather_data['weather_condition']
        if weather_condition in HEAVY_RAIN_OR_ABOVE:
            print(f"Weather condition is heavy rain or above: {weather_condition}")
            # Apply a 50% surcharge for heavy rain or above
            weather_condition_multiplier = 1.5
        else:
            print(f"Weather condition is not heavy rain or above: {weather_condition}")
            weather_condition_multiplier = 1.2

    all_available_prices = []
    for driver_base_price in driver_base_prices:
        total_base_price = base_price + driver_base_price + travel_base_price
        price_per_driver = total_base_price * weather_condition_multiplier
        all_available_prices.append(price_per_driver)
        print(f"calculated price: {price_per_driver}, with total base price: {total_base_price}")

    return all_available_prices

def haversine_distance(lat1, lon1, lat2, lon2):
    # Earth radius in kilometers
    R = 6371.0

    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Differences in coordinates
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Haversine formula
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    raw_distance = R * c

    return raw_distance

if __name__ == "__main__":
    calculate_price()