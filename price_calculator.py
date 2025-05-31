import json
import time
import requests
import math
import logging
from enum import Enum

# Configure logging
logging.basicConfig(
    filename='price_calculator.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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
    
    if client_data is None:
        raise ValueError("Client data must be provided.")

    # Get traffic data
    traffic_url = 'http://localhost:5050/traffic-batch'
    traffic_data = {}
    try:
        response = requests.get(traffic_url, timeout=0.5)
        traffic_data = response.json()
        # print(f"Latest traffic data batch: {traffic_data}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch traffic data batch: {e}")

    # Get current driver locations
    driver_location_url = 'http://localhost:5000/driver-locations-batch'
    try:
        response = requests.get(driver_location_url, timeout=0.5)
        latest_batch = response.json()
        # print(f"Latest driver locations batch: {latest_batch}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch driver locations batch: {e}")

    # Calculate distance between client and drivers
    for driver in latest_batch:
        # Get distance from client to driver
        distance = haversine_distance(
            client_data['pickup_latitude'],
            client_data['pickup_longitude'],
            driver['latitude'],
            driver['longitude']
        )

        driver['distance'] = distance

    valid_drivers = sorted(latest_batch, key=lambda x: x['distance'])[:10]
    # print(f"Valid drivers: {valid_drivers}")

    for entry in valid_drivers:
        entry.setdefault('price_field', {})
        entry['price_field']['base_price'] = base_price + entry['distance'] * 2

    # Calculate distance between pickup and dropoff locations
    travel_distance = haversine_distance(
        client_data['pickup_latitude'],
        client_data['pickup_longitude'],
        client_data['dropoff_latitude'],
        client_data['dropoff_longitude']
    )
    # print(f"Travel distance: {travel_distance} km")

    travel_base_price = 0  # Initialize travel base price
    if travel_distance > 1.5:
        travel_base_price = (travel_distance - 1.5) * 10

    # Calculate traffic multiplier by client and driver's region traffic status
    for driver in valid_drivers:
        # Get traffic multiplier
        driver['traffic_multiplier'] = get_traffic_multiplier(
            traffic_data=traffic_data, 
            client_data=client_data,
            driver_location=driver['location'])

    # Get weather data
    weather_url = 'http://localhost:5050/weather-batch'
    weather_data = {}
    try:
        response = requests.get(weather_url, timeout=0.5)
        weather_data = response.json()
        # print(f"Latest weather data batch: {weather_data}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch weather data batch: {e}")

    weather_condition_multiplier = get_weather_multiplier(weather_data)

    # Calculate the final price for each driver
    all_available_prices = []
    for driver in valid_drivers:
        driver['price_field']['total_base_price'] = base_price + driver['price_field']['base_price'] + travel_base_price
        driver['price_field']['final_price'] = driver['price_field']['total_base_price'] * weather_condition_multiplier * driver['traffic_multiplier']
        all_available_prices.append(driver['price_field']['final_price'])
        logging.info(f"calculated price: {driver['price_field']['final_price']}, with total base price: {driver['price_field']['total_base_price']}")
        logging.info(f"Weather condition multiplier: {weather_condition_multiplier}, Traffic multiplier: {driver['traffic_multiplier']}")

    logging.info(f"All available prices: {all_available_prices}")
    return valid_drivers

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

def get_weather_multiplier(weather_data=None, default_multiplier=1.0):
    if weather_data is {}:
        logging.warning("No weather data provided, using default multiplier.")
        return default_multiplier

    if weather_data and (weather_data['weather_condition'] in [item.value for item in FeaturedWeatherCondition]):
        weather_condition = weather_data['weather_condition']
    else:
        logging.warning("No featured weather condition or missing weather condition, using default multiplier.")
        return default_multiplier

    if weather_condition in HEAVY_RAIN_OR_ABOVE:
        # print(f"Weather condition is heavy rain or above: {weather_condition}")
        # Apply a 50% surcharge for heavy rain or above
        return 1.5
    else:
        # print(f"Weather condition is not heavy rain or above: {weather_condition}")
        return 1.2

def get_traffic_multiplier(traffic_data=None, client_data=None, driver_location=None, default_multiplier=1.0):
    if traffic_data is {}:
        logging.warning("No traffic data provided, using default multiplier.")
        return default_multiplier

    client_region = client_data['pickup_zone']
    driver_region = driver_location

    if client_region == driver_region:
        # print(f"Client and driver are in the same region: {client_region}")
        count = 0
        for entry in traffic_data:
            if client_region in entry['region'][3:]:
                status = entry['traffic_status']
                count += 1
                match status:
                    case "順暢":
                        return 1.0
                    case "中等":
                        return 1.1
                    case "擁塞":
                        return 1.2
                    case _:
                        logging.warning(f"Unknown traffic status: {status}, using default multiplier.")
                        return default_multiplier
        if count == 0:
            logging.warning(f"Region {client_region} not found in traffic data, using default multiplier.")
            return default_multiplier
    else:
        # print(f"Client and driver are in different regions: {client_region}, {driver_region}")
        client_count = 0
        driver_count = 0
        for entry in traffic_data:
            if client_region == entry['region'][3:]:
                status = entry['traffic_status']
                # print(f"Client region {client_region} traffic status: {status}")
                client_count += 1
                match status:
                    case "順暢":
                        client_side = 1.1
                    case "中等":
                        client_side = 1.2
                    case "擁塞":
                        client_side = 1.3
                    case _:
                        logging.warning(f"Unknown traffic status: {status}, using default multiplier.")
                        client_side = 1.0
            if driver_region == entry['region'][3:]:
                status = entry['traffic_status']
                # print(f"Driver region: {driver_region}, Traffic status: {status}")
                driver_count += 1
                match status:
                    case "順暢":
                        driver_side = 1.1
                    case "中等":
                        driver_side = 1.2
                    case "擁塞":
                        driver_side = 1.3
                    case _:
                        logging.warning(f"Unknown traffic status: {status}, using default multiplier.")
                        driver_side = 1.0
        if client_count == 0 or driver_count == 0:
            logging.warning(f"Client region {client_region} or driver region {driver_region} not found in traffic data, using default multiplier.")
            return default_multiplier
        
        # print(f"Client traffic multiplier: {client_side}, Driver traffic multiplier: {driver_side}")
        return client_side * driver_side
        
if __name__ == "__main__":
    calculate_price()