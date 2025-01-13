import random
import time
import json
from datetime import datetime
from geopy.distance import geodesic


paris_center = (48.8566, 2.3522)
def generate_random_location():
    base_location = paris_center  
    random_offset = random.uniform(-0.05, 0.05)
    new_location = (base_location[0] + random_offset, base_location[1] + random_offset)
    return new_location





# Function to generate random vehicle data
def generate_vehicle_data(vehicle_id):
    timestamp = datetime.now().isoformat()  # Timestamp in ISO format
    speed = random.uniform(0, 120)  # Speed in km/h
    location = generate_random_location()
    fuel_level = random.uniform(0, 100)  # Fuel level percentage
    engine_temp = random.uniform(70, 110)  # Engine temperature in Celsius
    tire_pressure = [random.uniform(30, 35) for _ in range(4)]  # Tire pressure for each tire
    battery_voltage = random.uniform(12, 14)  # Battery voltage in volts
    mileage = random.uniform(10000, 100000)  # Total distance traveled in kilometers
    oil_level = random.uniform(50, 100)  # Oil level percentage
    driving_mode = random.choice(["Normal", "Sport", "Eco"])
    engine_status = random.choice([True, False])  # Engine on or off
    vehicle_type = random.choice(["Car", "Truck", "SUV"])

       # Create a dictionary with the generated data
    data = {
        "vehicle_id": vehicle_id,
        "timestamp": timestamp,
        "speed": round(speed, 2),
        "location": location,
        "fuel_level": round(fuel_level, 2),
        "engine_temp": round(engine_temp, 2),
        "tire_pressure": tire_pressure,
        "battery_voltage": round(battery_voltage, 2),
        "mileage": round(mileage, 2),
        "oil_level": round(oil_level, 2),
        "driving_mode": driving_mode,
        "engine_status": engine_status,
        "vehicle_type": vehicle_type,
    }
    return data


if __name__ == "__main__":
    vehicle_ids = ["V123", "V124", "V125", "V126", "V127"]
    for vehicle_id in vehicle_ids:
        data=generate_vehicle_data(vehicle_id)
        print(json.dumps(data, indent=4))
