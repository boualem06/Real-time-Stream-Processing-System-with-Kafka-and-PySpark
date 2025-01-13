import random
import time
import json
from datetime import datetime
from geopy.distance import geodesic

import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import SerializingProducer



nb_vehiculs=10
paris_center = (48.8566, 2.3522)


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def generate_random_location():
    base_location = paris_center  
    random_offset = random.uniform(-0.05, 0.05)
    new_location = (base_location[0] + random_offset, base_location[1] + random_offset)
    return new_location



# Function to generate random vehicle data
def generate_vehicle_data(i):
    timestamp = datetime.now().isoformat()  # Timestamp in ISO format
    speed = random.uniform(0, 120)  # Speed in km/h
    location = generate_random_location()
    fuel_level = random.uniform(0, 100)  # Fuel level percentage
    engine_temp = random.uniform(70, 110)  # Engine temperature in Celsius
    battery_voltage = random.uniform(12, 14)  # Battery voltage in volts
    mileage = random.uniform(10000, 100000)  # Total distance traveled in kilometers
    oil_level = random.uniform(50, 100)  # Oil level percentage
    driving_mode = random.choice(["Normal", "Sport", "Eco"])
    engine_status = random.choice([True, False])  # Engine on or off
    vehicle_type = random.choice(["Car", "Truck", "SUV"])

       # Create a dictionary with the generated data
    data = {
        "vehicle_id":i,
        "timestamp": timestamp,
        "speed": round(speed, 2),
        "location": location,
        "fuel_level": round(fuel_level, 2),
        "engine_temp": round(engine_temp, 2),
        "battery_voltage": round(battery_voltage, 2),
        "mileage": round(mileage, 2),
        "oil_level": round(oil_level, 2),
        "driving_mode": driving_mode,
        "engine_status": engine_status,
        "vehicle_type": vehicle_type,
    }
    return data


def create_table(cursor,conn):
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vehicle_telemetry(
        id SERIAL PRIMARY KEY,
        vehicle_id FLOAT ,
        timestamp TIMESTAMP,
        speed FLOAT,
        latitude FLOAT,
        longitude FLOAT,
        fuel_level FLOAT,
        engine_temp FLOAT,
        battery_voltage FLOAT,
        mileage FLOAT,
        oil_level FLOAT,
        driving_mode VARCHAR(20),
        engine_status BOOLEAN,
        vehicle_type VARCHAR(20)
    ) ;
    """)
    conn.commit()

def insert_value(cursor,conn,data):
    cursor.execute("""
        INSERT INTO vehicle_telemetry
        (
        vehicle_id,
        timestamp,
        speed ,
        latitude ,
        longitude ,
        fuel_level ,
        engine_temp ,
        battery_voltage ,
        mileage ,
        oil_level ,
        driving_mode ,
        engine_status ,
        vehicle_type )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            data['vehicle_id'] ,data['timestamp'], data['speed'], 
            data['location'][0], data['location'][1], data['fuel_level'], 
            data['engine_temp'], 
            data['battery_voltage'], data['mileage'], 
            data['oil_level'], data['driving_mode'], 
            data['engine_status'], data['vehicle_type']
        )
        )
    conn.commit()




if __name__ == "__main__":

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="telemetry",
        user="admin",
        password="admin",
        host="localhost",
        port=5432
    )

    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })
    cursor = conn.cursor()



    # create the database of vehicle_telemetry 
    create_table(cursor,conn)

    #insert the data into the vehicle_telemetry database 
    for i in range(nb_vehiculs):
        data=generate_vehicle_data(i+1)
        insert_value(cursor,conn,data)
        producer.produce(
            "vehicles_topic",
            key= f'{i+1}',
            value=json.dumps(data),
            on_delivery=delivery_report
        )
        print(f' iserted the {i} value ')
        producer.flush()
    cursor.close()
    conn.close()

    

    