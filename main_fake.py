import asyncio
import random
import json
from faker import Faker
from kafka import KafkaProducer
# from openai import OpenAI
import time


# Faker instance for generating random data
fake = Faker()

def generate_random_property_details():
    """Generate random property details to mimic real estate listings."""
    print("Generating random property details...")
    
    return {
        "price": f"£{random.randint(200000, 1000000):,}",
        "address": fake.address(),
        "bedrooms": random.randint(1, 5),
        "bathrooms": random.randint(1, 3),
        "receptions": random.randint(1, 2),
        "EPC_Rating": random.choice(["A", "B", "C", "D", "E", "F"]),
        "tenure": random.choice(["Freehold", "Leasehold"]),
        "time_remaining_on_lease": f"{random.randint(50, 999)} years",
        "service_charge": f"£{random.randint(500, 3000)} per year",
        "council_tax_band": random.choice(["A", "B", "C", "D", "E"]),
        "ground_rent": f"£{random.randint(50, 500)} per year"
    }

def generate_random_pictures():
    """Generate random picture URLs for property images."""
    print("Generating random pictures...")
    return [f"https://example.com/property_image_{i}.webp" for i in range(1, random.randint(2, 6))]

def generate_random_floor_plan():
    """Generate a random floor plan URL."""
    print("Generating random floor plan...")
    return {"floor_plan": f"https://example.com/floor_plan_{random.randint(1, 10)}.jpg"}

async def run(producer):
    print('Generating random property data...')
    properties = []

    # Generate random listings and send each to Kafka
    for _ in range(random.randint(1, 500)):  # Adjust number of listings if needed
        data = {
            "address": fake.address(),
            "title": fake.catch_phrase(),
            "link": f"https://example.com/property_{random.randint(1000, 9999)}",
            "pictures": generate_random_pictures(),
            **generate_random_floor_plan(),
            **generate_random_property_details()
        }
        
        # Log the generated data
        print("Generated property data:", json.dumps(data, indent=2))
        properties.append(data)
        
        # Send data to Kafka topic
        print("Sending data to Kafka...")
        producer.send('properties', value=data)
        time.sleep(2)
        
    return properties

async def main():
    # Kafka producer instance
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',  # Update with your Kafka server
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await run(producer)

if __name__ == '__main__':
    asyncio.run(main())
