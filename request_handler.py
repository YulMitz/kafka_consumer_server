# Consumer
from kafka import KafkaConsumer
from datetime import datetime
from price_calculator import calculate_price
import json

def handle_request(bootstrap_servers='140.119.164.16:9092', topic_name='ride-requests', group_id='request-consumer'):
    """Consume events from Kafka topic"""
    # Create Kafka consumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest', # Consuming from the latest record
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Starting to consume events from topic '{topic_name}'...")

    try:
        for message in consumer:
            print(f"Received: {message}")
            print("-" * 50)
            
            request_id = message.value['passenger_id']
            pick_up_longitude = message.value['data']['pickup']['lon']
            pick_up_latitude = message.value['data']['pickup']['lat']
            drop_off_longitude = message.value['data']['dropoff']['lon']
            drop_off_latitude = message.value['data']['dropoff']['lat']

            request_data = {
                'request_id': request_id,
                'pickup_longitude': pick_up_longitude,
                'pickup_latitude': pick_up_latitude,
                'dropoff_longitude': drop_off_longitude,
                'dropoff_latitude': drop_off_latitude
            }

            price = calculate_price(base_price=30, client_data=request_data)
            print(f"Calculated price: {price}")
            break
    finally:
        consumer.close()
        print("Consumer closed")

if __name__ == "__main__":
    handle_request()