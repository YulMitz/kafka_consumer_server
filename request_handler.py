# Consumer
from kafka import KafkaConsumer
from datetime import datetime
from price_calculator import calculate_price
from flask import Flask, jsonify
from logging.handlers import RotatingFileHandler
import json
import threading
import logging
import time
import queue
import sys
import signal

# Configure logging
handler = RotatingFileHandler('request_handler.log', maxBytes=100*1024*1024, backupCount=3)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[handler]
)

# Thread-safe queue to hold responses
response_queue = queue.Queue()

# Flask app for handling requests
app = Flask(__name__)

batch_lock = threading.Lock()
stop_event = threading.Event()

# Signal handler to gracefully stop the consumer
def signal_handler(sig, frame):
    """Handle termination signals to stop the consumer gracefully."""
    logging.info("Signal received, stopping consumer...")
    stop_event.set()
    sys.exit(0)

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

    logging.info(f"Starting to consume events from topic '{topic_name}'...")
    print(f"Starting to consume events from topic '{topic_name}'...")

    try:
        count = 0
        while not stop_event.is_set():
            raw_messages = consumer.poll(timeout_ms=1000)

            for _, messages in raw_messages.items():
                for message in messages:
                    count += 1
                    if count % 100 == 0:
                        print(f"Processed {count} messages so far...")
                        print(f"Received: {message}")
                        logging.info("-" * 50)
                        logging.info(f"Processed {count} messages so far...")
                        logging.info(f"Received: {message}")
                        logging.info("-" * 50)

                    request_id = message.value['passenger_id']
                    pick_up_longitude = message.value['data']['pickup']['lon']
                    pick_up_latitude = message.value['data']['pickup']['lat']
                    drop_off_longitude = message.value['data']['dropoff']['lon']
                    drop_off_latitude = message.value['data']['dropoff']['lat']
                    pick_up_zone = message.value['data']['zone']

                    request_data = {
                        'request_id': request_id,
                        'pickup_longitude': pick_up_longitude,
                        'pickup_latitude': pick_up_latitude,
                        'dropoff_longitude': drop_off_longitude,
                        'dropoff_latitude': drop_off_latitude,
                        'pickup_zone': pick_up_zone
                    }

                    response = calculate_price(base_price=30, client_data=request_data)
                    if count % 100 == 0:
                        logging.info(f"Responded drivers: {response}")
            
                    # Put the response in the queue
                    response_queue.put({
                        'request_id': request_id,
                        'response': response
                    })
    finally:
        consumer.close()
        print("Consumer closed")

@app.route('/responses', methods=['GET'])
def get_responses():
    """Endpoint to get responses from the queue."""
    responses = []
    while not response_queue.empty():
        responses.append(response_queue.get())
    return jsonify(responses)

if __name__ == "__main__":
    try:
        consumer_thread = threading.Thread(target=handle_request, daemon=True)
        consumer_thread.start()
        app.run(port=5100, debug=False)

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    except KeyboardInterrupt:
        print("Request handler stopped manually")
        logging.info("Shutting down consumer threads...")
        stop_event.set()
        consumer_thread.join()