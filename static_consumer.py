# Consumer
from kafka import KafkaConsumer
from datetime import datetime
from flask import Flask, jsonify
import time
import json
import logging
import threading
import sys
import signal

# Configure logging
logging.basicConfig(
    filename='static_consumer.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Flask app
app = Flask(__name__)

latest_traffic_batch = None  # Hold batches of traffic data and weather data
latest_weather_batch = None
batch_lock = threading.Lock()
stop_event = threading.Event()

# Signal handler to gracefully stop the consumer
def signal_handler(sig, frame):
    """Handle termination signals to stop the consumer gracefully."""
    logging.info("Signal received, stopping consumer...")
    stop_event.set()
    sys.exit(0)

def stream_traffic_data(bootstrap_servers='140.119.164.16:9092', topic_name='traffic-data', group_id='static-consumer'):
    """Consume events from Kafka topic"""

    global latest_traffic_batch

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

    try:
        count = 0
        while not stop_event.is_set():
            batch = []
            raw_messages = consumer.poll(timeout_ms=1000)

            if not raw_messages or len(raw_messages) == 0:
                # No new messages received, wait before polling again
                logging.info("No new messages received, waiting...")
                time.sleep(1)
                continue

            for _, messages in raw_messages.items():
                for message in messages:
                    count += 1
                    if count % 50 == 0:
                        logging.info(f"Processed {count} traffic messages so far...")
                        logging.info(f"Received: {message}")
                        logging.info("-" * 50)
                    for region in message.value:
                        traffic_data = {
                            'region': region['location'],
                            'traffic_status': region['data']['status'],
                        }
                        batch.append(traffic_data)
            if batch:
                with batch_lock:
                    latest_traffic_batch = batch
    finally:
        consumer.close()
        logging.info("Consumer closed")

def stream_weather_data(bootstrap_servers='140.119.164.16:9092', topic_name='weather-data', group_id='static-consumer'):
    """Consume events from Kafka topic"""

    global latest_weather_batch

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

    try:
        count = 0
        while not stop_event.is_set():
            batch = None
            raw_messages = consumer.poll(timeout_ms=1000)

            if not raw_messages or len(raw_messages) == 0:
                logging.info("No new messages received, waiting...")
                time.sleep(1)
                continue

            for _, messages in raw_messages.items():
                for message in messages:
                    count += 1
                    if count % 50 == 0:
                        logging.info(f"Processed {count} temperature messages so far...")
                        logging.info(f"Received: {message}")
                        logging.info("-" * 50)
                    temperature = message.value['data']['temperature']
                    humidity = message.value['data']['humidity']
                    weather_condition = message.value['data']['weather']

                    result = {
                        'temperature': temperature,
                        'humidity': humidity,
                        'weather_condition': weather_condition
                    }
                    batch = result

            if batch:
                with batch_lock:
                    latest_weather_batch = batch
            
    finally:
        consumer.close()
        logging.info("Consumer closed")
    
@app.route('/traffic-batch', methods=['GET'])
def get_traffic_batch():
    """Endpoint to get a batch of traffic data """
    with batch_lock:
        return jsonify(latest_traffic_batch if latest_traffic_batch is not None else {})

@app.route('/weather-batch', methods=['GET'])
def get_weather_batch():
    """Endpoint to get a batch of weather data """
    with batch_lock:
        return jsonify(latest_weather_batch if latest_weather_batch is not None else {})

if __name__ == "__main__":
    try:
        traffic_thread = threading.Thread(target=stream_traffic_data, daemon=True)
        weather_thread = threading.Thread(target=stream_weather_data, daemon=True)

        # Start both threads
        traffic_thread.start()
        weather_thread.start()
        
        # Run Flask app
        app.run(port=5050)

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Keep main thread alive
        while True:
            time.sleep(3600)  # Sleep 1 hour
    except KeyboardInterrupt:
            logging.info("Shutting down consumer threads...")
            stop_event.set()
            traffic_thread.join()
            weather_thread.join()