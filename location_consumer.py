# Consumer
from kafka import KafkaConsumer
from flask import Flask, jsonify
from datetime import datetime
import json
import threading
import logging
import time
import signal
import sys

# Configure logging
logging.basicConfig(
    filename='location_consumer.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Flask app
app = Flask(__name__)

latest_batch = None  # Hold latest batche of driver locations
batch_lock = threading.Lock()
stop_event = threading.Event()

# Signal handler to gracefully stop the consumer
def signal_handler(sig, frame):
    """Handle termination signals to stop the consumer gracefully."""
    logging.info("Signal received, stopping consumer...")
    stop_event.set()
    sys.exit(0)

def stream_driver_locations(bootstrap_servers='140.119.164.16:9092', topic_name='driver-locations', group_id='location-consumer'):
    """Consume events from Kafka topic"""

    global latest_batch

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
        while not stop_event.is_set():
            batch = []
            raw_messages = consumer.poll(timeout_ms=1000)  # Poll for new messages with timeout stop event

            for _, messages in raw_messages.items():
                for message in messages:
                    for driver in message.value:
                        batch.append({
                            'id': driver['driver_id'],
                            'longitude': driver['data']['longitude'],
                            'latitude': driver['data']['latitude'],
                            'location': driver['data']['location']
                        })
            
            if batch:
                with batch_lock:
                    latest_batch = batch  # Update the latest batch

    except Exception as e:
        logging.info(f"Error consuming messages: {e}")
    except KeyboardInterrupt:
        logging.info("Consumer interrupted by user")
    finally:
        consumer.close()
        logging.info("Consumer closed")

@app.route('/driver-locations-batch', methods=['GET'])
def get_driver_locations_batch():
    """Endpoint to get a batch of driver locations"""
    with batch_lock:
        return jsonify(latest_batch if latest_batch is not None else [])

if __name__ == "__main__":
    try:
        # Start consumer thread
        consumer_thread = threading.Thread(target=stream_driver_locations)
        consumer_thread.daemon = True  # Auto-kill when main thread exits
        consumer_thread.start()

        # Start Flask app
        app.run(port=5000)

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Keep main thread alive
        while True:
            time.sleep(3600)  # Sleep 1 hour
    except KeyboardInterrupt:
        stop_event.set()
        consumer_thread.join()
