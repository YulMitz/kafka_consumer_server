from kafka import KafkaProducer
from logging.handlers import RotatingFileHandler
import json
import random
import time
import datetime
import logging
import threading
import sys
import signal
import requests

# Configure logging
handler = RotatingFileHandler('response_handler.log', maxBytes=100*1024*1024, backupCount=3)
logging.basicConfig(
    handlers=[handler],
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

stop_event = threading.Event()

# Signal handler to gracefully stop the consumer
def signal_handler(sig, frame):
    """Handle termination signals to stop the consumer gracefully."""
    logging.info("Signal received, stopping consumer...")
    stop_event.set()
    sys.exit(0)

def produce_responses(bootstrap_servers='140.119.164.16:9092', topic_name='response'):
    """Produce responses to kafka topic"""
    # Create kafka producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"Starting to produce events to topic '{topic_name}'...")
    logging.info(f"Starting to produce events to topic '{topic_name}'...")

    try:
        count = 0
        error_count = 0
        response_url = 'http://localhost:5100/responses'
        while not stop_event.is_set():
            try:
                response = requests.get(response_url, timeout=2)
                data = response.json()
            except requests.exceptions.RequestException as e:
                error_count += 1
                print(f"Error fetching response data: {e}")
                logging.error("Failed to fetch traffic data batch.")

                if error_count > 30:
                    print("Too many errors, stopping producer.")
                    logging.error("Too many errors fetching response data, stopping producer.")
                    break
            
            if data and len(data) > 0:
                for item in data:
                    item['timestamp'] = datetime.datetime.now().isoformat()
                    producer.send(topic_name, value=item)
                    count += 1

            if count % 100 == 0:
                producer.flush() # Ensure records are sent
                print(f"Produced {count} events. Latest: {data[-1] if data else 'No data'}")
                logging.info(f"Produced {count} events. Latest: {data[-1] if data else 'No data'}")

            # Sleep to control rate
            time.sleep(0.1)
        
    except KeyboardInterrupt:
        print("Event production stopped manually")
    finally:
        producer.flush()
        producer.close()
        print(f"Producer closed. Total events produced: {count}")
    
if __name__ == "__main__":
    try:
        producer_thread = threading.Thread(target=produce_responses, daemon=True)
        producer_thread.start()

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        while not stop_event.is_set():
            time.sleep(5)

    except KeyboardInterrupt:
        print("Response handler stopped manually")
        logging.info("Shutting down producer threads...")
        stop_event.set()
        producer_thread.join()