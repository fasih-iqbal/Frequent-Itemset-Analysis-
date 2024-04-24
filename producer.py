import json
import logging
from kafka import KafkaProducer
from time import sleep
import sys
import signal

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_kafka_producer(servers):
    return KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def stream_preprocessed_data(input_file, producer, topic, delay=1):
    def signal_handler(sig, frame):
        logging.info("Shutdown signal received. Closing producer.")
        producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    with open(input_file, 'r') as file:
        for line in file:
            try:
                message = json.loads(line.strip())
                producer.send(topic, message)
                producer.flush()
                logging.info(f"Sent: {message}")
                sleep(delay)
            except json.JSONDecodeError:
                logging.error("Failed to decode JSON.")
            except Exception as e:
                logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    input_file = 'pp.json'  # Path to the preprocessed data file
    topic_name = 'bda3'
    kafka_servers = ['localhost:9092']  # Could be parameterized
    delay_seconds = 1  # Could be parameterized

    producer = create_kafka_producer(kafka_servers)
    stream_preprocessed_data(input_file, producer, topic_name, delay_seconds)

