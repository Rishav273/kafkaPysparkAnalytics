from kafka import KafkaProducer
import csv
import os
import json
import time
import yaml
from lib.utils import list_files, json_serializer, load_config

# read config file
config = load_config()

if __name__ == '__main__':
    # Set up the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer
    )

    # List of CSV files to send to the Kafka topic
    csv_files = list_files(
        path=config['INPUT_DIR'],
        excluded=config['EXCLUDED']
    )

    # Iterate through the CSV files and send each row to the Kafka topic
    for csv_file in csv_files:

        with open(os.path.join(f"{config['INPUT_DIR']}/", csv_file), 'r') as f:
            reader = csv.DictReader(f, delimiter='|')
            rows = [row for row in reader]

        for row in rows:
            print(row)
            producer.send('transactions', value=row)
            time.sleep(1)

    # Flush the producer to ensure all messages are sent
    producer.flush()
