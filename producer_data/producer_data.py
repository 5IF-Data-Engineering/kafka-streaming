# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


from kafka import KafkaProducer
import logging
import pandas as pd
import json
import os
from time import sleep

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingestion_data")


# Callback for successful message send
def on_send_success(record_metadata):
    logger.info(
        f"Message sent to {record_metadata.topic} on partition {record_metadata.partition} with offset {record_metadata.offset}")


# Callback for message send failure (e.g., KafkaError)
def on_send_error(excp):
    logger.error('Error sending message', exc_info=excp)


# Serializer to convert to bytes for transmission
def json_serializer(data):
    return json.dumps(data).encode("utf-8")


if __name__ == '__main__':
    path_data = os.path.join(os.path.dirname(__file__), 'data/all_data.parquet')
    # Read data
    df = pd.read_parquet(path_data)
    # Convert timestamp to integer
    df['timestamp'] = df['timestamp'].apply(lambda x: int(x.timestamp()))
    # Convert to json
    records = df.to_dict(orient='records')
    # Create Kafka producer
    producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                             value_serializer=json_serializer)
    # Send data to Kafka
    for record in records:
        future = producer.send('ingestion_data', value=record)
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
        # sleep(0.5)
    # Flush producer
    producer.flush()
    # Close producer
    producer.close()
    logger.info("Data ingestion complete")
