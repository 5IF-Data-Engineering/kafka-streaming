from kafka import KafkaProducer
import logging
import argparse
import pandas as pd
import datetime
import json
import requests

# Set up command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--year', help='Year to ingest', type=int, required=False,
                    default=2017, choices=[2017, 2018, 2019, 2020, 2021, 2022])
args = parser.parse_args()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingestion_weather_data")


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


def extract_weather_data(year):
    """
    Extract weather data from local disk.
    :param year: Year to extract
    :return: dictionary
    """
    logger.info(f"Extracting weather data for {year}")
    link = "https://archive-api.open-meteo.com/v1/archive?"
    params = {
        "latitude": '43.653226',
        "longitude": '-79.383184',
        "start_date": "{}-01-01".format(year),
        "end_date": "{}-12-31".format(year),
        "hourly": "temperature_2m,precipitation,rain,snowfall,windspeed_10m",
        "timezone": "America/Toronto"
    }
    response = requests.get(link, params=params)
    data = json.loads(response.text)['hourly']
    return data


def transform_weather_data(data):
    """
    Transform data from json format to list of data in json format
    :param data:
    :return: list of data in json format
    """
    df = pd.DataFrame(data)
    df['year'] = df['time'].apply(lambda x: int(x[:4]))
    df['month'] = df['time'].apply(lambda x: int(x[5:7]))
    df['day'] = df['time'].apply(lambda x: int(x[8:10]))
    df['day_of_week'] = df['time'].apply(lambda x: datetime.datetime.strptime(x[:10], '%Y-%m-%d').strftime('%A'))
    df['hour'] = df['time'].apply(lambda x: int(x[11:13]))
    df.drop(columns=['time'], inplace=True)
    return df.to_dict(orient='records')


if __name__ == "__main__":
    # Extract data
    data = extract_weather_data(args.year)

    # Transform data
    records = transform_weather_data(data)

    # Set up Kafka producer
    producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                             value_serializer=json_serializer)
    # Send data
    for record in records:
        record['timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        producer.send('ingestion_weather_data', record).add_callback(on_send_success).add_errback(on_send_error)
    producer.flush()
    logger.info("Weather data ingestion complete")
