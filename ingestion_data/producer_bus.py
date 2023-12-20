# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


from kafka import KafkaProducer
import logging
import argparse
import pandas as pd
import datetime
from pandas import Timestamp
import json

# Set up command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--year', help='Year to ingest', type=int, required=False,
                    default=2017, choices=[2017, 2018, 2019, 2020, 2021, 2022])
parser.add_argument('--month', help='Month to ingest', type=int, required=False,
                    default=1, choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
args = parser.parse_args()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingestion_bus_data")


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


def extract_bus_data(year, month):
    """
    Extract bus data from local disk.
    :param year: Year to extract
    :param month: Month to extract
    :return: Pandas DataFrame
    """
    logger.info(f"Extracting bus data for {year}-{month}")
    df = pd.read_excel(f'./bus_data/ttc-bus-delay-data-{year}.xlsx', sheet_name='Sheet1', engine='openpyxl',
                       converters={'Day': str, 'Location': str, 'Incident': str, 'Direction': str})
    return df


def clean_bus_data(df: pd.DataFrame):
    """
    Remove the rows with missing location and incident values
    :param df: pandas dataframe
    :return: pandas dataframe
    """
    logger.info("Cleaning data ...")
    df = df[df['Location'].notna()]
    df = df[df['Incident'].notna()]
    return df


def transform_bus_data(data: pd.DataFrame):
    """
    Transform data from ingestion_weather.pipeline.extract.extract_data
    :param data: pd.DataFrame
    :return: pd.DataFrame
    """
    data.rename(columns={'Date': 'date', 'Route': 'route', 'Time': 'time', 'Day': 'day_of_week', 'Location': 'location',
                         'Incident': 'incident',
                         'Min Delay': 'min_delay', 'Min Gap': 'min_gap', 'Direction': 'direction',
                         'Vehicle': 'vehicle'}, inplace=True)
    if "Time Tmp" in data.columns:
        data.drop(columns=['Time Tmp'], inplace=True)
    # Change data types

    data['year'] = data['date'].apply(lambda x: int(x.year))
    data['month'] = data['date'].apply(lambda x: int(x.month))
    data['day'] = data['date'].apply(lambda x: int(x.day))
    data['day_of_week'] = data['day_of_week'].apply(lambda x: str(x))
    data['location'] = data['location'].apply(lambda x: str(x))
    data['incident'] = data['incident'].apply(lambda x: str(x))
    if isinstance(data['time'][0], datetime.time):
        data['time'] = data['time'].apply(lambda x: x.strftime('%H:%M:%S'))
    elif isinstance(data['time'][0], Timestamp):
        data['time'] = data['time'].apply(lambda x: x.strftime('%H:%M:%S'))
    data['hour'] = data['time'].apply(lambda x: int(x[:2]))
    data['minute'] = data['time'].apply(lambda x: int(x[3:5]))
    data['second'] = data['time'].apply(lambda x: int(x[6:8]))
    # Add timestamp column
    data['timestamp'] = data.apply(lambda x: pd.Timestamp(year=x['year'], month=x['month'], day=x['day'],
                                                          hour=x['hour'], minute=x['minute'], second=x['second']),
                                   axis=1)
    data['timestamp'] = data['timestamp'].apply(lambda x: int(x.timestamp()))
    # Drop date and time columns
    data.drop(columns=['date', 'time', 'minute', 'second'], inplace=True)
    return data


def load_bus_data(df: pd.DataFrame, year: int, month: int):
    """
    Load data to Kafka
    :param df: pandas dataframe
    :param year: Year to load
    :param month: Month to load
    :return: df
    """
    logger.info(f"Loading data for {year}-{month}")
    # Filter data for year and month
    df = df[(df['year'] == year) & (df['month'] == month)]
    return df


if __name__ == '__main__':
    # Extract
    bus_df = extract_bus_data(args.year, args.month)
    # Transform
    bus_df = transform_bus_data(bus_df)
    # Load
    bus_df = load_bus_data(bus_df, args.year, args.month)
    print("Data types: ", bus_df.dtypes)
    # Convert to json
    records = bus_df.to_dict(orient='records')
    # Create Kafka producer
    producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                             value_serializer=json_serializer)
    # Send data to Kafka
    for record in records:
        future = producer.send('ingestion_bus_data', value=record)
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
    # Flush producer
    producer.flush()
    # Close producer
    producer.close()
    logger.info("Data ingestion complete")
