from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, IntegerType, DoubleType
import sys

# Get the date from the command line
number_file = sys.argv[1]

# Spark Session
spark = SparkSession.builder.appName("WriteDebsDataKafka") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.dfs.replication", "1") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Schema for the input data
schema = StructType([
    StructField("medallion", StringType(), True),
    StructField("hack_license", StringType(), True),
    StructField("vendor_id", StringType(), True),
    StructField("rate_code", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_time_in_secs", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True)
])

# Read from CSV
debs_data = spark.read.csv("hdfs://namenode:8020/output/trip_data_{}.csv".format(number_file), 
                           header=True, schema=schema)

# Write to Kafka
topic = "debs_data"
debs_data.selectExpr("CAST(medallion AS STRING) AS key", "to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", topic) \
    .save()

# Stop Spark Session
spark.stop()