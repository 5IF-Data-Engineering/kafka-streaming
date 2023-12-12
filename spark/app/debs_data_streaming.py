from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Spark Session
spark = SparkSession.builder.appName("StreamingDebsData") \
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

# Read from Kafka
topic = "debs_data"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", topic) \
    .load()

# Convert the value as string
df_string = df.selectExpr("CAST(value AS STRING)")

# Deserialize the JSON data
df_json = df_string.select(from_json(col("value"), schema).alias("data")).select("data.*") \
    .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))

# Get the number of trips during the last 30 minutes
windowed_data = df_json \
    .withWatermark("pickup_datetime", "1 minute") \
    .groupBy(
        window(col("pickup_datetime"), "30 minutes")
    ) \
    .count()

output_data = windowed_data.select(
    col("window.start").alias("start"),
    col("window.end").alias("end"),
    col("count").alias("number_of_trips")
)

# Prepare Data for Writing
query = output_data.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

# Stop Spark Session
spark.stop()