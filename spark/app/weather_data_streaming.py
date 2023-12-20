from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Spark Session
spark = SparkSession.builder.appName("StreamingWeatherData") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Schema for the input data
schema = StructType([
    StructField("temperature_2m", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("snowfall", DoubleType(), True),
    StructField("windspeed_10m", DoubleType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("hour", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read from Kafka
topic = "ingestion_weather_data"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the value as string
df_string = df.selectExpr("CAST(value AS STRING)")

# Deserialize the JSON data
df_json = df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")

windowed_data = df_json \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "15 days"),
    ) \
    .count()

# Prepare Data for Writing
output_data = windowed_data.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("count")
)

output_data \
    .writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()

# Write the result to PostgreSQL
# jdbc_url = "jdbc:postgresql://postgres:5432/streaming"
# jdbc_properties = {
#     "user": "postgres",
#     "password": "",
#     "driver": "org.postgresql.Driver"
# }

# query_window = output_data.writeStream \
#     .foreachBatch(lambda batch_df, batch_id: batch_df.write.jdbc(jdbc_url, "weather", 
#                                                                  mode="append", properties=jdbc_properties)) \
#     .outputMode("update") \
#     .start()

# query_window.awaitTermination()

# Stop Spark Session
spark.stop()
