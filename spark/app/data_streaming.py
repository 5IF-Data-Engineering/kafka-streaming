from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Spark Session
spark = SparkSession.builder.appName("StreamingData") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Schema for the input data
# delay, gap, direction, vehicle, temperature, humidity,
# precipitation, rain, snowfall, windspeed, incident,
# latitude, longitude, timestamp
schema = StructType([
    StructField("delay", DoubleType(), True),
    StructField("gap", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("vehicle", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("snowfall", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("incident", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read from Kafka
topic = "ingestion_data"

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

# Write the result into PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/streaming"
jdbc_properties = {
    "user": "postgres",
    "password": "",
    "driver": "org.postgresql.Driver"
}

query = df_json.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write.jdbc(jdbc_url, "data",
                                                                 mode="append", properties=jdbc_properties)) \
    .outputMode("append") \
    .start()

query.awaitTermination()

# Stop Spark Session
spark.stop()
