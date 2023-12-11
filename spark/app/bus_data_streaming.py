from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Spark Session
spark = SparkSession.builder.appName("StreamingBusDelay") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Schema for the input data
schema = StructType([
    StructField("route", IntegerType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("location", StringType(), True),
    StructField("incident", StringType(), True),
    StructField("min_delay", DoubleType(), True),
    StructField("min_gap", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("vehicle", DoubleType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True)
])

# Read from Kafka
topic = "ingestion_bus_data"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", topic) \
    .load()

# Convert the value as string
df_string = df.selectExpr("CAST(value AS STRING)")

# Deserialize the JSON data
df_json = df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Show the data in the console
query = df_json.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Aggregate the data
# delay_count = df_json.groupBy(col("year"), col("month")).count()

# Show the result
# query = delay_count.writeStream \
#     .format("console") \
#     .outputMode("complete") \
#     .start()

# Write the result into PostgreSQL
# jdbc_url = "jdbc:postgresql://postgres:5432/streaming"
# jdbc_properties = {
#     "user": "postgres",
#     "password": "",
#     "driver": "org.postgresql.Driver"
# }

# query = output_data.writeStream \
#     .foreachBatch(lambda batch_df, batch_id: batch_df.write.jdbc(jdbc_url, "bus_delay", 
#                                                                  mode="append", properties=jdbc_properties)) \
#     .outputMode("complete") \
#     .start()

query.awaitTermination()

# Stop Spark Session
spark.stop()