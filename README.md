./bin/spark-submit --master local --driver-class-path ./jars/postgresql-42.2.23.jar --jars ./jars/postgresql-42.2.23.jar --name bus-streaming --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 ./app/bus_data_streaming.py

docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ingestion_bus_data --from-beginning

./bin/spark-submit --master local --driver-class-path ./jars/postgresql-42.2.23.jar --jars ./jars/postgresql-42.2.23.jar --name weather-streaming --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 ./app/weather_data_streaming.py

docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ingestion_weather_data --from-beginning