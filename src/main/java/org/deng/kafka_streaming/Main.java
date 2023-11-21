package org.deng.kafka_streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.deng.kafka_streaming.utils.ExtractExcel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final String INPUT_TOPIC = "ingestion-bus-delay-2017";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws IOException {
//        for (int i = 0; i < data.size(); i++) {
//            String timestampEpochDay = data.get(i).get(0);
//            LocalDate date = LocalDate.ofEpochDay(Long.parseLong(timestampEpochDay));
//            System.out.println(date);
//            String hour = data.get(i).get(2);
//            System.out.println(hour);
//        }

        LOGGER.info("Connecting to Kafka cluster via bootstrap servers {}", DEFAULT_BOOTSTRAP_SERVERS);

        final StreamsBuilder builder = new StreamsBuilder();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "consoleproducer");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KStream<String, String> inputData = builder.stream(INPUT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));
        inputData.print(Printed.toSysOut());


        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
