package org.deng.kafka_streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.deng.kafka_streaming.model.BusDelay;
import org.deng.kafka_streaming.serdes.BusDelaySerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final String INPUT_TOPIC = "ingestion-bus-delay";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        LOGGER.info("Connecting to Kafka cluster via bootstrap servers {}", DEFAULT_BOOTSTRAP_SERVERS);
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "bus-delay-streaming");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStream<String, BusDelay> busDelayKStream = builder.stream(INPUT_TOPIC,
                Consumed.with(Serdes.String(), new BusDelaySerde()));

//        busDelayKStream.print(Printed.<String, BusDelay>toSysOut().withLabel("bus-delay-input"));

        // group by year
        // group by year, month, dayType, hour, location, incident
        // tumbling window of 1 seconds
        TimeWindowedKStream<String, BusDelay> timeWindowedKStream = busDelayKStream
                .groupBy((key, value) -> String.valueOf(value.getYear()),
                        Grouped.with(Serdes.String(), new BusDelaySerde()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(1)));

        KTable<Windowed<String>, Long> countByWindow = timeWindowedKStream.count();

        countByWindow.toStream().print(Printed.<Windowed<String>, Long>toSysOut().withLabel("bus-delay-count"));

        Topology topology = builder.build();

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
