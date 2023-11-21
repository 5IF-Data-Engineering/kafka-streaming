package org.deng.kafka_streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.deng.kafka_streaming.producer.BusIngestionProducer;
import org.deng.kafka_streaming.serdes.ListStringSerde;
import org.deng.kafka_streaming.serializer.ListStringSerializer;
import org.deng.kafka_streaming.utils.ExtractExcel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class IngestionMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionMain.class);
    private static final String INPUT_TOPIC = "ingestion-bus-delay-2017";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static BusIngestionProducer createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "prod-0");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ListStringSerializer.class);
        KafkaProducer<String, List<String>> producer = new KafkaProducer<>(props);
        return new BusIngestionProducer(producer);
    }

    public static void main(String[] args) {
        LOGGER.info("Connecting to Kafka cluster via bootstrap servers {}", DEFAULT_BOOTSTRAP_SERVERS);
        BusIngestionProducer producer = createProducer();
        producer.initTransaction();
        try {
            producer.beginTransaction();
            URL resource = Main.class.getResource("/");
            assert resource != null;
            String path = resource.getPath() + "ttc-bus-delay-data-2017.xlsx";
            Map<Integer, List<String>> busData = ExtractExcel.read_excel(path);
            Stream.of(busData.keySet().toArray()).forEach(
                    (key) -> {
                        List<String> value = busData.get(key);
                        producer.send(INPUT_TOPIC, key.toString(), value);
                    }
            );
            producer.commitTransaction();

        } catch (IOException | KafkaException e) {
            producer.abortTransaction();

        }
        finally {
            producer.flush();
        }

    }
}
