package org.deng.kafka_streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.deng.kafka_streaming.model.BusDelay;
import org.deng.kafka_streaming.producer.BusIngestionProducer;
import org.deng.kafka_streaming.serializer.BusDelaySerializer;
import org.deng.kafka_streaming.utils.ExtractExcel;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class IngestionMain {
    private static final String INPUT_TOPIC = "ingestion-bus-delay";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static BusIngestionProducer createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "prod-one-time");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BusDelaySerializer.class);
        KafkaProducer<String, BusDelay> producer = new KafkaProducer<>(props);
        return new BusIngestionProducer(producer);
    }

    public static void main(String[] args) {
        BusIngestionProducer producer = createProducer();
        producer.initTransaction();
        try {
            producer.beginTransaction();
            URL resource = Main.class.getResource("/");
            assert resource != null;
            String path = resource.getPath() + "ttc-bus-delay-data-2017.xlsx";
            Map<Integer, BusDelay> busData = ExtractExcel.read_excel(path);
            Stream.of(busData.keySet().toArray()).forEach(
                    (key) -> {
                        BusDelay value = busData.get(key);
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
        producer.close();
    }
}
