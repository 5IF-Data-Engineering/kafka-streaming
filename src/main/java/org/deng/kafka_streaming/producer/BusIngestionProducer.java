package org.deng.kafka_streaming.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public class BusIngestionProducer {
    private KafkaProducer<String, List<String>> producer;

    public BusIngestionProducer(KafkaProducer<String, List<String>> producer) {
        this.producer = producer;
    }

    public void initTransaction() {
        producer.initTransactions();
    }

    public void beginTransaction() {
        producer.beginTransaction();
    }

    public void send(String topic, String key, List<String> value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    public void commitTransaction() {
        producer.commitTransaction();
    }

    public void abortTransaction() {
        producer.abortTransaction();
    }

    public void flush() {
        producer.flush();
    }
}
