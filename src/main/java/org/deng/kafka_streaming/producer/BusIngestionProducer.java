package org.deng.kafka_streaming.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.deng.kafka_streaming.model.BusDelay;

public class BusIngestionProducer {
    private KafkaProducer<String, BusDelay> producer;

    public BusIngestionProducer(KafkaProducer<String, BusDelay> producer) {
        this.producer = producer;
    }

    public void initTransaction() {
        producer.initTransactions();
    }

    public void beginTransaction() {
        producer.beginTransaction();
    }

    public void send(String topic, String key, BusDelay value) {
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
    public void close() {
        producer.close();
    }
}
