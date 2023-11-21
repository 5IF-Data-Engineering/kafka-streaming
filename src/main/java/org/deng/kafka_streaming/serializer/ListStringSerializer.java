package org.deng.kafka_streaming.serializer;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

public class ListStringSerializer implements Serializer<List<String>> {
    @Override
    public void configure(Map configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, List<String> data) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            // Serialize the list to bytes
            objectOutputStream.writeObject(data);

            // Get the bytes of the serialized object
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
