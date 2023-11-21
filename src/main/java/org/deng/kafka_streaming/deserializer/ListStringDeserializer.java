package org.deng.kafka_streaming.deserializer;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Map;

public class ListStringDeserializer implements Deserializer<List<String>> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public List<String> deserialize(String topic, byte[] data) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {

            // Deserialize the bytes back to a list
            List<String> deserializedList = (List<String>) objectInputStream.readObject();

            // Print the deserialized list
            System.out.println("Deserialized List: " + deserializedList);
            return deserializedList;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
