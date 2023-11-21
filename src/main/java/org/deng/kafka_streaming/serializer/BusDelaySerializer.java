package org.deng.kafka_streaming.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.deng.kafka_streaming.model.BusDelay;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BusDelaySerializer implements Serializer<BusDelay> {
    @Override
    public void configure(Map configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, BusDelay data) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            // Serialize the list to bytes
            List<String> serializedList = new ArrayList<>();
            serializedList.add(String.valueOf(data.getYear()));
            serializedList.add(String.valueOf(data.getMonth()));
            serializedList.add(String.valueOf(data.getDayOfMonth()));
            serializedList.add(data.getDayType());
            serializedList.add(String.valueOf(data.getHour()));
            serializedList.add(String.valueOf(data.getRoute()));
            serializedList.add(data.getDay());
            serializedList.add(data.getLocation());
            serializedList.add(data.getIncident());
            serializedList.add(String.valueOf(data.getDelay()));
            serializedList.add(String.valueOf(data.getGap()));
            serializedList.add(data.getDirection());
            serializedList.add(String.valueOf(data.getVehicle()));
            objectOutputStream.writeObject(serializedList);

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
