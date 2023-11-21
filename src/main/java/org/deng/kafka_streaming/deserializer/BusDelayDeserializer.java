package org.deng.kafka_streaming.deserializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.deng.kafka_streaming.model.BusDelay;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public class BusDelayDeserializer implements Deserializer<BusDelay> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public BusDelay deserialize(String topic, byte[] data) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {

            // Deserialize the bytes back to a list
            List<String> deserializedList = (List<String>) objectInputStream.readObject();

            BusDelay busDelay = new BusDelay();
            busDelay.setYear(Integer.parseInt(deserializedList.get(0)));
            busDelay.setMonth(Integer.parseInt(deserializedList.get(1)));
            busDelay.setDayOfMonth(Integer.parseInt(deserializedList.get(2)));
            busDelay.setDayType(deserializedList.get(3));
            busDelay.setHour(Integer.parseInt(deserializedList.get(4)));
            busDelay.setRoute(Integer.parseInt(deserializedList.get(5)));
            busDelay.setDay(deserializedList.get(6));
            busDelay.setLocation(deserializedList.get(7));
            busDelay.setIncident(deserializedList.get(8));
            busDelay.setDelay(Float.parseFloat(deserializedList.get(9)));
            busDelay.setGap(Float.parseFloat(deserializedList.get(10)));
            busDelay.setDirection(deserializedList.get(11));
            busDelay.setVehicle(Integer.parseInt(deserializedList.get(12)));

            return busDelay;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
