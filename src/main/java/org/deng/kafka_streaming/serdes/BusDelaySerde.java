package org.deng.kafka_streaming.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.deng.kafka_streaming.deserializer.BusDelayDeserializer;
import org.deng.kafka_streaming.model.BusDelay;
import org.deng.kafka_streaming.serializer.BusDelaySerializer;

import java.util.List;

public class BusDelaySerde implements Serde<BusDelay> {
    BusDelaySerializer busDelaySerializer = new BusDelaySerializer();
    BusDelayDeserializer busDelayDeserializer = new BusDelayDeserializer();

    @Override
    public Serializer<BusDelay> serializer() {
        return busDelaySerializer;
    }

    @Override
    public Deserializer<BusDelay> deserializer() {
        return busDelayDeserializer;
    }
}
