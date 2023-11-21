package org.deng.kafka_streaming.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.deng.kafka_streaming.deserializer.ListStringDeserializer;
import org.deng.kafka_streaming.serializer.ListStringSerializer;

import java.util.List;

public class ListStringSerde implements Serde<List<String>> {
    ListStringSerializer listStringSerializer = new ListStringSerializer();
    ListStringDeserializer listStringDeserializer = new ListStringDeserializer();

    @Override
    public Serializer<List<String>> serializer() {
        return listStringSerializer;
    }

    @Override
    public Deserializer<List<String>> deserializer() {
        return listStringDeserializer;
    }
}
