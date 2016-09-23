package com.qubole.streamx.s3;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import java.util.Map;
import java.util.HashMap;

public class ByteArrayConverter implements Converter {
    private final ByteArraySerializer serializer = new ByteArraySerializer();
    private final ByteArrayDeserializer deserializer = new ByteArrayDeserializer();

    public ByteArrayConverter() {
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        HashMap serializerConfigs = new HashMap();
        serializerConfigs.putAll(configs);
        HashMap deserializerConfigs = new HashMap();
        deserializerConfigs.putAll(configs);
        Object encodingValue = configs.get("converter.encoding");
        if (encodingValue != null) {
            serializerConfigs.put("serializer.encoding", encodingValue);
            deserializerConfigs.put("deserializer.encoding", encodingValue);
        }

        this.serializer.configure(serializerConfigs, isKey);
        this.deserializer.configure(deserializerConfigs, isKey);
    }

    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            return this.serializer.serialize(topic, value == null ? null : (byte[]) value);
        } catch (SerializationException var5) {
            throw new DataException("Failed to serialize to a string: ", var5);
        }
    }

    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, this.deserializer.deserialize(topic, value));
        } catch (SerializationException var4) {
            throw new DataException("Failed to deserialize byte: ", var4);
        }
    }
}
