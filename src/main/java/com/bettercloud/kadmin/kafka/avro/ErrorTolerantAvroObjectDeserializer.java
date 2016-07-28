package com.bettercloud.kadmin.kafka.avro;

import com.bettercloud.util.LoggerUtils;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;

/**
 * Created by davidesposito on 7/20/16.
 */
public class ErrorTolerantAvroObjectDeserializer extends KafkaAvroDeserializer {

    private static final Logger LOGGER = LoggerUtils.get(ErrorTolerantAvroObjectDeserializer.class);

    protected Object deserialize(byte[] payload) throws SerializationException {
        try {
            return super.deserialize(payload);
        } catch (SerializationException e) {
            LOGGER.warn("There was an error deserializing avro payload: {}", e.getMessage());
        }
        return "!!!there was an error!!!";
    }
}
