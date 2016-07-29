package com.bettercloud.kadmin.kafka.avro;

import com.bettercloud.util.LoggerUtils;
import com.bettercloud.util.Opt;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;

/**
 * Created by davidesposito on 7/20/16.
 */
public class ErrorTolerantAvroObjectDeserializer extends KafkaAvroDeserializer {

    private static final Logger LOGGER = LoggerUtils.get(ErrorTolerantAvroObjectDeserializer.class);

    protected Object deserialize(byte[] payload) throws SerializationException {
        String error;
        try {
            return super.deserialize(payload);
        } catch (SerializationException e) {
            LOGGER.warn("There was an error deserializing avro payload: {}, caused by: {}", e.getMessage(), e.getCause());
            error = String.format("\" \nThere was an error deserializing the avro payload. \n \nError: %s %s \n\"",
                    e.getMessage(), Opt.of(e.getCause()).map(cause -> "\n \nCause: " + cause.getMessage()).orElse(""));
        }
        return error;
    }
}
