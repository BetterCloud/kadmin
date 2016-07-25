package com.bettercloud.kadmin.kafka;

import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import com.bettercloud.kadmin.kafka.avro.ErrorTolerantAvroObjectDeserializer;
import com.google.common.collect.Maps;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.UUID;
import java.util.function.Function;

/**
 * Created by davidesposito on 7/23/16.
 */
public class SerializersProperties {


    public static final SerializerInfoModel STRING = sim("string", StringSerializer.class, s -> s);
    public static final SerializerInfoModel BYTES = sim("bytes", ByteArraySerializer.class, s -> s);
    public static final SerializerInfoModel INTEGER = sim("int", IntegerSerializer.class, s -> Integer.parseInt(s));
    public static final SerializerInfoModel LONG = sim("long", LongSerializer.class, s -> Long.parseLong(s));
    public static final SerializerInfoModel AVRO = sim("avro", ErrorTolerantAvroObjectDeserializer.class, s -> Long.parseLong(s));

    private static SerializerInfoModel sim(String id, Class<?> serializerClass, Function<String, Object> rawPrep) {
        return sim(id, serializerClass.getSimpleName(), serializerClass, rawPrep);
    }

    private static SerializerInfoModel sim(String id, String name, Class<?> serializerClass, Function<String, Object> rawPrep) {
        return SerializerInfoModel.builder()
                .id(UUID.randomUUID().toString())
                .name(name)
                .className(serializerClass.getName())
                .meta(Maps.newHashMap())
                .prepareRawFunc(rawPrep)
                .build();
    }
}
