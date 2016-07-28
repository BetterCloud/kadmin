package com.bettercloud.kadmin;

import com.bettercloud.kadmin.api.models.DeserializerInfoModel;
import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import com.bettercloud.kadmin.api.services.DeserializerRegistryService;
import com.bettercloud.kadmin.api.services.SerializerRegistryService;
import com.bettercloud.kadmin.kafka.avro.ErrorTolerantAvroObjectDeserializer;
import com.bettercloud.kadmin.services.KafkaDeserializerRegistryService;
import com.bettercloud.kadmin.services.KafkaSerializerRegistryService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.kafka.common.serialization.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Function;

/**
 * Created by davidesposito on 7/21/16.
 */
@Configuration
public class AppConfiguration {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String JSON_REPLACE;

    static {
        String temp = "[";
        for (int i=0;i<=20;i++) {
            temp += (char)i;
        }
        JSON_REPLACE = temp + "]";
    }

    @Bean
    public SerializerRegistryService serializerRegistryService() {
        SerializerRegistryService registry = new KafkaSerializerRegistryService();

        registry.register(sim("string", StringSerializer.class, s -> s));
        registry.register(sim("bytes", ByteArraySerializer.class, s -> s));
        registry.register(sim("int", IntegerSerializer.class, s -> Integer.parseInt(s)));
        registry.register(sim("long", LongSerializer.class, s -> Long.parseLong(s)));

        return registry;
    }

    private SerializerInfoModel sim(String id, Class<?> serializerClass, Function<String, Object> rawPrep) {
        return sim(id, serializerClass.getSimpleName(), serializerClass, rawPrep);
    }

    private SerializerInfoModel sim(String id, String name, Class<?> serializerClass, Function<String, Object> rawPrep) {
        return SerializerInfoModel.builder()
                .id(UUID.randomUUID().toString())
                .name(name)
                .className(serializerClass.getName())
                .meta(Maps.newHashMap())
                .prepareRawFunc(rawPrep)
                .build();
    }

    @Bean
    public DeserializerRegistryService deserializerRegistryService() {
        DeserializerRegistryService registry = new KafkaDeserializerRegistryService();

        registry.register(dim("Avro Object Deserializer", ErrorTolerantAvroObjectDeserializer.class, "avro", (o) -> toNode(o + "")));
        registry.register(dim(StringDeserializer.class, "string", (o) -> toNode(w(o + ""))));
        registry.register(dim(ByteArrayDeserializer.class, "bytes", (o) -> toNode(w(o + ""))));
        registry.register(dim(IntegerDeserializer.class, "int", (o) -> toNode(o + "")));
        registry.register(dim(LongDeserializer.class, "long", (o) -> toNode(o + "")));

        return registry;
    }

    private String w(String s) {
        return s == null ? "null" : "\"" + s + "\"";
    }

    private JsonNode toNode(String s) {
        try {
            return mapper.readTree(s.replace("\n", "\\n")
                    .replaceAll(JSON_REPLACE, ""));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private DeserializerInfoModel dim(Class<?> deserializerClass, String id, Function<Object, JsonNode> prepOutput) {
        return dim(deserializerClass.getSimpleName(), deserializerClass, id, prepOutput);
    }

    private DeserializerInfoModel dim(String name, Class<?> deserializerClass, String id, Function<Object, JsonNode> prepOutput) {
        return DeserializerInfoModel.builder()
                .id(id)
                .name(name)
                .className(deserializerClass.getName())
                .prepareOutputFunc(prepOutput)
                .build();
    }
}
