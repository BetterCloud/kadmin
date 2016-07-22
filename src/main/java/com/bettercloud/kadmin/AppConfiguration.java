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

    @Bean
    public SerializerRegistryService serializerRegistryService() {
        SerializerRegistryService registry = new KafkaSerializerRegistryService();

        registry.register(sim(StringSerializer.class, s -> s));
        registry.register(sim(ByteArraySerializer.class, s -> s));
        registry.register(sim(IntegerSerializer.class, s -> Integer.parseInt(s)));
        registry.register(sim(LongSerializer.class, s -> Long.parseLong(s)));

        return registry;
    }

    private SerializerInfoModel sim(Class<?> serializerClass, Function<String, Object> rawPrep) {
        return sim(serializerClass.getSimpleName(), serializerClass, rawPrep);
    }

    private SerializerInfoModel sim(String name, Class<?> serializerClass, Function<String, Object> rawPrep) {
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
                    .replace((char)0 + "", "")
                    .replace((char)1 + "", "")
                    .replace((char)10 + "", "")
                    .replace((char)20 + "", ""));
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
