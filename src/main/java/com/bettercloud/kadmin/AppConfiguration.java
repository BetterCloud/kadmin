package com.bettercloud.kadmin;

import com.bettercloud.kadmin.api.models.DeserializerInfoModel;
import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import com.bettercloud.kadmin.api.services.DeserializerRegistryService;
import com.bettercloud.kadmin.api.services.KadminConsumerGroupProviderService;
import com.bettercloud.kadmin.api.services.RegistryService;
import com.bettercloud.kadmin.api.services.SerializerRegistryService;
import com.bettercloud.kadmin.kafka.avro.ErrorTolerantAvroObjectDeserializer;
import com.bettercloud.kadmin.services.BasicKafkaConsumerProviderService;
import com.bettercloud.kadmin.services.KafkaDeserializerRegistryService;
import com.bettercloud.kadmin.services.KafkaSerializerRegistryService;
import com.bettercloud.kadmin.services.SimpleRegistryService;
import com.google.common.collect.Maps;
import org.apache.kafka.common.serialization.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

/**
 * Created by davidesposito on 7/21/16.
 */
@Configuration
public class AppConfiguration {

    @Bean
    public SerializerRegistryService serializerRegistryService() {
        SerializerRegistryService registry = new KafkaSerializerRegistryService();

        registry.register(sim(StringSerializer.class));
        registry.register(sim(ByteArraySerializer.class));
        registry.register(sim(IntegerSerializer.class));
        registry.register(sim(LongSerializer.class));

        return registry;
    }

    private SerializerInfoModel sim(Class<?> serializerClass) {
        return sim(serializerClass.getSimpleName(), serializerClass);
    }

    private SerializerInfoModel sim(String name, Class<?> serializerClass) {
        return SerializerInfoModel.builder()
                .id(UUID.randomUUID().toString())
                .name(name)
                .className(serializerClass.getName())
                .meta(Maps.newHashMap())
                .build();
    }

    @Bean
    public DeserializerRegistryService deserializerRegistryService() {
        DeserializerRegistryService registry = new KafkaDeserializerRegistryService();

        registry.register(dim("Avro Object Deserializer", ErrorTolerantAvroObjectDeserializer.class));
        registry.register(dim(StringDeserializer.class));
        registry.register(dim(ByteArrayDeserializer.class));
        registry.register(dim(IntegerDeserializer.class));
        registry.register(dim(LongDeserializer.class));

        return registry;
    }

    private DeserializerInfoModel dim(Class<?> deserializerClass) {
        return dim(deserializerClass.getSimpleName(), deserializerClass);
    }

    private DeserializerInfoModel dim(String name, Class<?> deserializerClass) {
        return DeserializerInfoModel.builder()
                .id(UUID.randomUUID().toString())
                .name(name)
                .className(deserializerClass.getName())
                .build();
    }
}
