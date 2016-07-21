package com.bettercloud.kadmin;

import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import com.bettercloud.kadmin.api.services.SerializerRegistryService;
import com.bettercloud.kadmin.kafka.avro.ErrorTolerantAvroObjectDeserializer;
import com.bettercloud.kadmin.kafka.serializers.DefaultSerializerRegistryService;
import com.google.common.collect.Maps;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
        DefaultSerializerRegistryService registry = new DefaultSerializerRegistryService();

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
}
