package com.bettercloud.kadmin.kafka;

import com.bettercloud.kadmin.api.kafka.KafkaProviderService;
import com.bettercloud.messaging.kafka.produce.ProducerService;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Properties;

/**
 * Created by davidesposito on 6/28/16.
 */
@Service
public class DefaultKafkaProviderService implements KafkaProviderService {

    private final Joiner keyJoiner = Joiner.on("<:=:>");

    @Value("${kafka.schema.registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;
    @Value("${kafka.schema.host.url:localhost:9092}")
    private String bootstrapServers;

    private final Map<String, ProducerService<String, Object>> cache = Maps.newHashMap();

    protected Properties producerProperties(String schemaRegistryUrl, String kafkaUrl) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return properties;
    }

    private String getCacheKey(String schemaRegistryUrl, String kafkaUrl) {
        return keyJoiner.join(schemaRegistryUrl, kafkaUrl);
    }

    public ProducerService<String, Object> producerService() {
        return producerService(bootstrapServers);
    }

    public ProducerService<String, Object> producerService(String kafkaUrl) {
        return producerService(schemaRegistryUrl, kafkaUrl);
    }

    public ProducerService<String, Object> producerService(String schemaRegistryUrl, String kafkaUrl) {
        if (kafkaUrl == null) {
            kafkaUrl = bootstrapServers;
        }
        if (schemaRegistryUrl == null) {
            schemaRegistryUrl = this.schemaRegistryUrl;
        }
        String key = getCacheKey(schemaRegistryUrl, kafkaUrl);
        if (!cache.containsKey(key)) {
            Properties props = producerProperties(schemaRegistryUrl, kafkaUrl);
            cache.put(key, producerService(props));
        }
        return cache.get(key);
    }

    protected ProducerService<String, Object> producerService(Properties producerProperties) {
        return ProducerService.<String, Object>create()
                .withProducerProperties(producerProperties)
                .withKeySerializer(new StringSerializer())
                .withMessageSerializer(new KafkaAvroSerializer())
                .build();
    }
}
