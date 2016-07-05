package com.bettercloud.kadmin.kafka;

import com.bettercloud.kadmin.api.kafka.KafkaProviderService;
import com.bettercloud.messaging.kafka.consume.ConsumerGroup;
import com.bettercloud.messaging.kafka.consume.MessageHandler;
import com.bettercloud.messaging.kafka.produce.ProducerService;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

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

    private final Map<String, ProducerService<String, Object>> producerMap = Maps.newHashMap();
    private final Map<String, ConsumerGroup<String, Object>> consumerMap = Maps.newHashMap();

    protected Properties producerProperties(String schemaRegistryUrl, String kafkaUrl) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return properties;
    }

    private String getProducerKey(String schemaRegistryUrl, String kafkaUrl) {
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
        String key = getProducerKey(schemaRegistryUrl, kafkaUrl);
        if (!producerMap.containsKey(key)) {
            Properties props = producerProperties(schemaRegistryUrl, kafkaUrl);
            producerMap.put(key, producerService(props));
        }
        return producerMap.get(key);
    }

    protected ProducerService<String, Object> producerService(Properties producerProperties) {
        return ProducerService.<String, Object>create()
                .withProducerProperties(producerProperties)
                .withKeySerializer(new StringSerializer())
                .withMessageSerializer(new KafkaAvroSerializer())
                .build();
    }

    /*
    ========================================================
                        CONSUMER
    ========================================================
     */

    private String getConsumerKey(String schemaRegistryUrl, String kafkaUrl, String topic) {
        return keyJoiner.join(schemaRegistryUrl, kafkaUrl, topic);
    }

    protected Properties kafkaConsumerProperties(String kafkaHost, String schemaRegistryUrl) throws UnknownHostException {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 32 * 1024);
        return properties;

    }

    @Override
    public ConsumerGroup<String, Object> consumerService(MessageHandler<String, Object> handler, String topic) {
        return consumerService(handler, topic, bootstrapServers);
    }

    @Override
    public ConsumerGroup<String, Object> consumerService(MessageHandler<String, Object> handler, String topic, String kafkaUrl) {
        return consumerService(handler, topic, kafkaUrl, schemaRegistryUrl);
    }

    @Override
    public ConsumerGroup<String, Object> consumerService(MessageHandler<String, Object> handler, String topic,
                                                         String schemaRegistryUrl, String kafkaUrl) {
        if (kafkaUrl == null) {
            kafkaUrl = bootstrapServers;
        }
        if (schemaRegistryUrl == null) {
            schemaRegistryUrl = this.schemaRegistryUrl;
        }
        String key = getConsumerKey(schemaRegistryUrl, kafkaUrl, topic);
        if (!consumerMap.containsKey(key)) {
            Properties props = null;
            try {
                props = kafkaConsumerProperties(schemaRegistryUrl, kafkaUrl);
                consumerMap.put(key, consumerService(props, topic, handler));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return consumerMap.get(key);
    }

    protected ConsumerGroup<String, Object> consumerService(Properties kafkaConsumerProperties, String topic,
                                                            MessageHandler<String, Object> handler) {
        return ConsumerGroup.<String, Object>create()
                .withConsumerProperties(kafkaConsumerProperties)
                .withMessageHandler(handler)
                .withKeyDeserializer(new StringDeserializer())
                .withMessageDeserializer(new KafkaAvroDeserializer())
                .forTopic(topic)
                .withPollTimeout(2000)
                .withThreads(1)
                .build();
    }
}
