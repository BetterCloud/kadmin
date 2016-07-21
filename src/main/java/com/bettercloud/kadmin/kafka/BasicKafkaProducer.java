package com.bettercloud.kadmin.kafka;

import com.bettercloud.kadmin.api.kafka.KadminProducer;
import com.bettercloud.kadmin.api.kafka.KadminProducerConfig;
import com.bettercloud.kadmin.api.kafka.avro.AvroProducer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by davidesposito on 7/20/16.
 */
public class BasicKafkaProducer<ValueT> implements KadminProducer<String, ValueT> {

    private final KadminProducerConfig config;
    private final String id;
    private KafkaProducer<String, Object> producer;

    public BasicKafkaProducer(KadminProducerConfig config) {
        this.config = config;
        this.id = UUID.randomUUID().toString();

        init();
    }

    /**
     * Gets called before init(). Set any default configs here
     * @param config
     */
    protected void initConfig(KadminProducerConfig config) { }

    protected void init() {
        initConfig(config);

        final Properties properties = new Properties();
        properties.put("acks", "all");
        properties.put("bootstrap.servers", config.getKafkaHost());
        properties.put("schema.registry.url", config.getSchemaRegistryUrl());
        properties.put("key.serializer", config.getKeySerializer());
        properties.put("value.serializer", config.getValueSerializer());

        this.producer = new KafkaProducer<>(properties);
    }

    @Override
    public KadminProducerConfig getConfig() {
        return config;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void send(String key, ValueT val) {
        if (this.producer != null) {
            ProducerRecord<String, Object> pr = new ProducerRecord<>(config.getTopic(), key, val);
            producer.send(pr);
        }
    }

    @Override
    public void shutdown() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }
}
