package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.kafka.KadminProducer;
import com.bettercloud.kadmin.api.kafka.KadminProducerConfig;
import com.bettercloud.kadmin.api.services.AvroProducerProviderService;
import com.bettercloud.kadmin.kafka.avro.DefaultAvroProducer;
import com.bettercloud.util.Opt;
import com.bettercloud.util.Page;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/20/16.
 */
@Service
public class DefaultAvroProducerProviderService implements AvroProducerProviderService {

    private final String defaultKafkaHost;
    private final String defaultSchemaRegistryUrl;

    private final LinkedHashMap<String, KadminProducer<String, Object>> producerMap;

    @Autowired
    public DefaultAvroProducerProviderService(
            @Value("${kafka.host:localhost:9092}")
                    String defaultKafkaHost,
            @Value("${schema.registry.url:http://localhost:8081}")
                    String defaultSchemaRegistryUrl,
            @Value("${kafka.consumer.threadpool.size:5}")
                    int consumerThreadPoolSize) {
        this.defaultKafkaHost = defaultKafkaHost;
        this.defaultSchemaRegistryUrl = defaultSchemaRegistryUrl;

        this.producerMap = Maps.newLinkedHashMap();
    }

    @Override
    public KadminProducer<String, Object> get(KadminProducerConfig config) {
        Opt.of(config.getKafkaHost()).notPresent(() -> config.setKafkaHost(defaultKafkaHost));
        Opt.of(config.getSchemaRegistryUrl()).notPresent(() -> config.setSchemaRegistryUrl(defaultSchemaRegistryUrl));

        DefaultAvroProducer producer = new DefaultAvroProducer(config);
        producerMap.put(producer.getId(), producer);
        return producer;
    }

    @Override
    public Page<KadminProducer<String, Object>> findAll() {
        return findAll(-1 ,- 1);
    }

    @Override
    public Page<KadminProducer<String, Object>> findAll(int page, int size) {
        if (page < 0) {
            page = 0;
        }
        if (size <= 0 || size > 100) {
            size = 20;
        }
        int skip = page * size;
        List<KadminProducer<String, Object>> consumers = producerMap.values().stream()
                .skip(skip)
                .limit(size)
                .collect(Collectors.toList());
        Page<KadminProducer<String, Object>> consumerPage = new Page<>();
        consumerPage.setPage(page);
        consumerPage.setSize(size);
        consumerPage.setTotalElements(count());
        consumerPage.setContent(consumers);
        return consumerPage;
    }

    @Override
    public KadminProducer<String, Object> findById(String consumerId) {
        return producerMap.get(consumerId);
    }

    @Override
    public long count() {
        return producerMap.size();
    }
}
