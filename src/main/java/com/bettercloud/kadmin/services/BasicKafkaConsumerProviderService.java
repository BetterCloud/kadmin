package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.kafka.KadminConsumerConfig;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroup;
import com.bettercloud.kadmin.api.kafka.avro.AvroConsumerGroup;
import com.bettercloud.kadmin.api.services.KadminConsumerGroupProviderService;
import com.bettercloud.kadmin.kafka.avro.DefaultAvroConsumerGroup;
import com.bettercloud.util.Opt;
import com.bettercloud.util.Page;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/19/16.
 */
@Service(value = "kafkaConsumerGroupProvider")
public class BasicKafkaConsumerProviderService implements KadminConsumerGroupProviderService<String, Object> {

    private final ExecutorService consumerExecutor;
    private final String defaultKafkaHost;
    private final String defaultSchemaRegistryUrl;

    private final LinkedHashMap<String, KadminConsumerGroup> consumerMap;

    @Autowired
    public BasicKafkaConsumerProviderService(
            @Value("${kafka.host:localhost:9092}")
            String defaultKafkaHost,
            @Value("${schema.registry.url:http://localhost:8081}")
            String defaultSchemaRegistryUrl,
            @Value("${kafka.consumer.threadpool.size:5}")
            int consumerThreadPoolSize) {
        this.consumerExecutor = Executors.newFixedThreadPool(consumerThreadPoolSize);
        this.defaultKafkaHost = defaultKafkaHost;
        this.defaultSchemaRegistryUrl = defaultSchemaRegistryUrl;

        this.consumerMap = Maps.newLinkedHashMap();
    }

    @Override
    public void start(KadminConsumerGroup consumer) {
        if (consumer != null && !consumerMap.containsKey(consumer.getClientId())) {
                consumerMap.put(consumer.getClientId(), consumer);
                consumerExecutor.submit(consumer);
        }
    }

    @Override
    public AvroConsumerGroup get(KadminConsumerConfig config, boolean start) {
        Opt.of(config.getKafkaHost()).notPresent(() -> config.setKafkaHost(defaultKafkaHost));
        Opt.of(config.getSchemaRegistryUrl()).notPresent(() -> config.setSchemaRegistryUrl(defaultSchemaRegistryUrl));

        DefaultAvroConsumerGroup defaultAvroConsumerGroup = new DefaultAvroConsumerGroup(config);
        if (start) {
            this.start(defaultAvroConsumerGroup);
        }
        return defaultAvroConsumerGroup;
    }

    @Override
    public Page<KadminConsumerGroup> findAll() {
        return findAll(-1 ,- 1);
    }

    @Override
    public Page<KadminConsumerGroup> findAll(int page, int size) {
        if (page < 0) {
            page = 0;
        }
        if (size <= 0 || size > 100) {
            size = 20;
        }
        int skip = page * size;
        List<KadminConsumerGroup> consumers = consumerMap.values().stream()
                .skip(skip)
                .limit(size)
                .collect(Collectors.toList());
        Page<KadminConsumerGroup> consumerPage = new Page<>();
        consumerPage.setPage(page);
        consumerPage.setSize(size);
        consumerPage.setTotalElements(count());
        consumerPage.setContent(consumers);
        return consumerPage;
    }

    @Override
    public KadminConsumerGroup findById(String consumerId) {
        return consumerMap.get(consumerId);
    }

    @Override
    public long count() {
        return consumerMap.size();
    }
}
