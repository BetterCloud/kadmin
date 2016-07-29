package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.kafka.KadminConsumerConfig;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroup;
import com.bettercloud.kadmin.api.services.KadminConsumerGroupProviderService;
import com.bettercloud.kadmin.kafka.BasicKafkaConsumerGroup;
import com.bettercloud.util.Opt;
import com.bettercloud.util.Page;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/19/16.
 */
@Service(value = "kafkaConsumerGroupProvider")
public class BasicKafkaConsumerProviderService implements KadminConsumerGroupProviderService {

    private static final Joiner KEY_BUILDER = Joiner.on("<==>");

    private final ExecutorService consumerExecutor;
    private final String defaultKafkaHost;
    private final String defaultSchemaRegistryUrl;
    private final Environment env;

    private final LinkedHashMap<String, KadminConsumerGroup> consumerMap;
    private final Map<String, String> correlationMap;

    @Autowired
    public BasicKafkaConsumerProviderService(
            @Value("${kafka.host:localhost:9092}")
            String defaultKafkaHost,
            @Value("${schema.registry.url:http://localhost:8081}")
            String defaultSchemaRegistryUrl,
            Environment env) {
        this.consumerExecutor = Executors.newCachedThreadPool();
        this.defaultKafkaHost = defaultKafkaHost;
        this.defaultSchemaRegistryUrl = defaultSchemaRegistryUrl;
        this.env = env;

        this.consumerMap = Maps.newLinkedHashMap();
        this.correlationMap = Maps.newConcurrentMap();
    }

    private String getConfigKey(KadminConsumerConfig config) {
        return KEY_BUILDER.join(config.getKafkaHost(),
                config.getSchemaRegistryUrl(),
                config.getTopic(),
                config.getValueDeserializer());
    }

    @Override
    public void start(KadminConsumerGroup consumer) {
        if (consumer != null && !consumerMap.containsKey(consumer.getClientId())) {
                consumerMap.put(consumer.getClientId(), consumer);
                consumerExecutor.submit(consumer);
        }
    }

    @Override
    public KadminConsumerGroup get(KadminConsumerConfig config, boolean start) {
        Opt.of(config.getKafkaHost()).notPresent(() -> config.setKafkaHost(defaultKafkaHost));
        Opt.of(config.getSchemaRegistryUrl()).notPresent(() -> config.setSchemaRegistryUrl(defaultSchemaRegistryUrl));

        KadminConsumerGroup consumerGroup = new BasicKafkaConsumerGroup(config, env);
        String key = getConfigKey(consumerGroup.getConfig());
        if (!correlationMap.containsKey(key)) {
            correlationMap.put(key, consumerGroup.getClientId());
            if (start) {
                this.start(consumerGroup);
            }
        } else {
            consumerGroup = consumerMap.get(correlationMap.get(key));
        }
        return consumerGroup;
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

    @Override
    public void dispose(String consumerId) {
        Opt.of(consumerMap.get(consumerId)).ifPresent(c -> {
            c.shutdown();
            consumerMap.remove(consumerId);
            correlationMap.remove(getConfigKey(c.getConfig()));
        });
    }
}
