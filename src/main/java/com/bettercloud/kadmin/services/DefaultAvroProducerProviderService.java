package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.kafka.KadminProducer;
import com.bettercloud.kadmin.api.kafka.KadminProducerConfig;
import com.bettercloud.kadmin.api.services.AvroProducerProviderService;
import com.bettercloud.kadmin.io.network.rest.KafkaMessageConsumerResource;
import com.bettercloud.kadmin.kafka.avro.DefaultAvroProducer;
import com.bettercloud.util.LoggerUtils;
import com.bettercloud.util.Opt;
import com.bettercloud.util.Page;
import com.bettercloud.util.TimedWrapper;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/20/16.
 */
@Service
public class DefaultAvroProducerProviderService implements AvroProducerProviderService {

    private static final Logger LOGGER = LoggerUtils.get(DefaultAvroProducerProviderService.class);
    private static final long IDLE_THRESHOLD = 15L * 60 * 1000; // 15 minutes
    private static final long IDLE_CHECK_DELAY = 60L * 60 * 1000; // 60 minutes

    private final String defaultKafkaHost;
    private final String defaultSchemaRegistryUrl;

    private final LinkedHashMap<String, TimedWrapper<KadminProducer<String, Object>>> producerMap;

    @Autowired
    public DefaultAvroProducerProviderService(
            @Value("${kafka.host:localhost:9092}")
                    String defaultKafkaHost,
            @Value("${schema.registry.url:http://localhost:8081}")
                    String defaultSchemaRegistryUrl) {
        this.defaultKafkaHost = defaultKafkaHost;
        this.defaultSchemaRegistryUrl = defaultSchemaRegistryUrl;

        this.producerMap = Maps.newLinkedHashMap();
    }

    @Scheduled(fixedRate = IDLE_CHECK_DELAY)
    private void clearMemory() {
        LOGGER.info("Cleaning up producers connections");
        List<String> keys = producerMap.keySet().stream()
                .filter(k -> producerMap.get(k).getIdleTime() > IDLE_THRESHOLD)
                .collect(Collectors.toList());
        keys.stream()
                .forEach(k -> {
                    TimedWrapper<KadminProducer<String, Object>> timedWrapper = producerMap.get(k);
                    LOGGER.debug("Disposing old consumer ({}) with timeout {}", k, timedWrapper.getIdleTime());
                    timedWrapper.getData().shutdown();
                });
        System.gc();
    }

    @Override
    public KadminProducer<String, Object> get(KadminProducerConfig config) {
        Opt.of(config.getKafkaHost()).notPresent(() -> config.setKafkaHost(defaultKafkaHost));
        Opt.of(config.getSchemaRegistryUrl()).notPresent(() -> config.setSchemaRegistryUrl(defaultSchemaRegistryUrl));

        DefaultAvroProducer producer = new DefaultAvroProducer(config);
        producerMap.put(producer.getId(), TimedWrapper.of(producer));
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
                .map(tw -> tw.getData())
                .collect(Collectors.toList());
        Page<KadminProducer<String, Object>> consumerPage = new Page<>();
        consumerPage.setPage(page);
        consumerPage.setSize(size);
        consumerPage.setTotalElements(count());
        consumerPage.setContent(consumers);
        return consumerPage;
    }

    @Override
    public TimedWrapper<KadminProducer<String, Object>> findById(String consumerId) {
        return producerMap.get(consumerId);
    }

    @Override
    public long count() {
        return producerMap.size();
    }

    @Override
    public boolean dispose(String producerId) {
        TimedWrapper<KadminProducer<String, Object>> twProducer = producerMap.get(producerId);
        if (twProducer == null) {
            return false;
        }
        twProducer.getData().shutdown();
        producerMap.remove(producerId);
        return true;
    }
}
