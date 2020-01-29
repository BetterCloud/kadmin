package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.kafka.KadminProducer;
import com.bettercloud.kadmin.api.kafka.KadminProducerConfig;
import com.bettercloud.kadmin.api.services.KadminProducerProviderService;
import com.bettercloud.kadmin.kafka.BasicKafkaProducer;
import com.bettercloud.util.LoggerUtils;
import com.bettercloud.util.Opt;
import com.bettercloud.util.Page;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/20/16.
 */
@Service
public class BasicKafkaProducerProviderService implements KadminProducerProviderService<String, Object> {

    private static final Logger LOGGER = LoggerUtils.get(BasicKafkaProducerProviderService.class);
    private static final long IDLE_THRESHOLD = 15L * 60 * 1000; // 15 minutes
    private static final long IDLE_CHECK_DELAY = 60L * 60 * 1000; // 60 minutes

    private final String defaultKafkaHost;
    private final String defaultSchemaRegistryUrl;
    private final String defaultSecurityProtocol;
    private final String defaultTrustStoreLocation;
    private final String defaultTrustStorePassword;
    private final String defaultKeyStoreLocation;
    private final String defaultKeyStorePassword;
    private final String defaultKeyPassword;

    private final LinkedHashMap<String, KadminProducer<String, Object>> producerMap;

    @Autowired
    public BasicKafkaProducerProviderService(
            @Value("${kafka.host:localhost:9092}")
                    String defaultKafkaHost,
            @Value("${schema.registry.url:http://localhost:8081}")
                    String defaultSchemaRegistryUrl,
            @Value("${security.protocol:PLAINTEXT}")
                    String defaultSecurityProtocol,
                    Environment env) {
        this.defaultKafkaHost = defaultKafkaHost;
        this.defaultSchemaRegistryUrl = defaultSchemaRegistryUrl;
        this.defaultSecurityProtocol = defaultSecurityProtocol;
        this.defaultTrustStoreLocation = env.getProperty("trust.store.location");
        this.defaultTrustStorePassword = env.getProperty("trust.store.password");
        this.defaultKeyStoreLocation = env.getProperty("key.store.location");
        this.defaultKeyStorePassword = env.getProperty("key.store.password");
        this.defaultKeyPassword = env.getProperty("key.password");

        this.producerMap = Maps.newLinkedHashMap();
    }

    @Scheduled(fixedRate = IDLE_CHECK_DELAY)
    private void clearMemory() {
        LOGGER.info("Cleaning up producers connections");
        long currTime = System.currentTimeMillis();
        List<String> keys = producerMap.keySet().stream()
                .filter(k -> currTime - producerMap.get(k).getLastUsedTime() > IDLE_THRESHOLD)
                .collect(Collectors.toList());
        keys.stream()
                .forEach(k -> {
                    KadminProducer<String, Object> p = producerMap.get(k);
                    LOGGER.debug("Disposing old consumer ({}) with timeout {}", k, System.currentTimeMillis() - p.getLastUsedTime());
                    p.shutdown();
                });
        System.gc();
    }

    @Override
    public KadminProducer<String, Object> get(KadminProducerConfig config) {
        Opt.of(config.getKafkaHost()).notPresent(() -> config.setKafkaHost(defaultKafkaHost));
        Opt.of(config.getSchemaRegistryUrl()).notPresent(() -> config.setSchemaRegistryUrl(defaultSchemaRegistryUrl));
        Opt.of(config.getSecurityProtocol()).notPresent(() -> config.setSecurityProtocol(defaultSecurityProtocol));
        Opt.of(config.getTrustStoreLocation()).notPresent(() -> config.setTrustStoreLocation(defaultTrustStoreLocation));
        Opt.of(config.getTrustStorePassword()).notPresent(() -> config.setTrustStorePassword(defaultTrustStorePassword));
        Opt.of(config.getKeyStoreLocation()).notPresent(() -> config.setKeyStoreLocation(defaultKeyStoreLocation));
        Opt.of(config.getKeyStorePassword()).notPresent(() -> config.setKeyStorePassword(defaultKeyStorePassword));
        Opt.of(config.getKeyPassword()).notPresent(() -> config.setKeyPassword(defaultKeyPassword));

        BasicKafkaProducer<Object> producer = new BasicKafkaProducer<>(config);
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

    @Override
    public boolean dispose(String producerId) {
        KadminProducer<String, Object> p = producerMap.get(producerId);
        if (p == null) {
            return false;
        }
        p.shutdown();
        producerMap.remove(producerId);
        return true;
    }
}
