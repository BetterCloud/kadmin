package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.kafka.KadminConsumerConfig;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroup;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroupContainer;
import com.bettercloud.kadmin.api.services.BundledKadminConsumerGroupProviderService;
import com.bettercloud.kadmin.api.services.KadminConsumerGroupProviderService;
import com.bettercloud.kadmin.kafka.QueuedKafkaMessageHandler;
import com.bettercloud.util.Opt;
import com.bettercloud.util.Page;
import com.google.common.collect.Maps;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/19/16.
 */
@Service
public class DefultBundledKadminConsumerGroupProviderService implements BundledKadminConsumerGroupProviderService {

    private final KadminConsumerGroupProviderService providerService;
    private final Map<String, KadminConsumerGroupContainer> containerMap;

    @Autowired
    public DefultBundledKadminConsumerGroupProviderService(KadminConsumerGroupProviderService providerService) {
        this.providerService = providerService;
        this.containerMap = Maps.newLinkedHashMap();
    }

    @Override
    public void start(String consumerId) {
        Opt.of(containerMap.get(consumerId)).ifPresent(cont -> providerService.start(cont.getConsumer()));
    }

    @Override
    public KadminConsumerGroupContainer get(KadminConsumerConfig config, boolean start, int queueSize) {
        KadminConsumerGroup consumer = providerService.get(config, start);
        KadminConsumerGroupContainer container = containerMap.get(consumer.getClientId());
        if (container == null) {
            QueuedKafkaMessageHandler queue = new QueuedKafkaMessageHandler(queueSize);
            consumer.register(queue);
            container = KadminConsumerGroupContainer.builder()
                    .consumer(consumer)
                    .queue(queue)
                    .build();
            containerMap.put(container.getId(), container);
        }
        return container;
    }

    @Override
    public Page<KadminConsumerGroupContainer> findAll() {
        return findAll(-1, -1);
    }

    @Override
    public Page<KadminConsumerGroupContainer> findAll(int page, int size) {
        if (page < 0) {
            page = 0;
        }
        if (size <= 0 || size > 100) {
            size = 20;
        }
        int skip = page * size;
        List<KadminConsumerGroupContainer> consumers = containerMap.values().stream()
                .skip(skip)
                .limit(size)
                .collect(Collectors.toList());
        Page<KadminConsumerGroupContainer> consumerPage = new Page<>();
        consumerPage.setPage(page);
        consumerPage.setSize(size);
        consumerPage.setTotalElements(count());
        consumerPage.setContent(consumers);
        return consumerPage;
    }

    @Override
    public KadminConsumerGroupContainer findById(String consumerId) {
        return containerMap.get(consumerId);
    }

    @Override
    public long count() {
        return containerMap.size();
    }

    @Override
    public KadminConsumerGroupContainer dispose(String consumerId) {
        return Opt.of(containerMap.get(consumerId))
                .ifPresent(container -> {
                    providerService.dispose(consumerId);
                    containerMap.remove(consumerId);
                })
                .orElse(null);
    }
}
