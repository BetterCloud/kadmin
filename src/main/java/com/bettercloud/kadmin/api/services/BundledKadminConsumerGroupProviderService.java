package com.bettercloud.kadmin.api.services;

import com.bettercloud.kadmin.api.kafka.KadminConsumerConfig;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroup;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroupContainer;
import com.bettercloud.util.Page;

/**
 * Created by davidesposito on 7/19/16.
 */
public interface BundledKadminConsumerGroupProviderService {

    void start(String consumerId);

    /**
     * Starts the consumer with the provided configurations
     * @param config
     * @return
     */
    KadminConsumerGroupContainer get(KadminConsumerConfig config, boolean start, int queueSize);

    Page<KadminConsumerGroupContainer> findAll();

    Page<KadminConsumerGroupContainer> findAll(int page, int size);

    KadminConsumerGroupContainer findById(String consumerId);

    long count();

    KadminConsumerGroupContainer dispose(String consumerId);
}
