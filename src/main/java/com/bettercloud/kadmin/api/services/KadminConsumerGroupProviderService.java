package com.bettercloud.kadmin.api.services;

import com.bettercloud.kadmin.api.kafka.KadminConsumerConfig;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroup;
import com.bettercloud.util.Page;

/**
 * Created by davidesposito on 7/19/16.
 */
public interface KadminConsumerGroupProviderService {

    void start(KadminConsumerGroup consumer);

    /**
     * Starts the consumer with the provided configurations
     * @param config
     * @return
     */
    KadminConsumerGroup get(KadminConsumerConfig config, boolean start);

    Page<KadminConsumerGroup> findAll();

    Page<KadminConsumerGroup> findAll(int page, int size);

    KadminConsumerGroup findById(String consumerId);

    long count();

    void dispose(String Id);
}
