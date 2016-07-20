package com.bettercloud.kadmin.api.services;

import com.bettercloud.kadmin.api.kafka.KadminConsumerConfig;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroup;
import com.bettercloud.util.Page;

/**
 * Created by davidesposito on 7/19/16.
 */
public interface KadminConsumerGroupProviderService<KeyT, ValueT> {

    void start(KadminConsumerGroup<KeyT, ValueT> consumer);

    /**
     * Starts the consumer with the provided configurations
     * @param config
     * @return
     */
    KadminConsumerGroup<KeyT, ValueT> get(KadminConsumerConfig config, boolean start);

    Page<KadminConsumerGroup<KeyT, ValueT>> findAll();

    Page<KadminConsumerGroup<KeyT, ValueT>> findAll(int page, int size);

    KadminConsumerGroup<KeyT, ValueT> findById(String consumerId);

    long count();
}
