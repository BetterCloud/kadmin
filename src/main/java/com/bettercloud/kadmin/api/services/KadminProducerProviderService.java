package com.bettercloud.kadmin.api.services;

import com.bettercloud.kadmin.api.kafka.KadminProducer;
import com.bettercloud.kadmin.api.kafka.KadminProducerConfig;
import com.bettercloud.util.Page;

/**
 * Created by davidesposito on 7/19/16.
 */
public interface KadminProducerProviderService<KeyT, ValueT> {

    /**
     * Starts the consumer with the provided configurations
     * @param config
     * @return
     */
    KadminProducer<KeyT, ValueT> get(KadminProducerConfig config);

    Page<KadminProducer<KeyT, ValueT>> findAll();

    Page<KadminProducer<KeyT, ValueT>> findAll(int page, int size);

    KadminProducer<KeyT, ValueT> findById(String producerId);

    long count();
}
