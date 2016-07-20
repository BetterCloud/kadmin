package com.bettercloud.kadmin.api.kafka;

import com.bettercloud.util.Page;

/**
 * Created by davidesposito on 7/19/16.
 */
public interface ConsumerGroupProviderService<KeyT, ValueT> {

    void start(ConsumerGroup<KeyT, ValueT> consumer);

    /**
     * Starts the consumer with the provided configurations
     * @param config
     * @return
     */
    ConsumerGroup<KeyT, ValueT> get(KafkaConsumerConfig config, boolean start);

    Page<ConsumerGroup<KeyT, ValueT>> findAll();

    Page<ConsumerGroup<KeyT, ValueT>> findAll(int page, int size);

    ConsumerGroup<KeyT, ValueT> findById(String consumerId);
}
