package com.bettercloud.kadmin.api.services;

import com.bettercloud.messaging.kafka.produce.ProducerService;

/**
 * Created by davidesposito on 6/28/16.
 */
public interface KafkaProviderService {

    ProducerService<String, Object> producerService(String schemaRegistryUrl, String kafkaUrl);

    boolean disposeProducer(String schemaRegistryUrl, String kafkaUrl);

    ProducerService<String, Object> lookupProducer(String key);

}
