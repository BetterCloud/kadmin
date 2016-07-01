package com.bettercloud.kadmin.api.kafka;

import com.bettercloud.messaging.kafka.produce.ProducerService;

/**
 * Created by davidesposito on 6/28/16.
 */
public interface KafkaProviderService {

    ProducerService<String, Object> producerService();

    ProducerService<String, Object> producerService(String kafkaUrl);

    ProducerService<String, Object> producerService(String schemaRegistryUrl, String kafkaUrl);
}
