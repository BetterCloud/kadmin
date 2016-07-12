package com.bettercloud.kadmin.api.kafka;

import com.bettercloud.messaging.kafka.consume.ConsumerGroup;
import com.bettercloud.messaging.kafka.consume.MessageHandler;
import com.bettercloud.messaging.kafka.produce.ProducerService;

/**
 * Created by davidesposito on 6/28/16.
 */
public interface KafkaProviderService {

    ProducerService<String, Object> producerService(String schemaRegistryUrl, String kafkaUrl);

    boolean disposeProducer(String schemaRegistryUrl, String kafkaUrl);

    ProducerService<String, Object> lookupProducer(String key);

    ConsumerGroup<String, Object> consumerService(MessageHandler<String, Object> handler, String topic,
                                                  String kafkaUrl, String schemaRegistryUrl);

    boolean disposeConsumer(String topic, String kafkaUrl, String schemaRegistryUrl);

    ConsumerGroup<String, Object> lookupConsumer(String key);
}
