package com.bettercloud.kadmin.api.kafka;

import com.bettercloud.messaging.kafka.consume.ConsumerGroup;
import com.bettercloud.messaging.kafka.consume.MessageHandler;
import com.bettercloud.messaging.kafka.produce.ProducerService;

/**
 * Created by davidesposito on 6/28/16.
 */
public interface KafkaProviderService {

    ProducerService<String, Object> producerService();

    ProducerService<String, Object> producerService(String kafkaUrl);

    ProducerService<String, Object> producerService(String schemaRegistryUrl, String kafkaUrl);

    ConsumerGroup<String, Object> consumerService(MessageHandler<String, Object> handler, String topic);

    ConsumerGroup<String, Object> consumerService(MessageHandler<String, Object> handler, String kafkaUrl, String topic);

    ConsumerGroup<String, Object> consumerService(MessageHandler<String, Object> handler, String schemaRegistryUrl,
                                                  String kafkaUrl, String topic);
}
