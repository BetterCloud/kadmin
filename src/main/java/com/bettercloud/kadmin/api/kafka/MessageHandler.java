package com.bettercloud.kadmin.api.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by davidesposito on 7/19/16.
 */
public interface MessageHandler<KeyT, ValueT> {

    void handle(ConsumerRecord<KeyT, ValueT> record);
}
