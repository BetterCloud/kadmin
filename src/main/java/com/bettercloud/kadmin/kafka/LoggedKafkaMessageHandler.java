package com.bettercloud.kadmin.kafka;

import com.bettercloud.kadmin.api.kafka.MessageHandler;
import com.bettercloud.util.LoggerUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;

/**
 * Created by davidesposito on 7/5/16.
 */
public class LoggedKafkaMessageHandler implements MessageHandler {

    private static final Logger logger = LoggerUtils.get(LoggedKafkaMessageHandler.class);

    @Override
    public void handle(ConsumerRecord<String, Object> record) {
        logger.info("({}@{}) {}: {}", record.offset(), record.topic(), record.key(), record.value());
    }
}
