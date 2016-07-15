package com.bettercloud.kadmin.kafka;

import com.bettercloud.messaging.kafka.consume.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Created by davidesposito on 7/5/16.
 */
@Component
public class ConsoleKafkaMessageHandler implements MessageHandler<String, Object> {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleKafkaMessageHandler.class);

    @Override
    public void handleMessage(String s, Object o) {
        logger.info("{}: {}", s, o);
    }
}
