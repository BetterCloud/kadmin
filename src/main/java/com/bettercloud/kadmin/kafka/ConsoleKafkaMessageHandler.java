package com.bettercloud.kadmin.kafka;

import com.bettercloud.logger.services.LogLevel;
import com.bettercloud.logger.services.Logger;
import com.bettercloud.messaging.kafka.consume.MessageHandler;
import com.bettercloud.util.LoggerUtils;
import org.springframework.stereotype.Component;

/**
 * Created by davidesposito on 7/5/16.
 */
@Component
public class ConsoleKafkaMessageHandler implements MessageHandler<String, Object> {

    private static final Logger logger = LoggerUtils.get(ConsoleKafkaMessageHandler.class);

    @Override
    public void handleMessage(String s, Object o) {
        logger.log(LogLevel.INFO, "{}: {}", s, o);
    }
}
