package com.bettercloud.kadmin.kafka;

import com.bettercloud.logger.services.Logger;
import com.bettercloud.logger.services.model.LogModel;
import com.bettercloud.messaging.kafka.consume.MessageHandler;
import com.bettercloud.util.LoggerUtils;
import org.springframework.stereotype.Component;

/**
 * Created by davidesposito on 7/5/16.
 */
@Component
public class LoggedKafkaMessageHandler implements MessageHandler<String, Object> {

    private static final Logger logger = LoggerUtils.get(LoggedKafkaMessageHandler.class);

    @Override
    public void handleMessage(String s, Object o) {
        logger.log(LogModel.info("{}: {}")
                .args(s, o)
                .build());
    }
}
