package com.bettercloud.util;

import com.bettercloud.logger.services.LogLevel;
import com.bettercloud.logger.services.Logger;
import com.bettercloud.logger.services.LoggerFactory;

/**
 * Created by davidesposito on 7/16/16.
 */
public final class LoggerUtils {

    private LoggerUtils() { }

    public static <T> Logger get(Class<T> logClass) {
        return LoggerFactory.getLogger(logClass);
    }

    public static <T> Logger get(Class<T> logClass, LogLevel level) {
        Logger logger = get(logClass);
        logger.setLogLevel(level);
        return logger;
    }
}
