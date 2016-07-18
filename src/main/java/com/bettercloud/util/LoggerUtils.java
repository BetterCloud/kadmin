package com.bettercloud.util;

import com.bettercloud.logger.services.Logger;
import com.bettercloud.logger.services.LoggerFactory;

/**
 * Created by davidesposito on 7/16/16.
 */
public final class LoggerUtils {

    private LoggerUtils() { }

//    public static Logger setLogLevel(Logger logger, Level level) {
//        ((ch.qos.logback.classic.Logger) logger).setLevel(level);
//        return logger;
//    }

    public static <T> Logger get(Class<T> logClass) {
        return LoggerFactory.getLogger(logClass);
    }

//    public static <T> Logger get(Class<T> logClass, Level level) {
//        return setLogLevel(get(logClass), level);
//    }
}
