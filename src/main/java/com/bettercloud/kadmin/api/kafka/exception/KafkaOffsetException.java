package com.bettercloud.kadmin.api.kafka.exception;

/**
 * Created by davidesposito on 7/19/16.
 */
public class KafkaOffsetException extends Exception {

    public KafkaOffsetException(String message) {
        super(message);
    }

    public KafkaOffsetException(String message, Throwable e) {
        super(message, e);
    }
}
