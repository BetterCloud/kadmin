package com.bettercloud.kadmin.api.kafka;

import com.bettercloud.kadmin.api.kafka.exception.KafkaOffsetException;

import java.util.Optional;

/**
 * Created by davidesposito on 7/19/16.
 */
public interface KafkaOffsetService {

    long getOffset(Optional<String> oUrl, String topic) throws KafkaOffsetException;

    boolean setOffset(Optional<String> oUrl, String topic, long newOffset) throws KafkaOffsetException;
}
