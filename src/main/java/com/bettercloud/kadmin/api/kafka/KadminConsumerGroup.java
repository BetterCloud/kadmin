package com.bettercloud.kadmin.api.kafka;

/**
 * Created by davidesposito on 7/19/16.
 */
public interface KadminConsumerGroup extends Runnable, MessageHandlerRegistry {

    KadminConsumerConfig getConfig();

    String getClientId();

    String getGroupId();

    long getOffset();

    void setOffset(long newOffset);

    void shutdown();
}
