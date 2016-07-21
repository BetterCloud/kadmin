package com.bettercloud.kadmin.api.kafka;

/**
 * Created by davidesposito on 7/20/16.
 */
public interface MessageHandlerRegistry {

    void register(MessageHandler handler);

    boolean remove(MessageHandler handler);
}
