package com.bettercloud.kadmin.api.kafka;

/**
 * Created by davidesposito on 7/20/16.
 */
public interface MessageHandlerRegistry<KeyT, ValueT> {

    void register(MessageHandler<KeyT, ValueT> handler);

    boolean remove(MessageHandler<KeyT, ValueT> handler);
}
