package com.bettercloud.kadmin.api.kafka;

import com.bettercloud.kadmin.kafka.QueuedKafkaMessageHandler;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.Collection;

/**
 * Created by davidesposito on 7/23/16.
 */
@Data
@Builder
public class KadminConsumerGroupContainer {

    @NonNull private final KadminConsumerGroup consumer;
    @NonNull private final QueuedKafkaMessageHandler queue;

    /**
     * Must return the id of the contained consumer. Merely a delegate method for {@link KadminConsumerGroup#getClientId()}.
     * @return The consumers unique client id
     */
    public String getId() {
        return getConsumer().getClientId();
    }

    public KadminConsumerGroup getConsumer() {
        return consumer;
    }

    /**
     * The Cached queue of the contained consumer. Note that this {@link QueuedKafkaMessageHandler} will be included in the
     * {@link KadminConsumerGroupContainer#getHandlers()} return values.
     * @return
     */
    public QueuedKafkaMessageHandler getQueue() {
        return queue;
    }

    /**
     * Must return the handlers list of the contained consumer. Merely a delegate method for {@link KadminConsumerGroup#getHandlers()}.
     * @return The consumers handlers
     */
    public Collection<MessageHandler> getHandlers() {
        return getConsumer().getHandlers();
    }
}
