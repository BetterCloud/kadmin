package com.bettercloud.kadmin.kafka;

import com.bettercloud.kadmin.api.kafka.MessageHandler;
import com.bettercloud.util.LoggerUtils;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by davidesposito on 7/1/16.
 */
public class QueuedKafkaMessageHandler implements MessageHandler<String, Object> {

    private static final Logger LOGGER = LoggerUtils.get(QueuedKafkaMessageHandler.class);

    private final FixedSizeList<MessageContainer> messageQueue;
    private final AtomicLong total = new AtomicLong(0L);

    public QueuedKafkaMessageHandler(int maxSize) {
        messageQueue = new FixedSizeList<>(maxSize);
    }

    @Override
    public void handle(ConsumerRecord<String, Object> record) {
        LOGGER.debug("receiving => {}, queued => {}",total.get() + 1, messageQueue.spine.size());
        total.incrementAndGet();
        this.messageQueue.add(MessageContainer.builder()
                .key(record.key())
                .message(record.value())
                .offset(record.offset())
                .partition(record.partition())
                .topic(record.topic())
                .writeTime(System.currentTimeMillis())
                .build());
    }

    public List<Object> get(Long since) {
        return messageQueue.stream()
                .filter(c -> isValidDate(since, c.getWriteTime()))
                .collect(Collectors.toList());
    }

    public int count(Long since) {
        return (int)messageQueue.stream()
                .filter(c -> isValidDate(since, c.getWriteTime()))
                .count();
    }

    public long total() {
        return total.get();
    }

    public void clear() {
        total.set(0L);
        messageQueue.clear();
    }

    protected boolean isValidDate(Long since, Long writeTime) {
        return since < 0 || writeTime > since;
    }

    @Data
    @Builder
    public static class MessageContainer {

        private final long writeTime;
        private final String key;
        private final Object message;
        private final String topic;
        private final int partition;
        private final long offset;
    }

    protected static class FixedSizeList<E> {

        private final LinkedList<E> spine;
        private final int maxSize;

        public FixedSizeList(int maxSize) {
            spine = Lists.newLinkedList();
            this.maxSize = Math.min(Math.max(maxSize, 1), 2000);
        }

        public synchronized void add(E ele) {
            if (spine.size() >= maxSize) {
                spine.removeFirst();
            }
            spine.add(ele);
        }

        public void clear() {
            spine.clear();
        }

        public synchronized Stream<E> stream() {
            return spine.stream();
        }
    }
}
