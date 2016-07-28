package com.bettercloud.kadmin.kafka;

import com.bettercloud.kadmin.api.kafka.KadminConsumerConfig;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroup;
import com.bettercloud.kadmin.api.kafka.MessageHandler;
import com.bettercloud.kadmin.api.kafka.MessageHandlerRegistry;
import com.bettercloud.util.LoggerUtils;
import com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by davidesposito on 7/20/16.
 */
public class BasicKafkaConsumerGroup implements KadminConsumerGroup, MessageHandlerRegistry {

    private static final Logger LOGGER = LoggerUtils.get(BasicKafkaConsumerGroup.class);

    private KafkaConsumer<String, Object> consumer;
    private final KadminConsumerConfig config;
    private final String clientId;
    private final String groupId;
    private final AtomicLong lastOffset;

    private final Set<MessageHandler> handlers;

    public BasicKafkaConsumerGroup(KadminConsumerConfig config) {
        assert(config != null);
        assert(config.getKeyDeserializer() != null);
        assert(config.getValueDeserializer() != null);

        this.config = config;
        this.clientId = UUID.randomUUID().toString();
        this.groupId = this.clientId;
        this.lastOffset = new AtomicLong(-1);
        this.handlers = Collections.synchronizedSet(Sets.newLinkedHashSet());
    }

    /**
     * Gets called before init(). Set any default configs here
     * @param config
     */
    protected void initConfig(KadminConsumerConfig config) { }

    protected void init() {
        initConfig(config);

        LOGGER.info("Initializing Consumer: {}", config);

        final Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getKafkaHost());
        properties.put("schema.registry.url", config.getSchemaRegistryUrl());

        properties.put("client.id", getClientId());
        properties.put("group.id", getGroupId());

        properties.put("session.timeout.ms", 30000);
        properties.put("heartbeat.interval.ms", 3000);
        properties.put("partition.assignment.strategy", RangeAssignor.class.getName());
        properties.put("metadata.max.age.ms", 5 * 60 * 1000);
        properties.put("enable.auto.commit", false);
        properties.put("auto.commit.interval.ms", 5000);
        properties.put("max.partition.fetch.bytes", 1 * 1024 * 1024);
        properties.put("send.buffer.bytes", 128 * 1024);
        properties.put("receive.buffer.bytes", 32 * 1024);
        properties.put("fetch.min.bytes", 1);
        properties.put("fetch.max.wait.ms", 500);
        properties.put("reconnect.backoff.ms", 50L);
        properties.put("retry.backoff.ms", 100L);
        properties.put("auto.offset.reset", "latest");
        properties.put("check.crcs", true);
        properties.put("metrics.sample.window.ms", 30000);
        properties.put("metrics.num.samples", 2);
        properties.put("metric.reporters", "");
        properties.put("request.timeout.ms", 40 * 1000);
        properties.put("connections.max.idle.ms", 9 * 60 * 1000);
        properties.put("security.protocol", "PLAINTEXT");

        properties.put("specific.avro.reader", true);

        properties.put("key.deserializer", config.getKeyDeserializer());
        properties.put("value.deserializer", config.getValueDeserializer().getClassName());

        this.consumer = new KafkaConsumer<>(properties);
    }

    @Override
    public KadminConsumerConfig getConfig() {
        return config;
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public String getGroupId() {
        return groupId;
    }

    @Override
    public long getOffset() {
        return lastOffset.get();
    }

    @Override
    public void setOffset(long newOffset) {
        if (consumer != null) {
            // TODO: for real
            // consumer.seek(partition, newOffset);
            lastOffset.set(newOffset);
        }
    }

    @Override
    public void shutdown() {
        if (consumer != null) {
            consumer.wakeup();
            consumer = null;
        }
    }

    @Override
    public void run() {
        if (consumer != null) {
            return;
        } else {
            init();
        }
        try {
            consumer.subscribe(Arrays.asList(config.getTopic()));

            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(0);
                for (ConsumerRecord<String, Object> record : records) {
                    lastOffset.set(record.offset());
                    synchronized (handlers) {
                        handlers.stream().forEach(h -> h.handle(record));
                    }
                }
                try {
                    Thread.sleep(500); // TODO: configure
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    @Override
    public void register(MessageHandler handler) {
        if (handler != null) {
            handlers.add(handler);
        }
    }

    @Override
    public boolean remove(MessageHandler handler) {
        if (handler != null) {
            return handlers.remove(handler);
        }
        return false;
    }

    @Override
    public Collection<MessageHandler> getHandlers() {
        return Collections.unmodifiableSet(Sets.newLinkedHashSet(handlers));
    }
}
