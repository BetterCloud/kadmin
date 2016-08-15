package com.bettercloud.kadmin.kafka;

import com.bettercloud.kadmin.api.kafka.KadminConsumerConfig;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroup;
import com.bettercloud.kadmin.api.kafka.MessageHandler;
import com.bettercloud.kadmin.api.kafka.MessageHandlerRegistry;
import com.bettercloud.util.LoggerUtils;
import com.bettercloud.util.Opt;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.springframework.core.env.Environment;

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
    private final Environment env;

    private final Set<MessageHandler> handlers;

    public BasicKafkaConsumerGroup(KadminConsumerConfig config, Environment env) {
        assert(config != null);
        assert(config.getKeyDeserializer() != null);
        assert(config.getValueDeserializer() != null);

        this.config = config;
        this.clientId = "kadmin-" + UUID.randomUUID().toString();
        this.groupId = this.clientId;
        this.env = env;
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

        properties.put("session.timeout.ms", env.getProperty("session.timeout.ms", Integer.class, 30000));
        properties.put("heartbeat.interval.ms", env.getProperty("heartbeat.interval.ms", Integer.class, 3000));
        properties.put("partition.assignment.strategy", env.getProperty("partition.assignment.strategy", RangeAssignor.class.getName()));
        properties.put("metadata.max.age.ms", env.getProperty("metadata.max.age.ms", Integer.class, 5 * 60 * 1000));
        properties.put("enable.auto.commit", env.getProperty("enable.auto.commit", Boolean.class, false));
        properties.put("auto.commit.interval.ms", env.getProperty("auto.commit.interval.ms", Integer.class, 5000));
        properties.put("max.partition.fetch.bytes", env.getProperty("max.partition.fetch.bytes", Integer.class, 1 * 1024 * 1024));
        properties.put("send.buffer.bytes", env.getProperty("send.buffer.bytes", Integer.class, 128 * 1024));
        properties.put("receive.buffer.bytes", env.getProperty("receive.buffer.bytes", Integer.class, 32 * 1024));
        properties.put("fetch.min.bytes", env.getProperty("fetch.min.bytes", Integer.class, 1));
        properties.put("fetch.max.wait.ms", env.getProperty("fetch.max.wait.ms", Integer.class, 500));
        properties.put("reconnect.backoff.ms", env.getProperty("reconnect.backoff.ms", Long.class, 50L));
        properties.put("retry.backoff.ms", env.getProperty("retry.backoff.ms", Long.class, 100L));
        properties.put("auto.offset.reset", env.getProperty("auto.offset.reset", "latest"));
        properties.put("check.crcs", env.getProperty("check.crcs", Boolean.class, true));
        properties.put("metrics.sample.window.ms", env.getProperty("metrics.sample.window.ms", Integer.class, 30000));
        properties.put("metrics.num.samples", env.getProperty("metrics.num.samples", Integer.class, 2));
        properties.put("metric.reporters", env.getProperty("metric.reporters", ""));
        properties.put("request.timeout.ms", env.getProperty("request.timeout.ms", Integer.class, 40 * 1000));
        properties.put("connections.max.idle.ms", env.getProperty("connections.max.idle.ms", Integer.class, 9 * 60 * 1000));
        properties.put("security.protocol", env.getProperty("security.protocol", "PLAINTEXT"));

        properties.put("key.deserializer", config.getKeyDeserializer());
        properties.put("value.deserializer", config.getValueDeserializer().getClassName());

        this.consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(config.getTopic()));

        poll(1000);

        setInitialOffset();
    }

    private void setInitialOffset() {
        Set<TopicPartition> topicPartitions = consumer.assignment();
        int perPartitionRewind = 100 / topicPartitions.size();
        topicPartitions.stream()
                .map(tp -> {
                    System.out.println(tp);
                    return Pair.of(tp, consumer.committed(tp));
                })
                .forEach(pair -> {
                    TopicPartition tp = pair.getLeft();
                    Opt.of(pair.getRight())
                            .map(meta -> meta.offset())
                            .map(offset -> Math.max(0, offset - perPartitionRewind))
                            .ifPresent(newOffset -> LOGGER.info("Seeing {} to {} (per partiion rewind {})",
                                    tp.toString(), newOffset, perPartitionRewind))
                            .ifPresent(newOffset -> consumer.seek(tp, newOffset))
                            .notPresent(() -> LOGGER.error("Could not calculate new offset for {}", tp.toString()));
                });
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
    public void shutdown() {
        if (consumer != null) {
            consumer.wakeup();
            consumer = null;
        }
    }

    private void poll() {
        poll(0L);
    }

    private void poll(long pollTime) {
        ConsumerRecords<String, Object> records = consumer.poll(pollTime);
        for (ConsumerRecord<String, Object> record : records) {
            synchronized (handlers) {
                handlers.stream().forEach(h -> h.handle(record));
            }
        }
        consumer.commitSync();
    }

    @Override
    public void run() {
        if (consumer != null) {
            return;
        } else {
            init();
        }
        try {
            while (true) {
                poll();
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
