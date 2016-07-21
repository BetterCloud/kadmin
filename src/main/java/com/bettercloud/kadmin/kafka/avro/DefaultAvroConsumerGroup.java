package com.bettercloud.kadmin.kafka.avro;

import com.bettercloud.kadmin.api.kafka.KadminConsumerConfig;
import com.bettercloud.kadmin.api.kafka.avro.AvroConsumerGroup;
import com.bettercloud.kadmin.kafka.BasicKafkaConsumerGroup;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Created by davidesposito on 7/20/16.
 */
public class DefaultAvroConsumerGroup extends BasicKafkaConsumerGroup<Object> implements AvroConsumerGroup {

    public DefaultAvroConsumerGroup(KadminConsumerConfig config) {
        super(config);
    }

    @Override
    protected void initConfig(KadminConsumerConfig config) {
        config.setKeyDeserializer(StringDeserializer.class.getName());
        config.setValueDeserializer(ErrorTolerantAvroObjectDeserializer.class.getName());
    }
}
