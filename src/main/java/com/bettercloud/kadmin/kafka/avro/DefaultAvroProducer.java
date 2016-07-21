package com.bettercloud.kadmin.kafka.avro;

import com.bettercloud.kadmin.api.kafka.KadminProducerConfig;
import com.bettercloud.kadmin.api.kafka.avro.AvroProducer;
import com.bettercloud.kadmin.kafka.BasicKafkaProducer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by davidesposito on 7/20/16.
 */
public class DefaultAvroProducer extends BasicKafkaProducer<Object> implements AvroProducer {

    public DefaultAvroProducer(KadminProducerConfig config) {
        super(config);
    }

    @Override
    protected void initConfig(KadminProducerConfig config) {
        config.setKeySerializer(StringSerializer.class.getName());
        config.setValueSerializer(KafkaAvroSerializer.class.getName());
    }
}
