package com.bettercloud.kadmin.api.kafka;

import com.bettercloud.kadmin.api.MessageConverter;
import com.bettercloud.kadmin.api.models.KafkaProduceMessageMeta;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.generic.GenericRecord;

/**
 * Created by davidesposito on 7/1/16.
 */
public interface KafkaMessageConverter<ReturnT> extends MessageConverter<KafkaProduceMessageMeta, ReturnT> {
}
