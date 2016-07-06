package com.bettercloud.kadmin.kafka;

import com.bettercloud.avro.workflow.*;
import com.bettercloud.kadmin.api.kafka.KafkaMessageConverter;
import com.bettercloud.kadmin.api.models.KafkaProduceMessageMeta;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/1/16.
 */
@Component("genericRecordKafkaConverter")
public class DefaultKafkaMessageConverter implements KafkaMessageConverter<GenericRecord> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, Class<? extends GenericRecord>> SCHEMA_MAP;

    static {
        List<Class<? extends GenericRecord>> schemaClasses = Collections.unmodifiableList(Lists.newArrayList(
                ActionCall.class,
                ActionEndpoint.class,
                ActionResponsePayload.class,
                ConditionPayload.class,
                ContextPayload.class,
                EventCall.class,
                MapEntry.class,
                MapKeyValueEntry.class,
                MetaCondition.class,
                MetaConditionValue.class,
                WorkflowEventPayload.class,
                WorkflowPayload.class
        ));
        SCHEMA_MAP = Collections.unmodifiableMap(schemaClasses.stream()
                .collect(Collectors.toMap(
                        Class::getSimpleName,
                        c -> c
                )));
    }

    @Override
    public GenericRecord convert(JsonNode rawMessage, KafkaProduceMessageMeta meta) {
        GenericRecord rec = null;
        if (rawMessage != null && meta != null && meta.getSchema() != null && SCHEMA_MAP.containsKey(meta.getSchema())) {
            Class<? extends GenericRecord> c = SCHEMA_MAP.get(meta.getSchema());
            if (c != null) {
                try {
                    rec = MAPPER.treeToValue(rawMessage, c);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
        return rec;
    }
}
