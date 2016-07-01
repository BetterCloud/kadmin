package com.bettercloud.kadmin.kafka;

import com.bettercloud.avro.Header;
import com.bettercloud.avro.workflow.EventCall;
import com.bettercloud.avro.workflow.MapKeyValueEntry;
import com.bettercloud.kadmin.api.models.KafkaProduceMessageMeta;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by davidesposito on 7/1/16.
 */
public class DefaultKafkaMessageConverterTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private final DefaultKafkaMessageConverter kmc = new DefaultKafkaMessageConverter();

    private KafkaProduceMessageMeta getMeta(Class c) {
        return KafkaProduceMessageMeta.builder()
                .schema(c.getSimpleName())
                .build();
    }

    @Test
    public void testNull() {
        assertNull(kmc.convert(null, getMeta(EventCall.class)));
    }

    @Test
    public void testEmpty() {
        ObjectNode emptyNode = mapper.createObjectNode();
        GenericRecord rec = kmc.convert(emptyNode, getMeta(EventCall.class));
        assertNotNull(rec);
        assertNull(rec.get("header"));
        assertNull(rec.get("eventId"));
        assertNull(rec.get("eventParams"));
        assertNull(rec.get("values"));
    }

    @Test
    public void testEventCall() throws IOException {
        JsonNode rawMessage = mapper.readTree(KafkaTestConstants.JSON_EVENT_CALL_01);
        GenericRecord rec = kmc.convert(rawMessage, getMeta(EventCall.class));
        assertNotNull(rec);

        EventCall ec = (EventCall) rec;
        assertNotNull(ec.getHeader());
        assertEquals("f7d7e7c5-1a1f-4d2a-9ae0-ce07e83907fa", ec.getEventId());
        assertNotNull(ec.getEventId());
        assertNull(ec.getEventParams());
        assertNotNull(ec.getValues());

        Header header = ec.getHeader();
        assertNotNull(header.getEventId());
        assertNotNull(header.getCorrelationId());
        assertNotNull(header.getDomainId());
        assertNotNull(header.getExternalCorrelationId());
        assertNotNull(header.getProviderId());
        assertNotNull(header.getReceivedDate());
        assertNotNull(header.getSenderId());
        assertNotNull(header.getTenantId());
        assertNotNull(header.getUserId());
        assertNotNull(header.getEventMeta());

        List<MapKeyValueEntry> eventMeta = header.getEventMeta();
        assertFalse(eventMeta.isEmpty());
        assertEquals(2, eventMeta.size());
        MapKeyValueEntry ff = eventMeta.get(0);
        assertEquals("workflow", ff.getKey());
        assertEquals("true", ff.getValue());
        ff = eventMeta.get(1);
        assertEquals("workflow_directory_integration", ff.getKey());
        assertEquals("false", ff.getValue());

        List<MapKeyValueEntry> values = ec.getValues();
        assertFalse(values.isEmpty());
        assertEquals(1, values.size());
        MapKeyValueEntry entry = values.get(0);
        assertEquals("userId", entry.getKey());
        assertEquals("f7d7e7c5-1a1f-4d2a-9ae0-ce07e83907fa", entry.getValue());
    }
}
