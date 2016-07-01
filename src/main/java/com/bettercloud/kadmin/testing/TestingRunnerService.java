package com.bettercloud.kadmin.testing;

import com.bettercloud.avro.Header;
import com.bettercloud.avro.workflow.EventCall;
import com.bettercloud.avro.workflow.MapKeyValueEntry;
import com.bettercloud.kadmin.api.kafka.KafkaProviderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Created by davidesposito on 6/29/16.
 */
@Service
public class TestingRunnerService {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private KafkaProviderService kpService;

    public void run() {
        kpService.producerService().send("testing123", "It Worked!");
        kpService.producerService().send("testing123", "And again!");
        kpService.producerService().send("testing123", "Uno mas...");
        Schema classSchema = EventCall.getClassSchema();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println(classSchema);
        System.out.println();
        System.out.println();
        System.out.println();
        kpService.producerService().send("testing123", EventCall.newBuilder()
                .setHeader(Header.newBuilder()
                        .setCorrelationId(UUID.randomUUID().toString())
                        .setDomainId("testing")
                        .setEventId(UUID.randomUUID().toString())
                        .setExternalCorrelationId(UUID.randomUUID().toString())
                        .setProviderId("google")
                        .setReceivedDate(System.currentTimeMillis())
                        .setTenantId(UUID.randomUUID().toString())
                        .setSenderId(null)
                        .setUserId(UUID.randomUUID().toString())
                        .build())
                .setEventId(UUID.randomUUID().toString())
                .setValues(Lists.newArrayList(
                        MapKeyValueEntry.newBuilder()
                                .setKey("workflowUser")
                                .setValue(Lists.newArrayList(
                                        MapKeyValueEntry.newBuilder()
                                                .setKey("googleUser")
                                                .setValue(Lists.newArrayList(
                                                        MapKeyValueEntry.newBuilder()
                                                                .setKey("userId")
                                                                .setValue(UUID.randomUUID().toString())
                                                                .build(),
                                                        MapKeyValueEntry.newBuilder()
                                                                .setKey("email")
                                                                .setValue("user@test.ing")
                                                                .build()
                                                ))
                                                .build()
                                ))
                                .build()
                ))
                .build());

        System.out.println();
        System.out.println();
        System.out.println();

        ObjectNode ec = mapper.createObjectNode();
        ObjectNode header = ec.putObject("header");
        header.put("correlationId", UUID.randomUUID().toString());
        header.put("domainId", "testing");
        header.put("eventId", UUID.randomUUID().toString());
        header.put("externalCorrelationId", UUID.randomUUID().toString());
        header.put("providerId", "google");
        header.put("receivedDate", System.currentTimeMillis());
        header.put("tenantId", UUID.randomUUID().toString());
        header.put("senderId", (String)null);
        header.put("userId", UUID.randomUUID().toString());
        ec.put("eventId", UUID.randomUUID().toString());
        ObjectNode workflowUser = ec.putArray("values").addObject();
        workflowUser.put("key", "workflowUser");
        ObjectNode googleUser = workflowUser.putArray("value").addObject();
        googleUser.put("key", "googleUser");
        ArrayNode props = googleUser.putArray("value");
        ObjectNode guProp = props.addObject();
        guProp.put("key", "userId");
        guProp.put("value", UUID.randomUUID().toString());
        guProp = props.addObject();
        guProp.put("key", "email");
        guProp.put("email", "user@test.ing");
        System.out.println(ec);
//		EventCall ec1 = mapper.treeToValue(ec, EventCall.class);
//		kpService.producerService().send("testing123", ec1);
        EventCall ec2 = mapper.convertValue(ec, EventCall.class);
        kpService.producerService().send("testing123", ec2);
    }
}
