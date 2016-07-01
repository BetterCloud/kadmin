package com.bettercloud.kadmin.testing;

import com.bettercloud.avro.Header;
import com.bettercloud.avro.workflow.EventCall;
import com.bettercloud.avro.workflow.MapKeyValueEntry;
import com.bettercloud.kadmin.WorkflowProperties;
import com.bettercloud.kadmin.api.kafka.KafkaProviderService;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Created by davidesposito on 6/29/16.
 */
@Service
public class TestListener {

    @Autowired
    private KafkaProviderService kpService;

    public void run() {
        kpService.producerService().send(WorkflowProperties.TOPIC_LIST_TO_CONTEXT, EventCall.newBuilder()
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
        //context_workflow_payload
    }
}
