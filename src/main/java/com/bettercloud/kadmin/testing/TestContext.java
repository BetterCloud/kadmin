package com.bettercloud.kadmin.testing;

import com.bettercloud.avro.Header;
import com.bettercloud.avro.workflow.ContextPayload;
import com.bettercloud.avro.workflow.MapKeyValueEntry;
import com.bettercloud.kadmin.WorkflowProperties;
import com.bettercloud.kadmin.api.kafka.KafkaProviderService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.UUID;

/**
 * Created by davidesposito on 6/29/16.
 */
@Service
public class TestContext {

    @Autowired
    private KafkaProviderService kpService;

    public void run2943() {
        HashMap<String, Object> params = Maps.newHashMap();
        params.put("userId", UUID.randomUUID().toString());
        kpService.producerService().send(WorkflowProperties.TOPIC_LIST_TO_CONTEXT, ContextPayload.newBuilder()
                .setHeader(Header.newBuilder()
                        .setCorrelationId(UUID.randomUUID().toString())
                        .setDomainId("testing")
                        .setEventId(UUID.randomUUID().toString())
                        .setEventMeta(Lists.newArrayList(
                                MapKeyValueEntry.newBuilder()
                                        .setKey("workflow")
                                        .setValue(true)
                                        .build(),
                                MapKeyValueEntry.newBuilder()
                                        .setKey("workflow_clouddirectory_integration")
                                        .setValue(false)
                                        .build()
                        ))
                        .setExternalCorrelationId(UUID.randomUUID().toString())
                        .setProviderId("google")
                        .setReceivedDate(System.currentTimeMillis())
                        .setTenantId(UUID.randomUUID().toString())
                        .setSenderId(null)
                        .setUserId(UUID.randomUUID().toString())
                        .build())
                .setEventParams(params)
                .setWorkflowId(UUID.randomUUID().toString())
                .setWorkflowType("EVENT")
                .build());
    }
}
