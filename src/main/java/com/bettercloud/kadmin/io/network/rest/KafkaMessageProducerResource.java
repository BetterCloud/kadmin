package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.kadmin.api.kafka.KafkaMessageConverter;
import com.bettercloud.kadmin.api.kafka.KafkaProviderService;
import com.bettercloud.kadmin.api.models.KafkaProduceMessageMeta;
import com.bettercloud.logger.services.LogLevel;
import com.bettercloud.logger.services.Logger;
import com.bettercloud.logger.services.LoggerFactory;
import com.bettercloud.messaging.kafka.produce.ProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import lombok.*;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Optional;

/**
 * Created by davidesposito on 7/1/16.
 */
@RestController
@RequestMapping("/kafka/send")
public class KafkaMessageProducerResource {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageProducerResource.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final KafkaProviderService providerService;
    private final KafkaMessageConverter converter;

    @Autowired
    public KafkaMessageProducerResource(KafkaProviderService providerService, KafkaMessageConverter converter) {
        this.providerService = providerService;
        this.converter = converter;
    }

    @RequestMapping(
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<ProducerResponse> send(@RequestBody KafkaProduceRequestModel requestModel,
                                       @RequestParam("count") Optional<Integer> oCount) {
        if (requestModel.getRawMessage().getNodeType().equals(JsonNodeType.STRING)) {
            try {
                requestModel.setRawMessage(mapper.readTree(requestModel.getRawMessage().asText()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        KafkaProduceMessageMeta meta = requestModel.getMeta();
        GenericRecord message = converter.convert(requestModel.getRawMessage(), meta);
        ProducerService<String, Object> ps = providerService.producerService(meta.getKafkaUrl(), meta.getSchemaRegistryUrl());
        boolean sendMessage = message != null && ps != null;
        int count = 0;
        int success = 0;
        long duration = -1;
        double rate = -1;
        if (sendMessage) {
            count = oCount.orElse(1);
            long startTime = System.currentTimeMillis();
            for (int i=0;i<count;i++) {
                try {
                    ps.send(meta.getTopic(), message);
                    success++;
                } catch (Exception e) {
                }
            }
            duration = System.currentTimeMillis() - startTime;
            rate = count * 1000.0 / duration;
        }
        ProducerResponse res = ProducerResponse.builder()
                .count(success)
                .duration(duration)
                .rate(rate)
                .sent(sendMessage)
                .success(success > 0 && success == count)
                .build();
        logger.log(LogLevel.INFO, "Produced: {}", res);
        return ResponseEntity.ok(res);
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProducerResponse {
        private int count;
        private long duration;
        private double rate;
        private boolean success;
        private boolean sent;
    }
}
