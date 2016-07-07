package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.kadmin.api.kafka.JsonToAvroConverter;
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
import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Optional;

/**
 * Created by davidesposito on 7/1/16.
 */
@RestController
@RequestMapping("/kafka")
public class KafkaMessageProducerResource {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageProducerResource.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final KafkaProviderService providerService;
    private final KafkaMessageConverter<GenericRecord> genericRecordKafkaConverter;
    private final JsonToAvroConverter jtaConverter;

    @Autowired
    public KafkaMessageProducerResource(KafkaProviderService providerService,
                                        KafkaMessageConverter<GenericRecord> genericRecordKafkaConverter,
                                        JsonToAvroConverter jtaConverter) {
        this.providerService = providerService;
        this.genericRecordKafkaConverter = genericRecordKafkaConverter;
        this.jtaConverter = jtaConverter;
    }

    @RequestMapping(
            path = "/send",
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
        GenericRecord message = genericRecordKafkaConverter.convert(requestModel.getRawMessage(), meta);
        ProducerService<String, Object> ps = providerService.producerService(meta.getKafkaUrl(), meta.getSchemaRegistryUrl());
        boolean sendMessage = message != null && ps != null;
        ProducerResponse res = ProducerResponse.builder()
                .sent(sendMessage)
                .count(0)
                .success(false)
                .duration(-1)
                .rate(-1)
                .build();
        if (sendMessage) {
            res = sendMessage(ps, meta.getTopic(), message, oCount.orElse(1));
        }
        logger.log(LogLevel.INFO, "Produced: {}", res);
        return ResponseEntity.ok(res);
    }

    @RequestMapping(
            path = "/publish",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<ProducerResponse> publish(@RequestBody KafkaProduceRequestModel requestModel,
                                                 @RequestParam("count") Optional<Integer> oCount) {
        if (requestModel.getRawMessage().getNodeType().equals(JsonNodeType.STRING)) {
            try {
                requestModel.setRawMessage(mapper.readTree(requestModel.getRawMessage().asText()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        KafkaProduceMessageMeta meta = requestModel.getMeta();
        Object message;
        try {
            message = jtaConverter.convert(requestModel.getRawMessage().toString(), meta.getRawSchema());
        } catch (AvroTypeException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(null);
        }
        ProducerService<String, Object> ps = providerService.producerService(meta.getKafkaUrl(), meta.getSchemaRegistryUrl());
        boolean sendMessage = message != null && ps != null;
        ProducerResponse res = ProducerResponse.builder()
                .sent(sendMessage)
                .count(0)
                .success(false)
                .duration(-1)
                .rate(-1)
                .build();
        if (sendMessage) {
            res = sendMessage(ps, meta.getTopic(), message, oCount.orElse(1));
        }
        logger.log(LogLevel.INFO, "Produced: {}", res);
        return ResponseEntity.ok(res);
    }

    private <PayloadT> ProducerResponse sendMessage(ProducerService<String, PayloadT> ps, String topic,
                                                    PayloadT payload, int count) {
        int success = 0;
        long duration;
        double rate;
        long startTime = System.currentTimeMillis();
        for (int i=0;i<count;i++) {
            try {
                ps.send(topic, payload);
                success++;
            } catch (Exception e) { }
        }
        duration = System.currentTimeMillis() - startTime;
        rate = count * 1000.0 / duration;
        return ProducerResponse.builder()
                .count(success)
                .duration(duration)
                .rate(rate)
                .sent(true)
                .success(success > 0 && success == count)
                .build();
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
