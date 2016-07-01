package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.kadmin.api.kafka.KafkaMessageConverter;
import com.bettercloud.kadmin.api.kafka.KafkaProviderService;
import com.bettercloud.kadmin.api.models.KafkaProduceMessageMeta;
import com.bettercloud.messaging.kafka.produce.ProducerService;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by davidesposito on 7/1/16.
 */
@RestController
@RequestMapping("/kafka/send")
public class KafkaMessageProducerResource {

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
    public ResponseEntity<Object> send(@RequestBody KafkaProduceRequestModel requestModel) {
        KafkaProduceMessageMeta meta = requestModel.getMeta();
        GenericRecord message = converter.convert(requestModel.getRawMessage(), meta);
        ProducerService<String, Object> ps = providerService.producerService(meta.getKafkaUrl(), meta.getSchemaRegistryUrl());
        ps.send(meta.getTopic(), message);
        return ResponseEntity.ok(true);
    }
}
