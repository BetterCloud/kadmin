package com.bettercloud.kadmin.io.network.rest;

import ch.qos.logback.classic.Level;
import com.bettercloud.kadmin.api.kafka.JsonToAvroConverter;
import com.bettercloud.kadmin.api.kafka.KadminProducer;
import com.bettercloud.kadmin.api.kafka.KadminProducerConfig;
import com.bettercloud.kadmin.api.services.AvroProducerProviderService;
import com.bettercloud.kadmin.io.network.dto.*;
import com.bettercloud.kadmin.io.network.rest.utils.ResponseUtil;
import com.bettercloud.util.LoggerUtils;
import com.bettercloud.util.Opt;
import com.bettercloud.util.Page;
import com.bettercloud.util.TimedWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.Data;
import org.apache.avro.AvroTypeException;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/1/16.
 */
@RestController
@RequestMapping("/api")
public class AvroMessageProducerResource {

    private static final Logger LOGGER = LoggerUtils.get(AvroMessageProducerResource.class, Level.TRACE);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final AvroProducerProviderService producerProvider;
    private final JsonToAvroConverter jtaConverter;
    private final Map<String, ProducerMetrics> metricsMap;

    @Autowired
    public AvroMessageProducerResource(AvroProducerProviderService producerProvider,
                                       JsonToAvroConverter jtaConverter) {
        this.producerProvider = producerProvider;
        this.jtaConverter = jtaConverter;
        metricsMap = Maps.newHashMap();
    }

    @RequestMapping(
            path = "/kafka/publish",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<ProducerRepsonseModel> publish(@RequestBody KafkaProduceRequestModel requestModel,
                                                         @RequestParam("count") Optional<Integer> oCount) {
        if (requestModel.getRawMessage().getNodeType().equals(JsonNodeType.STRING)) {
            try {
                requestModel.setRawMessage(MAPPER.readTree(requestModel.getRawMessage().asText()));
            } catch (IOException e) {
                return ResponseUtil.error(e);
            }
        }
        KafkaProduceMessageMetaModel meta = requestModel.getMeta();
        Object message;
        try {
            message = jtaConverter.convert(requestModel.getRawMessage().toString(), meta.getRawSchema());
        } catch (AvroTypeException e) {
            return ResponseUtil.error(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
        KadminProducer<String, Object> producer = producerProvider.get(KadminProducerConfig.builder()
                .kafkaHost(meta.getKafkaUrl())
                .schemaRegistryUrl(meta.getSchemaRegistryUrl())
                .topic(meta.getTopic())
                .build());
        boolean sendMessage = message != null && producer != null;
        ProducerRepsonseModel res = ProducerRepsonseModel.builder()
                .sent(sendMessage)
                .count(0)
                .success(false)
                .duration(-1)
                .rate(-1)
                .build();
        if (sendMessage) {
            res = sendMessage(producer, requestModel.getKey(), message, oCount.orElse(1));
            updateMetrics(producer.getId(), res, oCount.orElse(1));
        }
        return ResponseEntity.ok(res);
    }

    private void updateMetrics(String producerId, ProducerRepsonseModel res, int total) {
        synchronized (metricsMap) {
            if (!metricsMap.containsKey(producerId)) {
                metricsMap.put(producerId, ProducerMetrics.builder().build());
            }
            ProducerMetrics curr = metricsMap.get(producerId);
            metricsMap.put(producerId, ProducerMetrics.builder()
                    .errorCount(curr.getErrorCount() + total - res.getCount())
                    .sentCount(curr.getSentCount() + res.getCount())
                    .build());
        }
    }

    private <PayloadT> ProducerRepsonseModel sendMessage(KadminProducer<String, PayloadT> producer, String key,
                PayloadT payload, int count) {
        int success = 0;
        long duration;
        double rate;
        long startTime = System.currentTimeMillis();
        for (int i=0;i<count;i++) {
            try {
                producer.send(key, payload);
                success++;
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error("There was an error: {}", e);
            }
        }
        LOGGER.trace("/publish Done sending ({}/{}) on {}", success, count, producer.getConfig().getTopic());
        duration = System.currentTimeMillis() - startTime;
        rate = count * 1000.0 / duration;
        return ProducerRepsonseModel.builder()
                .count(success)
                .duration(duration)
                .rate(rate)
                .sent(true)
                .success(success > 0 && success == count)
                .build();
    }

    @RequestMapping(
            path = "/manager/producers",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Page<ProducerInfoModel>> producers() {
        Page<ProducerInfoModel> page = new Page<>();
        List<ProducerInfoModel> content = producerProvider.findAll().getContent().stream()
                .map(p -> {
                    ProducerMetrics metrics = metricsMap.get(p.getId());
                    return ProducerInfoModel.builder()
                            .id(p.getId())
                            .topic(p.getConfig().getTopic())
                            .lastUsedTime(producerProvider.findById(p.getId()).getLastUsed())
                            .totalErrors(metrics.getErrorCount())
                            .totalMessagesSent(metrics.getSentCount())
                            .build();
                })
                .collect(Collectors.toList());
        page.setContent(content);
        page.setPage(0);
        page.setSize(content.size());
        page.setTotalElements(producerProvider.count());
        return ResponseEntity.ok(page);
    }

    @RequestMapping(
            path = "/manager/producers/{id}",
            method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<ProducerInfoModel> killProducer(@PathVariable("id") String producerId) {
        Opt<TimedWrapper<KadminProducer<String, Object>>> twProducer = Opt.of(producerProvider.findById(producerId));
        if (!twProducer.isPresent()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
        KadminProducer<String, Object> p = twProducer.get().getData();
        ProducerMetrics metrics = metricsMap.get(producerId);

        producerProvider.dispose(producerId);
        metricsMap.remove(producerId);

        return ResponseEntity.ok(ProducerInfoModel.builder()
                .id(producerId)
                .topic(p.getConfig().getTopic())
                .lastUsedTime(twProducer.get().getLastUsed())
                .totalErrors(metrics.getErrorCount())
                .totalMessagesSent(metrics.getSentCount())
                .build());
    }

    @Data
    @Builder
    private static class ProducerMetrics {
        private final long sentCount;
        private final long errorCount;
    }
}
