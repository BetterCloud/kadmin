package com.bettercloud.kadmin.io.network.rest;

import ch.qos.logback.classic.Level;
import com.bettercloud.kadmin.api.kafka.JsonToAvroConverter;
import com.bettercloud.kadmin.api.kafka.KadminProducer;
import com.bettercloud.kadmin.api.kafka.KadminProducerConfig;
import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import com.bettercloud.kadmin.api.services.SerializerRegistryService;
import com.bettercloud.kadmin.io.network.dto.KafkaProduceMessageMetaModel;
import com.bettercloud.kadmin.io.network.dto.KafkaProduceRequestModel;
import com.bettercloud.kadmin.io.network.dto.ProducerInfoModel;
import com.bettercloud.kadmin.io.network.dto.ProducerRepsonseModel;
import com.bettercloud.kadmin.io.network.rest.utils.ResponseUtil;
import com.bettercloud.kadmin.kafka.avro.ErrorTolerantAvroObjectDeserializer;
import com.bettercloud.kadmin.services.BasicKafkaProducerProviderService;
import com.bettercloud.util.LoggerUtils;
import com.bettercloud.util.Opt;
import com.bettercloud.util.Page;
import com.bettercloud.util.TimedWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;
import org.apache.avro.AvroTypeException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/1/16.
 */
@RestController
@RequestMapping("/api")
public class KafkaProducerResource {

    private static final Logger LOGGER = LoggerUtils.get(KafkaProducerResource.class, Level.TRACE);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BasicKafkaProducerProviderService basicProducerProvider;
    private final JsonToAvroConverter jtaConverter;
    private final SerializerRegistryService serializerRegistryService;

    @Autowired
    public KafkaProducerResource(BasicKafkaProducerProviderService basicProducerProvider,
                                 JsonToAvroConverter jtaConverter,
                                 SerializerRegistryService serializerRegistryService) {
        this.basicProducerProvider = basicProducerProvider;
        this.jtaConverter = jtaConverter;
        this.serializerRegistryService = serializerRegistryService;
    }

    @RequestMapping(
            path = "/avro/publish",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<ProducerRepsonseModel> publishAvro(@RequestBody KafkaProduceRequestModel requestModel,
                                                         @RequestParam("count") Optional<Integer> oCount) {
        Opt<JsonNode> rawMessage;
        try {
            rawMessage = Opt.of(MAPPER.readTree(requestModel.getRawMessage()));
            rawMessage.notPresent(() -> { throw new RuntimeException("Could not parse raw message"); });
        } catch (Exception e) {
            return ResponseUtil.error(e);
        }
        KafkaProduceMessageMetaModel meta = requestModel.getMeta();
        Object message;
        try {
            message = jtaConverter.convert(rawMessage.get().toString(), meta.getRawSchema());
        } catch (AvroTypeException e) {
            return ResponseUtil.error(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
        KadminProducer<String, Object> producer = basicProducerProvider.get(KadminProducerConfig.builder()
                .kafkaHost(meta.getKafkaUrl())
                .schemaRegistryUrl(meta.getSchemaRegistryUrl())
                .topic(meta.getTopic())
                .keySerializer(StringSerializer.class.getName())
                .valueSerializer(ErrorTolerantAvroObjectDeserializer.class.getName())
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
        }
        return ResponseEntity.ok(res);
    }

    @RequestMapping(
            path = "/kafka/publish",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<ProducerRepsonseModel> publish(@RequestBody KafkaProduceRequestModel requestModel,
                                                         @RequestParam("count") Optional<Integer> oCount) {
        if (requestModel.getMeta().getSerializerId() == null) {
            return ResponseUtil.error("Missing serializer id", HttpStatus.BAD_REQUEST);
        }
        SerializerInfoModel ser = serializerRegistryService.findById(requestModel.getMeta().getSerializerId());
        if (ser == null) {
            return ResponseUtil.error("Invalid serializer id", HttpStatus.BAD_REQUEST);
        }
        KafkaProduceMessageMetaModel meta = requestModel.getMeta();
        KadminProducer<String, Object> producer = basicProducerProvider.get(KadminProducerConfig.builder()
                .kafkaHost(meta.getKafkaUrl())
                .schemaRegistryUrl(meta.getSchemaRegistryUrl())
                .topic(meta.getTopic())
                .keySerializer(StringSerializer.class.getName())
                .valueSerializer(ser.getClassName())
                .build());
        Object message = ser.getPrepareRawFunc().apply(requestModel.getRawMessage());
        boolean sendMessage = requestModel.getRawMessage() != null && producer != null;
        ProducerRepsonseModel res = ProducerRepsonseModel.builder()
                .sent(sendMessage)
                .count(0)
                .success(false)
                .duration(-1)
                .rate(-1)
                .build();
        if (sendMessage) {
            res = sendMessage(producer, requestModel.getKey(), message, oCount.orElse(1));
        }
        return ResponseEntity.ok(res);
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
        List<ProducerInfoModel> content = basicProducerProvider.findAll().getContent().stream()
                .map(p -> {
                    return ProducerInfoModel.builder()
                            .id(p.getId())
                            .topic(p.getConfig().getTopic())
                            .lastUsedTime(basicProducerProvider.findById(p.getId()).getLastUsedTime())
                            .totalErrors(p.getErrorCount())
                            .totalMessagesSent(p.getSentCount())
                            .build();
                })
                .collect(Collectors.toList());
        page.setContent(content);
        page.setPage(0);
        page.setSize(content.size());
        page.setTotalElements(basicProducerProvider.count());
        return ResponseEntity.ok(page);
    }

    @RequestMapping(
            path = "/manager/producers/{id}",
            method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<ProducerInfoModel> killProducer(@PathVariable("id") String producerId) {
        Opt<KadminProducer<String, Object>> producer = Opt.of(basicProducerProvider.findById(producerId));
        if (!producer.isPresent()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
        KadminProducer<String, Object> p = producer.get();

        basicProducerProvider.dispose(producerId);

        return ResponseEntity.ok(ProducerInfoModel.builder()
                .id(producerId)
                .topic(p.getConfig().getTopic())
                .lastUsedTime(producer.get().getLastUsedTime())
                .totalErrors(p.getErrorCount())
                .totalMessagesSent(p.getSentCount())
                .build());
    }

    @Data
    @Builder
    private static class ProducerMetrics {
        private final long sentCount;
        private final long errorCount;
    }
}
