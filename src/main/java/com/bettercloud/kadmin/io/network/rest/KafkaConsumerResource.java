package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.kadmin.api.kafka.KadminConsumerConfig;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroup;
import com.bettercloud.kadmin.api.kafka.KadminConsumerGroupContainer;
import com.bettercloud.kadmin.api.models.DeserializerInfoModel;
import com.bettercloud.kadmin.api.services.BundledKadminConsumerGroupProviderService;
import com.bettercloud.kadmin.api.services.DeserializerRegistryService;
import com.bettercloud.kadmin.api.services.FeaturesService;
import com.bettercloud.kadmin.io.network.rest.utils.ResponseUtil;
import com.bettercloud.kadmin.kafka.QueuedKafkaMessageHandler;
import com.bettercloud.util.Page;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/5/16.
 */
@RestController
@RequestMapping("/api")
public class KafkaConsumerResource {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Joiner keyBuilder = Joiner.on(':');

    private final BundledKadminConsumerGroupProviderService kafkaConsumerProvider;
    private final DeserializerRegistryService deserializerRegistryService;
    private final FeaturesService featuresService;

    @Autowired
    public KafkaConsumerResource(BundledKadminConsumerGroupProviderService kafkaConsumerProvider,
                                 DeserializerRegistryService deserializerRegistryService,
                                 FeaturesService featuresService) {
        this.kafkaConsumerProvider = kafkaConsumerProvider;
        this.deserializerRegistryService = deserializerRegistryService;
        this.featuresService = featuresService;

    }

    @RequestMapping(
            path = "/kafka/read/{topic}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<ConsumerResponse> readKafka(@PathVariable("topic") String topic,
                                                     @RequestParam("since") Optional<Long> oSince,
                                                     @RequestParam("window") Optional<Long> oWindow,
                                                     @RequestParam("kafkaUrl") Optional<String> kafkaUrl,
                                                     @RequestParam("schemaUrl") Optional<String> schemaUrl,
                                                     @RequestParam("size") Optional<Integer> queueSize,
                                                     @RequestParam("deserializerId") String deserializerId) {
        DeserializerInfoModel des = deserializerRegistryService.findById(deserializerId);
        if (des == null) {
            return ResponseUtil.error("Invalid deserializer id", HttpStatus.NOT_FOUND);
        }
        KadminConsumerConfig config = KadminConsumerConfig.builder()
                .topic(topic)
                .kafkaHost(featuresService.getCustomUrl(kafkaUrl.orElse(null)))
                .schemaRegistryUrl(featuresService.getCustomUrl(schemaUrl.orElse(null)))
                .keyDeserializer(StringDeserializer.class.getName())
                .valueDeserializer(des)
                .build();
        int maxSize = queueSize.filter(s -> s < 100).orElse(50);
        KadminConsumerGroupContainer consumerGroupContainer = kafkaConsumerProvider.get(config, true, maxSize);

        QueuedKafkaMessageHandler handler = consumerGroupContainer.getQueue();
        Long since = getSince(oSince, oWindow);
        Page<JsonNode> page = null;
        try {
            List<JsonNode> messages = handler.get(since).stream()
                    .map(m -> (QueuedKafkaMessageHandler.MessageContainer) m)
                    .map(mc -> {
                        ObjectNode node = mapper.createObjectNode();
                        node.put("key", mc.getKey());
                        node.put("writeTime", mc.getWriteTime());
                        node.put("offset", mc.getOffset());
                        node.put("topic", mc.getTopic());
                        node.replace("message", des.getPrepareOutputFunc().apply(mc.getMessage()));
                        return node;
                    })
                    .map(n -> (JsonNode) n)
                    .collect(Collectors.toList());
            page = new Page<>();
            page.setPage(0);
            page.setSize(messages.size());
            page.setTotalElements(handler.total());
            page.setContent(messages);
        } catch (RuntimeException e) {
            e.printStackTrace();
            return ResponseUtil.error(e);
        }
        return ResponseEntity.ok(ConsumerResponse.builder()
                .consumerId(consumerGroupContainer.getId())
                .page(page)
                .build());
    }

    protected long getSince(Optional<Long> oSince, Optional<Long> oWindow) {
        return oSince.isPresent() ? oSince.get() :
                oWindow.map(win -> System.currentTimeMillis() - win * 1000).orElse(-1L);
    }

    @Data
    @Builder
    private static class ConsumerResponse {
        private final String consumerId;
        private final Page<JsonNode> page;
    }
}
