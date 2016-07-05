package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.kadmin.api.kafka.KafkaProviderService;
import com.bettercloud.kadmin.kafka.QueuedKafkaMessageHandler;
import com.bettercloud.logger.services.LogLevel;
import com.bettercloud.logger.services.Logger;
import com.bettercloud.logger.services.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import javafx.collections.ObservableMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/5/16.
 */
@RestController
@RequestMapping("/kafka/read")
public class KafkaMessageConsumerResource {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageConsumerResource.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final KafkaProviderService kps;
    private final Map<String, QueuedKafkaMessageHandler> handlerMap;

    @Autowired
    public KafkaMessageConsumerResource(KafkaProviderService kps) {
        this.kps = kps;
        this.handlerMap = Maps.newHashMap();
    }

    @RequestMapping(
            path = "/{topic}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Page<JsonNode>> read(@PathVariable("topic") String topic,
                                             @RequestParam("since") Optional<Long> oSince,
                                             @RequestParam("window") Optional<Long> oWindow) {
        if (!handlerMap.containsKey(topic)) {
            QueuedKafkaMessageHandler queue = new QueuedKafkaMessageHandler(100, 5 * 60 * 1000);// 5 minutes
            handlerMap.put(topic, queue);
            kps.consumerService(queue, topic).start();
        }
        QueuedKafkaMessageHandler handler = handlerMap.get(topic);
        Long since = getSince(oSince, oWindow);
        logger.log(LogLevel.INFO, "Requesting message({}) since {}", handler.count(), since);
        PageImpl<JsonNode> page = new PageImpl<>(handler.get(since).stream()
                .map(Object::toString)
                .map(json -> {
                    JsonNode node = null;
                    try {
                        node = mapper.readTree(json);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return node;
                })
                .collect(Collectors.toList()));
        page.getContent().forEach(m -> logger.log(LogLevel.INFO, "Retrieved for topic {} - {}: {}", topic, m.getClass().getSimpleName(), m));
        return ResponseEntity.ok(page);
    }

    protected long getSince(Optional<Long> oSince, Optional<Long> oWindow) {
        return oSince.isPresent() ? oSince.get() :
                oWindow.map(win -> System.currentTimeMillis() - win * 1000).orElse(-1L);
    }
}
