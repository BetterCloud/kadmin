package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.kadmin.api.kafka.KafkaProviderService;
import com.bettercloud.kadmin.kafka.QueuedKafkaMessageHandler;
import com.bettercloud.logger.services.LogLevel;
import com.bettercloud.logger.services.Logger;
import com.bettercloud.logger.services.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import javafx.collections.ObservableMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
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
    private static final Joiner keyBuilder = Joiner.on(':');

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
                                               @RequestParam("window") Optional<Long> oWindow,
                                               @RequestParam("kafkaUrl") Optional<String> kafkaUrl,
                                               @RequestParam("schemaUrl") Optional<String> schemaUrl,
                                               @RequestParam("size") Optional<Integer> queueSize) {
        String key = keyBuilder.join(kafkaUrl.orElse("default"), schemaUrl.orElse("default"), topic);
        if (!handlerMap.containsKey(key)) {
            Integer maxSize = queueSize.orElse(25);
            QueuedKafkaMessageHandler queue = new QueuedKafkaMessageHandler(maxSize);
            handlerMap.put(key, queue);
            kps.consumerService(queue, topic, kafkaUrl.orElse(null), schemaUrl.orElse(null)).start();
        }
        QueuedKafkaMessageHandler handler = handlerMap.get(key);
        Long since = getSince(oSince, oWindow);
        PageImpl<JsonNode> page = new PageImpl<>(
                handler.get(since).stream()
                        .map(m -> (QueuedKafkaMessageHandler.MessageContainer)m)
                        .map(mc -> {
                            ObjectNode node = mapper.createObjectNode();
                            node.put("key", mc.getKey());
                            node.put("writeTime", mc.getWriteTime());
                            /*
                             * There appears to be some incompatibility with JSON serializing Avro models. So,
                             *
                             * disgusting hack start...
                             */
                            JsonNode message = null;
                            try {
                                message = mapper.readTree(mc.getMessage().toString());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            node.replace("message", message);
                            // disgusting hack end
                            return node;
                        })
                        .collect(Collectors.toList()
                        ),
                new PageRequest(0, 25), handler.total());
        return ResponseEntity.ok(page);
    }

    @RequestMapping(
            path = "/{topic}",
            method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Boolean> clear(@PathVariable("topic") String topic,
                                               @RequestParam("kafkaUrl") Optional<String> kafkaUrl,
                                               @RequestParam("schemaUrl") Optional<String> schemaUrl) {
        String key = keyBuilder.join(kafkaUrl.orElse("default"), schemaUrl.orElse("default"), topic);
        boolean cleared = false;
        if (handlerMap.containsKey(key) && handlerMap.get(key) != null) {
            handlerMap.get(key).clear();
            cleared = true;
        }
        return ResponseEntity.ok(cleared);
    }

    protected long getSince(Optional<Long> oSince, Optional<Long> oWindow) {
        return oSince.isPresent() ? oSince.get() :
                oWindow.map(win -> System.currentTimeMillis() - win * 1000).orElse(-1L);
    }
}
