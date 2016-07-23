package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.kadmin.api.kafka.KadminConsumerGroup;
import com.bettercloud.kadmin.api.models.DeserializerInfoModel;
import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import com.bettercloud.kadmin.api.services.BundledKadminConsumerGroupProviderService;
import com.bettercloud.kadmin.api.services.DeserializerRegistryService;
import com.bettercloud.kadmin.api.services.SerializerRegistryService;
import com.bettercloud.kadmin.io.network.dto.ConsumerInfoModel;
import com.bettercloud.kadmin.io.network.rest.utils.ResponseUtil;
import com.bettercloud.kadmin.kafka.QueuedKafkaMessageHandler;
import com.bettercloud.util.Opt;
import com.bettercloud.util.Page;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/21/16.
 */
@RestController
@RequestMapping("/api/manager")
public class ManagerResource {

    private final SerializerRegistryService serializerRegistryService;
    private final DeserializerRegistryService deserializerRegistryService;
    private final BundledKadminConsumerGroupProviderService consumerGroupProviderService;

    @Autowired
    public ManagerResource(SerializerRegistryService serializerRegistryService,
                           DeserializerRegistryService deserializerRegistryService,
                           BundledKadminConsumerGroupProviderService consumerGroupProviderService) {
        this.serializerRegistryService = serializerRegistryService;
        this.deserializerRegistryService = deserializerRegistryService;
        this.consumerGroupProviderService = consumerGroupProviderService;
    }

    @RequestMapping(
            path = "/serializers",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Page<SerializerInfoModel>> serializers() {
        return ResponseEntity.ok(serializerRegistryService.findAll());
    }

    @RequestMapping(
            path = "/deserializers",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Page<DeserializerInfoModel>> deserializers() {
        return ResponseEntity.ok(deserializerRegistryService.findAll());
    }

    @RequestMapping(
            path = "/consumers",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Page<ConsumerInfoModel>> getAllConsumers() {
        Page<ConsumerInfoModel> consumers = new Page<>();
        List<ConsumerInfoModel> content = consumerGroupProviderService.findAll(0, 100).getContent().stream()
                .map(container -> {
                    KadminConsumerGroup consumer = container.getConsumer();
                    QueuedKafkaMessageHandler queue = container.getQueue();
                    return ConsumerInfoModel.builder()
                            .consumerGroupId(consumer.getGroupId())
                            .deserializerName(consumer.getConfig().getValueDeserializer().getName())
                            .deserializerId(consumer.getConfig().getValueDeserializer().getId())
                            .lastMessageTime(queue.getLastMessageTime())
                            .lastUsedTime(queue.getLastReadTime())
                            .queueSize(queue.getQueueSize())
                            .topic(consumer.getConfig().getTopic())
                            .total(queue.total())
                            .build();
                })
                .collect(Collectors.toList());
        consumers.setContent(content);
        consumers.setPage(0);
        consumers.setTotalElements(content.size());
        consumers.setTotalElements(content.size());
        return ResponseEntity.ok(consumers);
    }

    @RequestMapping(
            path = "/consumers/{consumerId}",
            method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Boolean> kill(@PathVariable("consumerId") String id) {
        if (!Opt.of(consumerGroupProviderService.dispose(id)).isPresent()) {
            return ResponseUtil.error(HttpStatus.NOT_FOUND);
        }
        return ResponseEntity.ok(true);
    }

    @RequestMapping(
            path = "/consumers/{consumerId}/truncate",
            method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Boolean> truncate(@PathVariable("consumerId") String id) {
        if (!Opt.of(consumerGroupProviderService.dispose(id))
                .ifPresent(container -> container.getQueue().clear())
                .isPresent()) {
            return ResponseUtil.error(HttpStatus.NOT_FOUND);
        }
        return ResponseEntity.ok(true);
    }
}
