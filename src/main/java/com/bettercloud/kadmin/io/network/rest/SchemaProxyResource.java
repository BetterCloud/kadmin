package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.kadmin.api.kafka.exception.SchemaRegistryRestException;
import com.bettercloud.kadmin.api.services.IntrospectionService;
import com.bettercloud.kadmin.api.services.SchemaRegistryService;
import com.bettercloud.kadmin.io.network.dto.SchemaInfoModel;
import com.bettercloud.util.LoggerUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Created by davidesposito on 7/6/16.
 */
@RestController
@RequestMapping(path = "/api")
public class SchemaProxyResource {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOGGER = LoggerUtils.get(SchemaProxyResource.class);

    private final SchemaRegistryService schemaRegistryService;
    private final IntrospectionService introspectionService;

    @Autowired
    public SchemaProxyResource(SchemaRegistryService schemaRegistryService, IntrospectionService introspectionService) {
        this.schemaRegistryService = schemaRegistryService;
        this.introspectionService = introspectionService;
    }

    @RequestMapping(
            path = "/schemas",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<List<String>> schemas(@RequestParam("url") Optional<String> oUrl) {
        try {
            return ResponseEntity.ok(schemaRegistryService.findAll(oUrl.orElse(null)));
        } catch (SchemaRegistryRestException e) {
            return ResponseEntity.status(e.getStatusCode())
                    .header("error-message", e.getMessage())
                    .body(null);
        }
    }

    @RequestMapping(
            path = "/topics",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Collection<String>> topics(@RequestParam("kafka-url") Optional<String> kafkaUrl) {
        try {
            return ResponseEntity.ok(introspectionService.getAllTopicNames(kafkaUrl));
        } catch (Exception e) {
            return ResponseEntity.status(500)
                    .header("error-message", e.getMessage())
                    .body(null);
        }
    }

    @RequestMapping(
            path = "/schemas/{name}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<SchemaInfoModel> info(@PathVariable("name") String name,
                                                @RequestParam("url") Optional<String> oUrl) {
        try {
            return ResponseEntity.ok(schemaRegistryService.getInfo(name, oUrl.orElse(null)));
        } catch (SchemaRegistryRestException e) {
            return ResponseEntity.status(e.getStatusCode())
                    .header("error-message", e.getMessage())
                    .body(null);
        }
    }

    @RequestMapping(
            path = "/schemas/{name}/{version}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<JsonNode> version(@PathVariable("name") String name,
                                                  @PathVariable("version") Integer version,
                                                  @RequestParam("url") Optional<String> oUrl) {
        try {
            return ResponseEntity.ok(schemaRegistryService.getVersion(name, version, oUrl.orElse(null)));
        } catch (SchemaRegistryRestException e) {
            return ResponseEntity.status(e.getStatusCode())
                    .header("error-message", e.getMessage())
                    .body(null);
        }
    }
}
