package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.kadmin.api.kafka.exception.SchemaRegistryRestException;
import com.bettercloud.kadmin.io.network.dto.SchemaInfoModel;
import com.bettercloud.kadmin.api.services.SchemaRegistryService;

import com.bettercloud.util.LoggerUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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

    @Autowired
    public SchemaProxyResource(SchemaRegistryService schemaRegistryService) {
        this.schemaRegistryService = schemaRegistryService;
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
    public ResponseEntity<List<String>> topics(@RequestParam("url") Optional<String> oUrl) {
        try {
            return ResponseEntity.ok(schemaRegistryService.guessAllTopics(oUrl.orElse(null)));
        } catch (SchemaRegistryRestException e) {
            return ResponseEntity.status(e.getStatusCode())
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
