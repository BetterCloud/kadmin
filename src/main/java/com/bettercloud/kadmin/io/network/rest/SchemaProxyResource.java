package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.logger.services.LogLevel;
import com.bettercloud.logger.services.Logger;
import com.bettercloud.logger.services.model.LogModel;
import com.bettercloud.util.LoggerUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by davidesposito on 7/6/16.
 */
@RestController
@RequestMapping(path = "/schemas")
public class SchemaProxyResource {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOGGER = LoggerUtils.get(SchemaProxyResource.class);

    @Value("${schema.registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;

    private final HttpClient client;

    @Autowired
    public SchemaProxyResource(HttpClient defaultClient) {
        this.client = defaultClient;
    }

    @RequestMapping(
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<List<String>> schemas() {
        String url = String.format("%s/subjects", this.schemaRegistryUrl);
        NodeConverter<List<String>> c = (node) -> {
            if (node.isArray()) {
                ArrayNode arr = (ArrayNode) node;
                return StreamSupport.stream(arr.spliterator(), false)
                        .map(n -> n.asText())
                        .collect(Collectors.toList());
            }
            return null;
        };
        return proxyResponse(url, c, null);
    }

    @RequestMapping(
            path = "/{name}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<SchemaInfo> info(@PathVariable("name") String name) {
        String url = String.format("%s/subjects/%s/versions", this.schemaRegistryUrl, name);
        NodeConverter<List<Integer>> c = (node) -> {
            if (node.isArray()) {
                ArrayNode arr = (ArrayNode) node;
                return StreamSupport.stream(arr.spliterator(), false)
                        .map(n -> n.asInt())
                        .collect(Collectors.toList());
            }
            return null;
        };
        List<Integer> versions = null;
        JsonNode currSchema = null;
        int statusCode = 200;
        ResponseEntity<List<Integer>> versionsRes = proxyResponse(url, c, null);
        if (versionsRes.getStatusCode().is2xxSuccessful()) {
            versions = versionsRes.getBody();
            ResponseEntity<JsonNode> info = version(name, versions.get(versions.size() - 1));
            if (info.getStatusCode().is2xxSuccessful()) {
                currSchema = info.getBody();
            } else {
                statusCode = info.getStatusCode().value();
            }
        } else {
            statusCode = versionsRes.getStatusCode().value();
        }
        if (statusCode != 200) {
            return ResponseEntity.status(statusCode).body(SchemaInfo.builder()
                    .name(name)
                    .build());
        }
        return ResponseEntity.ok(SchemaInfo.builder()
                .name(name)
                .versions(versions)
                .currSchema(currSchema)
                .build());
    }

    @RequestMapping(
            path = "/{name}/{version}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<JsonNode> version(@PathVariable("name") String name,
                                                  @PathVariable("version") Integer version) {
        String url = String.format("%s/subjects/%s/versions/%d",
                this.schemaRegistryUrl,
                name,
                version
        );
        return proxyResponse(url, n -> n, null);
    }

    private <ResponseT> ResponseEntity<ResponseT> proxyResponse(String url, NodeConverter<ResponseT> c, ResponseT defaultVal) {
        HttpGet get = new HttpGet(url);
        try {
            HttpResponse res = client.execute(get);
            int statusCode = res.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                LOGGER.log(LogModel.error("Non 200 status: {}")
                        .addArg(statusCode)
                        .build());
                return ResponseEntity.status(statusCode).body(defaultVal);
            }
            ResponseT val = c.convert(mapper.readTree(res.getEntity().getContent()));
            if (val == null) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(defaultVal);
            }
            return ResponseEntity.ok(val);
        } catch (IOException e) {
            LOGGER.log(LogModel.error("There was an error: {}")
                    .addArg(e.getMessage())
                    .error(e)
                    .build());
            e.printStackTrace();
        }
        LOGGER.log(LogModel.error("There was an unknown error").build());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(defaultVal);
    }

    private <ResponseT> ResponseT proxy(String url, NodeConverter<ResponseT> c) {
        HttpGet get = new HttpGet(url);
        try {
            HttpResponse res = client.execute(get);
            int statusCode = res.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                LOGGER.log(LogModel.error("Non 200 status: {}")
                        .addArg(statusCode)
                        .build());
                return null;
            }
            ResponseT val = c.convert(mapper.readTree(res.getEntity().getContent()));
            return val;
        } catch (IOException e) {
            LOGGER.log(LogModel.error("There was an error: {}")
                    .args(e.getMessage())
                    .error(e)
                    .build());
            e.printStackTrace();
        }
        LOGGER.log(LogModel.error("There was an unknown error").build());
        return null;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SchemaInfo {
        private String name;
        private List<Integer> versions;
        private JsonNode currSchema;
    }

    public interface NodeConverter<ToT> extends Converter<JsonNode, ToT> { }

    public interface Converter<FromT, ToT> {
        ToT convert(FromT o);
    }
}
