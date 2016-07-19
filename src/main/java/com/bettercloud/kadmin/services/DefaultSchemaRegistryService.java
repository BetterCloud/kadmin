package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.kafka.SchemaRegistryRestException;
import com.bettercloud.kadmin.api.models.SchemaInfo;
import com.bettercloud.kadmin.api.services.SchemaRegistryService;
import com.bettercloud.kadmin.io.network.rest.SchemaProxyResource;
import com.bettercloud.logger.services.Logger;
import com.bettercloud.logger.services.model.LogModel;
import com.bettercloud.util.LoggerUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by davidesposito on 7/18/16.
 */
@Service
public class DefaultSchemaRegistryService implements SchemaRegistryService {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerUtils.get(SchemaProxyResource.class);

    private final String schemaRegistryUrl;

    private final HttpClient client;

    @Autowired
    public DefaultSchemaRegistryService(HttpClient defaultClient,
                @Value("${schema.registry.url:http://localhost:8081}")
                String schemaRegistryUrl) {
        this.client = defaultClient;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public List<String> findAll(Optional<String> oUrl) throws SchemaRegistryRestException {
        String url = String.format("%s/subjects",
                oUrl.orElse(this.schemaRegistryUrl)
        );
        NodeConverter<List<String>> c = (node) -> {
            if (node.isArray()) {
                ArrayNode arr = (ArrayNode) node;
                return StreamSupport.stream(arr.spliterator(), false)
                        .map(n -> n.asText())
                        .sorted()
                        .collect(Collectors.toList());
            }
            return null;
        };
        return proxyResponse(url, c, null);
    }

    @Override
    public List<String> guessAllTopics(Optional<String> oUrl) throws SchemaRegistryRestException {
        return findAll(oUrl).stream()
                .map(schemaName -> schemaName.replaceAll("-value", ""))
                .collect(Collectors.toList());
    }

    @Override
    public SchemaInfo getInfo(String name, Optional<String> oUrl) throws SchemaRegistryRestException {
        String url = String.format("%s/subjects/%s/versions",
                oUrl.orElse(this.schemaRegistryUrl),
                name
        );
        NodeConverter<List<Integer>> c = (node) -> {
            if (node.isArray()) {
                ArrayNode arr = (ArrayNode) node;
                return StreamSupport.stream(arr.spliterator(), false)
                        .map(n -> n.asInt())
                        .collect(Collectors.toList());
            }
            return null;
        };
        List<Integer> versions = proxyResponse(url, c, null);
        JsonNode info = getVersion(name, versions.get(versions.size() - 1), oUrl);
        JsonNode currSchema = info;
        return SchemaInfo.builder()
                .name(name)
                .versions(versions)
                .currSchema(currSchema)
                .build();
    }

    @Override
    public JsonNode getVersion(String name, int version, Optional<String> oUrl) throws SchemaRegistryRestException {
        String url = String.format("%s/subjects/%s/versions/%d",
                oUrl.orElse(this.schemaRegistryUrl),
                name,
                version
        );
        return proxyResponse(url, n -> n, null);
    }

    private <ResponseT> ResponseT proxyResponse(String url, NodeConverter<ResponseT> c, ResponseT defaultVal)
            throws SchemaRegistryRestException {
        HttpGet get = new HttpGet(url);
        try {
            HttpResponse res = client.execute(get);
            int statusCode = res.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                LOGGER.log(LogModel.error("Non 200 status: {}")
                        .addArg(statusCode)
                        .build());
                throw new SchemaRegistryRestException("Non 200 status: " + statusCode, statusCode);
            }
            ResponseT val = c.convert(MAPPER.readTree(res.getEntity().getContent()));
            if (val == null) {
                return defaultVal;
            }
            return val;
        } catch (IOException e) {
            LOGGER.log(LogModel.error("There was an error: {}")
                    .addArg(e.getMessage())
                    .error(e)
                    .build());
            throw new SchemaRegistryRestException(e.getMessage(), e, 500);
        }
    }

    public interface NodeConverter<ToT> extends Converter<JsonNode, ToT> { }

    public interface Converter<FromT, ToT> {
        ToT convert(FromT o);
    }
}
