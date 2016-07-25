package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.kafka.exception.SchemaRegistryRestException;
import com.bettercloud.kadmin.api.services.FeaturesService;
import com.bettercloud.kadmin.io.network.dto.SchemaInfoModel;
import com.bettercloud.kadmin.api.services.SchemaRegistryService;
import com.bettercloud.kadmin.io.network.rest.SchemaProxyResource;
import com.bettercloud.util.LoggerUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
    private final FeaturesService featuresService;

    private final Cache<String, List<String>> schemasCache;
    private final Cache<String, SchemaInfoModel> schemaInfoCache;
    private final Cache<String, JsonNode> schemaVersionCache;

    @Autowired
    public DefaultSchemaRegistryService(HttpClient defaultClient,
                @Value("${schema.registry.url:http://localhost:8081}")
                String schemaRegistryUrl,
                FeaturesService featuresService) {
        this.client = defaultClient;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.featuresService = featuresService;
        schemasCache = defaultCache();
        schemaInfoCache = defaultCache();
        schemaVersionCache = defaultCache();
    }

    private <ValueT> Cache<String, ValueT> defaultCache() {
        return CacheBuilder.newBuilder()
                .expireAfterAccess(30, TimeUnit.SECONDS)
                .expireAfterWrite(90, TimeUnit.SECONDS)
                .build();
    }

    @Override
    public List<String> findAll(String url) throws SchemaRegistryRestException {
        url = String.format("%s/subjects",
                Optional.ofNullable(featuresService.getCustomUrl(url)).orElse(this.schemaRegistryUrl)
        );
        if (schemasCache.getIfPresent(url) == null) {
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
            schemasCache.put(url, proxyResponse(url, c, null));
        } else {
            LOGGER.debug("Hit schema cache for: {}", url);
        }
        return schemasCache.getIfPresent(url);
    }

    @Override
    public List<String> guessAllTopics(String oUrl) throws SchemaRegistryRestException {
        return findAll(oUrl).stream()
                .map(schemaName -> schemaName.replaceAll("-value", ""))
                .collect(Collectors.toList());
    }

    @Override
    public SchemaInfoModel getInfo(String name, String url) throws SchemaRegistryRestException {
        url = String.format("%s/subjects/%s/versions",
                Optional.ofNullable(featuresService.getCustomUrl(url)).orElse(this.schemaRegistryUrl),
                name
        );
        if (schemaInfoCache.getIfPresent(url) == null) {
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
            JsonNode info = getVersion(name, versions.get(versions.size() - 1), url);
            JsonNode currSchema = info;
            schemaInfoCache.put(url, SchemaInfoModel.builder()
                    .name(name)
                    .versions(versions)
                    .currSchema(currSchema)
                    .build());
        } else {
            LOGGER.debug("Hit info cache for: {}", url);
        }
        return schemaInfoCache.getIfPresent(url);
    }

    @Override
    public JsonNode getVersion(String name, int version, String url) throws SchemaRegistryRestException {
        url = String.format("%s/subjects/%s/versions/%d",
                Optional.ofNullable(featuresService.getCustomUrl(url)).orElse(this.schemaRegistryUrl),
                name,
                version
        );
        if (schemaVersionCache.getIfPresent(url) == null) {
            schemaVersionCache.put(url, proxyResponse(url, n -> n, null));
        } else {
            LOGGER.debug("Hit version cache for: {}", url);
        }
        return schemaVersionCache.getIfPresent(url);
    }

    private <ResponseT> ResponseT proxyResponse(String url, NodeConverter<ResponseT> c, ResponseT defaultVal)
            throws SchemaRegistryRestException {
        HttpGet get = new HttpGet(url);
        try {
            HttpResponse res = client.execute(get);
            int statusCode = res.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                LOGGER.error("Non 200 status: {}", statusCode);
                throw new SchemaRegistryRestException("Non 200 status: " + statusCode, statusCode);
            }
            ResponseT val = c.convert(MAPPER.readTree(res.getEntity().getContent()));
            if (val == null) {
                return defaultVal;
            }
            return val;
        } catch (IOException e) {
            LOGGER.error("There was an error: {}", e.getMessage());
            throw new SchemaRegistryRestException(e.getMessage(), e, 500);
        }
    }

    public interface NodeConverter<ToT> extends Converter<JsonNode, ToT> { }

    public interface Converter<FromT, ToT> {
        ToT convert(FromT o);
    }
}
