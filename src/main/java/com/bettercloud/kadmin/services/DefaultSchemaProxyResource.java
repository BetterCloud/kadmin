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
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.util.List;

/**
 * Created by davidesposito on 7/18/16.
 */
public class DefaultSchemaProxyResource implements SchemaRegistryService {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerUtils.get(SchemaProxyResource.class);

    private final String schemaRegistryUrl;

    private final HttpClient client;

    @Autowired
    public DefaultSchemaProxyResource(HttpClient defaultClient,
                               @Value("${schema.registry.url:http://localhost:8081}")
                               String schemaRegistryUrl) {
        this.client = defaultClient;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public List<String> findAll() {
        return null;
    }

    @Override
    public SchemaInfo getInfo(String name) {
        return null;
    }

    @Override
    public JsonNode getVersion(String name, int version) {
        return null;
    }

    private <ResponseT> ResponseT proxyResponse(String url,
                                                NodeConverter<ResponseT> c,
                                                ResponseT defaultVal)
            throws SchemaRegistryRestException {
        HttpGet get = new HttpGet(url);
        try {
            HttpResponse res = client.execute(get);
            int statusCode = res.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                LOGGER.log(LogModel.error("Non 200 status: {}")
                        .addArg(statusCode)
                        .build());
                throw new SchemaRegistryRestException("Non 200 status: " + statusCode);
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
            throw new SchemaRegistryRestException("There was an error ")
        }
        LOGGER.log(LogModel.error("There was an unknown error").build());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(defaultVal);
    }

    public interface NodeConverter<ToT> extends Converter<JsonNode, ToT> { }

    public interface Converter<FromT, ToT> {
        ToT convert(FromT o);
    }
}
