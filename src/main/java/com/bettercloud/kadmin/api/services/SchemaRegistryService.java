package com.bettercloud.kadmin.api.services;

import com.bettercloud.kadmin.api.kafka.exception.SchemaRegistryRestException;
import com.bettercloud.kadmin.io.network.dto.SchemaInfoModel;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Optional;

/**
 * Created by davidesposito on 7/18/16.
 */
public interface SchemaRegistryService {

    List<String> findAll(String oUrl) throws SchemaRegistryRestException;

    List<String> guessAllTopics(String oUrl) throws SchemaRegistryRestException;

    SchemaInfoModel getInfo(String name, String oUrl) throws SchemaRegistryRestException;

    JsonNode getVersion(String name, int version, String oUrl) throws SchemaRegistryRestException;
}
