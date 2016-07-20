package com.bettercloud.kadmin.api.services;

import com.bettercloud.kadmin.api.kafka.exception.SchemaRegistryRestException;
import com.bettercloud.kadmin.io.network.dto.SchemaInfo;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Optional;

/**
 * Created by davidesposito on 7/18/16.
 */
public interface SchemaRegistryService {

    List<String> findAll(Optional<String> oUrl) throws SchemaRegistryRestException;

    List<String> guessAllTopics(Optional<String> oUrl) throws SchemaRegistryRestException;

    SchemaInfo getInfo(String name, Optional<String> oUrl) throws SchemaRegistryRestException;

    JsonNode getVersion(String name, int version, Optional<String> oUrl) throws SchemaRegistryRestException;
}
