package com.bettercloud.kadmin.api.services;

import com.bettercloud.kadmin.api.models.SchemaInfo;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * Created by davidesposito on 7/18/16.
 */
public interface SchemaRegistryService {

    List<String> findAll();

    SchemaInfo getInfo(String name);

    JsonNode getVersion(String name, int version);
}
