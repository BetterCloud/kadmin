package com.bettercloud.kadmin.api;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Created by davidesposito on 7/1/16.
 */
public interface MessageConverter<ConversionMetaT, ModelT> {

    ModelT convert(JsonNode rawMessage, ConversionMetaT meta);
}
