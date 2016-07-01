package com.bettercloud.kadmin.io.network.rest;

import com.bettercloud.kadmin.api.models.KafkaProduceMessageMeta;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by davidesposito on 7/1/16.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaProduceRequestModel {

    private JsonNode rawMessage;
    private KafkaProduceMessageMeta meta;
}
