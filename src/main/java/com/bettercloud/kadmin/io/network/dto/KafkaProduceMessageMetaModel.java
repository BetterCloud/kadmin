package com.bettercloud.kadmin.io.network.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by davidesposito on 7/1/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaProduceMessageMetaModel {

    private String kafkaUrl;
    private String schemaRegistryUrl;
    private String schema;
    private String rawSchema;
    private String topic;
    private String serializerId;
}
