package com.bettercloud.kadmin.api.models;

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
public class KafkaProduceMessageMeta {

    private String schema;
    private String rawSchema;
    private String topic;
}
