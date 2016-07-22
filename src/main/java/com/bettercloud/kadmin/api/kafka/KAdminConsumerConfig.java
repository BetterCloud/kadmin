package com.bettercloud.kadmin.api.kafka;

import com.bettercloud.kadmin.api.models.DeserializerInfoModel;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Created by davidesposito on 7/19/16.
 */
@Data
@Builder
public class KadminConsumerConfig {

    @NonNull private final String topic;
    private String kafkaHost;
    private String schemaRegistryUrl;
    private String keyDeserializer;
    private DeserializerInfoModel valueDeserializer;
}
