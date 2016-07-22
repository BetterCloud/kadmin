package com.bettercloud.kadmin.api.kafka;

import com.bettercloud.kadmin.api.models.SerializerInfoModel;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Created by davidesposito on 7/19/16.
 */
@Data
@Builder
public class KadminProducerConfig {

    @NonNull private final String topic;
    private String kafkaHost;
    private String schemaRegistryUrl;
    private String keySerializer;
    private SerializerInfoModel valueSerializer;
}
