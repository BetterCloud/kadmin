package com.bettercloud.kadmin.api.kafka.model;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.Properties;

/**
 * Created by davidesposito on 7/19/16.
 */
@Data
@Builder
public class KafkaConsumerProperties {

    @NonNull private final Properties properties;
    @NonNull private final String groupId;
    @NonNull private final String clientId;
}
