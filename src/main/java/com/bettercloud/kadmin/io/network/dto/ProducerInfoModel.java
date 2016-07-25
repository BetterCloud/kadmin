package com.bettercloud.kadmin.io.network.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by davidesposito on 7/20/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProducerInfoModel {

    private String id;
    private String topic;
    private long lastUsedTime;
    private long totalMessagesSent;
    private long totalErrors;
}
