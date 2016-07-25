package com.bettercloud.kadmin.io.network.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by davidesposito on 7/21/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProducerRepsonseModel {
    private int count;
    private long duration;
    private double rate;
    private boolean success;
    private boolean sent;
}
