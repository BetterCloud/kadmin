package com.bettercloud.kadmin.api.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.function.Function;

/**
 * Created by davidesposito on 7/21/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeserializerInfoModel implements Model {

    private String id;
    private String name;
    private String className;
}
