package com.bettercloud.kadmin.api.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Created by davidesposito on 7/21/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SerializerInfoModel {

    private String id;
    private String name;
    private String className;
    private Map<String, Object> meta;
}
