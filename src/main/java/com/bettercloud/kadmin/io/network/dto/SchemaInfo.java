package com.bettercloud.kadmin.io.network.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Created by davidesposito on 7/18/16.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaInfo {
    private String name;
    private List<Integer> versions;
    private JsonNode currSchema;
}
