package com.bettercloud.kadmin.api.kafka;

/**
 * Created by davidesposito on 7/11/16.
 */
public interface AvrifyConverter {

    String avrify(String json, String schema);
}
