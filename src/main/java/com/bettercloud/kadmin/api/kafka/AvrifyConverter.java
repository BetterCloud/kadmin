package com.bettercloud.kadmin.api.kafka;

import org.apache.avro.generic.GenericRecord;

/**
 * Created by davidesposito on 7/11/16.
 */
public interface AvrifyConverter {

    String avrify(String json, String schema);
}
