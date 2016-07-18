package com.bettercloud.kadmin.api.kafka;

/**
 * Created by davidesposito on 7/18/16.
 */
public class SchemaRegistryRestException extends Exception {

    public SchemaRegistryRestException(String message) {
        super(message);
    }

    public SchemaRegistryRestException(String message, Throwable cause) {
        super(message, cause);
    }
}
