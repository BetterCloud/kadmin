package com.bettercloud.kadmin.api.kafka.exception;

/**
 * Created by davidesposito on 7/18/16.
 */
public class SchemaRegistryRestException extends Exception {

    private final int statusCode;

    public SchemaRegistryRestException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public SchemaRegistryRestException(String message, Throwable cause, int statusCode) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
