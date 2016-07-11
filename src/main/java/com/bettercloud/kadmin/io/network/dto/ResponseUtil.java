package com.bettercloud.kadmin.io.network.dto;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Created by davidesposito on 7/11/16.
 */
public class ResponseUtil {

    public static <T> ResponseEntity<T> error(Throwable e) {
        return error(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    public static <T> ResponseEntity<T> error(HttpStatus status) {
        return error("", status);
    }

    public static <T> ResponseEntity<T> error(String message, HttpStatus status) {
        return ResponseEntity.status(status)
                .header("error-message", message)
                .body(null);
    }
}
