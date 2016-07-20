package com.bettercloud.util;

import lombok.*;

import java.util.List;

/**
 * Created by davidesposito on 7/20/16.
 */
@Data
@NoArgsConstructor
public class Page<T> {

    private List<T> content;
    private int page;
    private int size;
    private long totalElements;
}
