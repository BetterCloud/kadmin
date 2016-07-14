package com.bettercloud.util;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by davidesposito on 7/8/16.
 */
public class StreamUtils {

    public static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
        return asStream(sourceIterator, false);
    }

    public static <T> Stream<T> asStream(Iterator<T> sourceIterator, boolean parallel) {
        return asStream(() -> sourceIterator, parallel);
    }

    public static <T> Stream<T> asStream(Iterable<T> sourceIterable) {
        return asStream(sourceIterable, false);
    }

    public static <T> Stream<T> asStream(Iterable<T> sourceIterable, boolean parallel) {
        return StreamSupport.stream(sourceIterable.spliterator(), parallel);
    }
}
