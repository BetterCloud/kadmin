package com.bettercloud.util;

/**
 * Created by davidesposito on 7/11/16.
 */
public class TimedWrapper<DataT> {

    private long lastUsed;
    private final DataT data;

    private TimedWrapper(DataT data) {
        this.data = data;
        this.lastUsed = System.currentTimeMillis();
    }

    public DataT getData() {
        return getData(true);
    }

    public DataT getData(boolean used) {
        if (used) {
            lastUsed = System.currentTimeMillis();
        }
        return data;
    }

    public long getLastUsed() {
        return lastUsed;
    }

    public long getIdleTime() {
        return System.currentTimeMillis() - getLastUsed();
    }

    public static <StaticDataT> TimedWrapper<StaticDataT> of(StaticDataT data) {
        return new TimedWrapper<>(data);
    }
}