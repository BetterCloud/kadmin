package com.bettercloud.util;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by davidesposito on 7/8/16.
 */
public class Opt<T> {

    private static final Runnable NOOP = () -> { };

    private final T o;

    private Opt(T o) {
        this.o = o;
    }

    public boolean isPresent() {
        return o != null;
    }

    public T get() {
        return o;
    }

    public T orElse(T other) {
        return isPresent() ? o : other;
    }

    public Opt<T> ifPresent(Consumer<T> _if) {
        if (isPresent()) {
            _if.accept(get());
        }
        return this;
    }

    public Opt<T> ifPresent(Consumer<T> _if, Runnable _else) {
        if (isPresent()) {
            _if.accept(get());
        } else {
            _else.run();
        }
        return this;
    }

    public void notPresent(Runnable _else) {
        if (!isPresent()) {
            _else.run();
        }
    }

    public <MappingT> Opt<MappingT> map(Function<T, MappingT> _map) {
        return isPresent() ? Opt.of(_map.apply(get())) : Opt.empty();
    }

    public <MappingT> Opt<MappingT> flatMap(Function<T, Opt<MappingT>> _map) {
        return isPresent() ? Opt.of(_map.apply(get()).get()) : Opt.empty();
    }

    public Opt<T> filter(Function<T, Boolean> _filter) {
        return isPresent() && _filter.apply(get()) ? this : Opt.empty();
    }

    public Opt<T> filter(Function<T, Boolean> _filter, Consumer<T> _else) {
        boolean filtered = isPresent() && _filter.apply(get());
        if (filtered) {
            _else.accept(get());
        }
        return filtered ? this : Opt.empty();
    }

    public static <StaticT> Opt<StaticT> empty() {
        return of(null);
    }

    public static <StaticT> Opt<StaticT> of(StaticT o) {
        return new Opt<>(o);
    }
}
