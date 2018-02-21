package com.obsidiandynamics.blackstrom.util.throwing;

@FunctionalInterface
public interface ThrowingConsumer<T> extends CheckedConsumer<T, Exception> {}
