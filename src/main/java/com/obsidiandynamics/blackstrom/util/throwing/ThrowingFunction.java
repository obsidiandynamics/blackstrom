package com.obsidiandynamics.blackstrom.util.throwing;

@FunctionalInterface
public interface ThrowingFunction<T, R> extends CheckedFunction<T, R, Exception> {}