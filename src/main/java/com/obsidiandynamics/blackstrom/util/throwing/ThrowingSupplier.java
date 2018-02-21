package com.obsidiandynamics.blackstrom.util.throwing;

@FunctionalInterface
public interface ThrowingSupplier<T> extends CheckedSupplier<T, Exception> {}