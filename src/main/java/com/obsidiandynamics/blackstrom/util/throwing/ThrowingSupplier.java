package com.obsidiandynamics.blackstrom.util.throwing;

@FunctionalInterface
public interface ThrowingSupplier<T> {
  T get() throws Exception;
}