package com.obsidiandynamics.blackstrom.util.throwing;

@FunctionalInterface
public interface CheckedSupplier<T, X extends Exception> {
  T get() throws X;
}