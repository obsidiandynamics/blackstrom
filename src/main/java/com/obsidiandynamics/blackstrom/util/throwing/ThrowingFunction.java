package com.obsidiandynamics.blackstrom.util.throwing;

import java.util.*;

@FunctionalInterface
public interface ThrowingFunction<T, R> {
  R apply(T t) throws Exception;

  default <V> ThrowingFunction<V, R> compose(ThrowingFunction<? super V, ? extends T> before) {
    Objects.requireNonNull(before);
    return v -> apply(before.apply(v));
  }

  default <V> ThrowingFunction<T, V> andThen(ThrowingFunction<? super R, ? extends V> after) {
    Objects.requireNonNull(after);
    return t -> after.apply(apply(t));
  }
}