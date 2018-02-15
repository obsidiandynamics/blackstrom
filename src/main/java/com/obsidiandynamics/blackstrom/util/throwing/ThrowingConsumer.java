package com.obsidiandynamics.blackstrom.util.throwing;

import java.util.*;

@FunctionalInterface
public interface ThrowingConsumer<T> {
  void accept(T t) throws Exception;

  default ThrowingConsumer<T> andThen(ThrowingConsumer<? super T> after) {
    Objects.requireNonNull(after);
    return t -> { 
      accept(t); after.accept(t);
    };
  }
}
