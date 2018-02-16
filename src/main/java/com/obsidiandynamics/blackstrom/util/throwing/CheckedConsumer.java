package com.obsidiandynamics.blackstrom.util.throwing;

import java.util.*;

@FunctionalInterface
public interface CheckedConsumer<T, X extends Exception> {
  void accept(T t) throws X;

  default CheckedConsumer<T, X> andThen(CheckedConsumer<? super T, ? extends X> after) {
    Objects.requireNonNull(after);
    return t -> { 
      accept(t); after.accept(t);
    };
  }
  
  /**
   *  A no-op.
   *  
   *  @param <T> Parameter type.
   */
  static <T> void nop(T t) {}
}
