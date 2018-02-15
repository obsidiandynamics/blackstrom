package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

import com.obsidiandynamics.indigo.util.*;

public final class ThenThrowing<T, E, R> {
  private final SelectThrowing<T, R> select;
  private final E value;
  private final boolean fire;

  ThenThrowing(SelectThrowing<T, R> select, E value, boolean fire) {
    this.select = select;
    this.value = value;
    this.fire = fire;
  }
  
  public SelectThrowing<T, R> then(ThrowingConsumer<E> action) throws Exception {
    return then(value -> {
      action.accept(value);
      return null;
    });
  }
  
  public SelectThrowing<T, R> then(ThrowingFunction<E, R> action) throws Exception {
    if (fire) {
      select.setReturn(action.apply(value));
    }
    return select;
  }
  
  public <U> ThenThrowing<T, U, R> transform(Function<E, U> transform) {
    final U newValue = fire ? transform.apply(value) : null;
    return new ThenThrowing<>(select, newValue, fire);
  }
}
