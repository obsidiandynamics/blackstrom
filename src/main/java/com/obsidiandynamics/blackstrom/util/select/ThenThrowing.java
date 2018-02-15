package com.obsidiandynamics.blackstrom.util.select;

import com.obsidiandynamics.indigo.util.*;

public final class ThenThrowing<T, E> {
  private final SelectThrowing<T> select;
  private final E value;
  private final boolean fire;

  ThenThrowing(SelectThrowing<T> select, E value, boolean fire) {
    this.select = select;
    this.value = value;
    this.fire = fire;
  }
  
  public SelectThrowing<T> then(ThrowingConsumer<E> action) throws Exception {
    if (fire) {
      action.accept(value);
    }
    return select;
  }
}
