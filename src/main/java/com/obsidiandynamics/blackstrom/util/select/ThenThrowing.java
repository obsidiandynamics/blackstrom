package com.obsidiandynamics.blackstrom.util.select;

import com.obsidiandynamics.indigo.util.*;

public final class ThenThrowing<T> {
  private final T value;
  private final boolean fire;

  ThenThrowing(T value, boolean fire) {
    this.value = value;
    this.fire = fire;
  }
  
  public void then(ThrowingConsumer<T> action) throws Exception {
    if (fire) {
      action.accept(value);
    }
  }
}
