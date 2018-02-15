package com.obsidiandynamics.blackstrom.util.select;

import com.obsidiandynamics.indigo.util.*;

public final class NullThenThrowing<T, R> {
  private final SelectThrowing<T, R> select;
  private final boolean fire;

  NullThenThrowing(SelectThrowing<T, R> select, boolean fire) {
    this.select = select;
    this.fire = fire;
  }
  
  public SelectThrowing<T, R> then(ThrowingRunnable action) throws Exception {
    return then(() -> {
      action.run();
      return null;
    });
  }
  
  public SelectThrowing<T, R> then(ThrowingSupplier<R> action) throws Exception {
    if (fire) {
      select.setReturn(action.create());
    }
    return select;
  }
}
