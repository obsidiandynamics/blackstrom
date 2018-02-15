package com.obsidiandynamics.blackstrom.util.select;

import com.obsidiandynamics.indigo.util.*;

public final class NullThenThrowing<T> {
  private final SelectThrowing<T> select;
  private final boolean fire;

  NullThenThrowing(SelectThrowing<T> select, boolean fire) {
    this.select = select;
    this.fire = fire;
  }
  
  public SelectThrowing<T> then(ThrowingRunnable action) throws Exception {
    if (fire) {
      action.run();
    }
    return select;
  }
}
