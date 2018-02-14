package com.obsidiandynamics.blackstrom.util.select;

import com.obsidiandynamics.indigo.util.*;

public final class NullThenThrowing<T> {
  private final boolean fire;

  NullThenThrowing(boolean fire) {
    this.fire = fire;
  }
  
  public void then(ThrowingRunnable action) throws Exception {
    if (fire) {
      action.run();
    }
  }
}
