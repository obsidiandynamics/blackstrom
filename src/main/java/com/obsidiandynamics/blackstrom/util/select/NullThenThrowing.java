package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class NullThenThrowing<T, R> {
  private final SelectThrowing<T, R> select;
  private final boolean fire;

  NullThenThrowing(SelectThrowing<T, R> select, boolean fire) {
    this.select = select;
    this.fire = fire;
  }
  
  public SelectThrowing<T, R> then(Runnable action) {
    return thenReturn(() -> {
      action.run();
      return null;
    });
  }
  
  public SelectThrowing<T, R> thenReturn(Supplier<R> action) {
    if (fire) {
      select.setReturn(action.get());
    }
    return select;
  }
  
  public final class ThrowingBuilder {
    ThrowingBuilder() {}
    
    public SelectThrowing<T, R> then(ThrowingRunnable action) throws Exception {
      return thenReturn(() -> {
        action.run();
        return null;
      });
    }
    
    public SelectThrowing<T, R> thenReturn(ThrowingSupplier<R> action) throws Exception {
      if (fire) {
        select.setReturn(action.get());
      }
      return select;
    }
  }
  
  public ThrowingBuilder checked() {
    return new ThrowingBuilder();
  }
}
