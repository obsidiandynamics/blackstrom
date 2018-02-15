package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class NullThen<T, R> {
  private final Select<T, R> select;
  private final boolean fire;

  NullThen(Select<T, R> select, boolean fire) {
    this.select = select;
    this.fire = fire;
  }
  
  public Select<T, R> then(Runnable action) {
    return thenReturn(() -> {
      action.run();
      return null;
    });
  }
  
  public Select<T, R> thenReturn(Supplier<R> action) {
    if (fire) {
      select.setReturn(action.get());
    }
    return select;
  }
  
  public final class Checked {
    Checked() {}
    
    public Select<T, R> then(ThrowingRunnable action) throws Exception {
      return thenReturn(() -> {
        action.run();
        return null;
      });
    }
    
    public Select<T, R> thenReturn(ThrowingSupplier<R> action) throws Exception {
      if (fire) {
        select.setReturn(action.get());
      }
      return select;
    }
  }
  
  public Checked checked() {
    return new Checked();
  }
}
