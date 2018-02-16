package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class NullThen<S extends SelectRoot<R>, R> {
  private final S select;
  private final boolean fire;

  NullThen(S select, boolean fire) {
    this.select = select;
    this.fire = fire;
  }
  
  public S then(Runnable action) {
    return thenReturn(() -> {
      action.run();
      return null;
    });
  }
  
  public S thenReturn(Supplier<R> action) {
    if (fire) {
      select.setReturn(action.get());
    }
    return select;
  }
  
  public final class Checked {
    Checked() {}
    
    public <X extends Exception> S then(CheckedRunnable<X> action) throws X {
      return thenReturn(() -> {
        action.run();
        return null;
      });
    }
    
    public <X extends Exception> S thenReturn(CheckedSupplier<R, X> action) throws X {
      if (fire) {
        select.setReturn(action.get());
      }
      return select;
    }
  }
  
  Checked checked() {
    return new Checked();
  }
}
