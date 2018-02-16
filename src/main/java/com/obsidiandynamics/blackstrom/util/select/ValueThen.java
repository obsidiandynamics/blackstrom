package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class ValueThen<S extends SelectRoot<R>, V, R> {
  private final S select;
  private final V value;
  private final boolean fire;

  ValueThen(S select, V value, boolean fire) {
    this.select = select;
    this.value = value;
    this.fire = fire;
  }
  
  public S then(Consumer<V> action) {
    return thenReturn(value -> {
      action.accept(value);
      return null;
    });
  }
  
  public S thenReturn(Function<V, R> action) {
    if (fire) {
      select.setReturn(action.apply(value));
    }
    return select;
  }
  
  public <W> ValueThen<S, W, R> transform(Function<V, W> transform) {
    final W newValue = fire ? transform.apply(value) : null;
    return new ValueThen<>(select, newValue, fire);
  }
  
  public final class Checked {
    Checked() {}
    
    public <X extends Exception> S then(CheckedConsumer<V, X> action) throws X {
      return thenReturn(value -> {
        action.accept(value);
        return null;
      });
    }
    
    public <X extends Exception> S thenReturn(CheckedFunction<V, R, X> action) throws X {
      if (fire) {
        select.setReturn(action.apply(value));
      }
      return select;
    }
    
    public <W, X extends Exception> ValueThen<S, W, R>.Checked transform(CheckedFunction<V, W, X> transform) throws X {
      final W newValue = fire ? transform.apply(value) : null;
      return new ValueThen<>(select, newValue, fire).checked();
    }
  }
  
  <U> Checked checked() {
    return new Checked();
  }
}
