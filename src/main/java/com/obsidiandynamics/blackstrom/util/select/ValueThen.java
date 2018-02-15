package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class ValueThen<S extends SelectRoot<R>, T, E, R> {
  private final S select;
  private final E value;
  private final boolean fire;

  ValueThen(S select, E value, boolean fire) {
    this.select = select;
    this.value = value;
    this.fire = fire;
  }
  
  public S then(Consumer<E> action) {
    return thenReturn(value -> {
      action.accept(value);
      return null;
    });
  }
  
  public S thenReturn(Function<E, R> action) {
    if (fire) {
      select.setReturn(action.apply(value));
    }
    return select;
  }
  
  public <U> ValueThen<S, T, U, R> transform(Function<E, U> transform) {
    final U newValue = fire ? transform.apply(value) : null;
    return new ValueThen<>(select, newValue, fire);
  }
  
  public final class Checked {
    Checked() {}
    
    public S then(ThrowingConsumer<E> action) throws Exception {
      return thenReturn(value -> {
        action.accept(value);
        return null;
      });
    }
    
    public S thenReturn(ThrowingFunction<E, R> action) throws Exception {
      if (fire) {
        select.setReturn(action.apply(value));
      }
      return select;
    }
    
    public <U> ValueThen<S, T, U, R>.Checked transform(ThrowingFunction<E, U> transform) throws Exception {
      final U newValue = fire ? transform.apply(value) : null;
      return new ValueThen<S, T, U, R>(select, newValue, fire).checked();
    }
  }
  
  <U> Checked checked() {
    return new Checked();
  }
}
