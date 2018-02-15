package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class ValueThen<T, E, R> {
  private final Select<T, R> select;
  private final E value;
  private final boolean fire;

  ValueThen(Select<T, R> select, E value, boolean fire) {
    this.select = select;
    this.value = value;
    this.fire = fire;
  }
  
  public Select<T, R> then(Consumer<E> action) {
    return thenReturn(value -> {
      action.accept(value);
      return null;
    });
  }
  
  public Select<T, R> thenReturn(Function<E, R> action) {
    if (fire) {
      select.setReturn(action.apply(value));
    }
    return select;
  }
  
  public <U> ValueThen<T, U, R> transform(Function<E, U> transform) {
    final U newValue = fire ? transform.apply(value) : null;
    return new ValueThen<>(select, newValue, fire);
  }
  
  public final class Checked {
    Checked() {}
    
    public Select<T, R> then(ThrowingConsumer<E> action) throws Exception {
      return thenReturn(value -> {
        action.accept(value);
        return null;
      });
    }
    
    public Select<T, R> thenReturn(ThrowingFunction<E, R> action) throws Exception {
      if (fire) {
        select.setReturn(action.apply(value));
      }
      return select;
    }
    
    public <U> ValueThen<T, U, R> transform(ThrowingFunction<E, U> transform) throws Exception {
      final U newValue = fire ? transform.apply(value) : null;
      return new ValueThen<>(select, newValue, fire);
    }
  }
  
  public <U> Checked checked() {
    return new Checked();
  }
}
