package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class ThenThrowing<T, E, R> {
  private final SelectThrowing<T, R> select;
  private final E value;
  private final boolean fire;

  ThenThrowing(SelectThrowing<T, R> select, E value, boolean fire) {
    this.select = select;
    this.value = value;
    this.fire = fire;
  }
  
  public SelectThrowing<T, R> then(Consumer<E> action) {
    return thenReturn(value -> {
      action.accept(value);
      return null;
    });
  }
  
  public SelectThrowing<T, R> thenReturn(Function<E, R> action) {
    if (fire) {
      select.setReturn(action.apply(value));
    }
    return select;
  }
  
  public <U> ThenThrowing<T, U, R> transform(Function<E, U> transform) {
    final U newValue = fire ? transform.apply(value) : null;
    return new ThenThrowing<>(select, newValue, fire);
  }
  
  public final class ThrowingBuilder {
    ThrowingBuilder() {}
    
    public SelectThrowing<T, R> then(ThrowingConsumer<E> action) throws Exception {
      return thenReturn(value -> {
        action.accept(value);
        return null;
      });
    }
    
    public SelectThrowing<T, R> thenReturn(ThrowingFunction<E, R> action) throws Exception {
      if (fire) {
        select.setReturn(action.apply(value));
      }
      return select;
    }
    
    public <U> ThenThrowing<T, U, R> transform(ThrowingFunction<E, U> transform) throws Exception {
      final U newValue = fire ? transform.apply(value) : null;
      return new ThenThrowing<>(select, newValue, fire);
    }
  }
  
  public <U> ThrowingBuilder checked() {
    return new ThrowingBuilder();
  }
}
