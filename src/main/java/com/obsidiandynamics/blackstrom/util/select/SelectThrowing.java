package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

public final class SelectThrowing<T, R> extends Select<T, R> {
  SelectThrowing(T value) {
    super(value);
  }
  
  public ThenThrowing<T, T, R> when(Predicate<? super T> predicate) {
    return new ThenThrowing<>(this, value, test(predicate));
  }
  
  public NullThenThrowing<T, R> whenNull() {
    return new NullThenThrowing<>(this, test(isNull()));
  }
  
  public <E> ThenThrowing<T, E, R> whenInstanceOf(Class<E> type) {
    return new ThenThrowing<>(this, cast(value), test(instanceOf(type)));
  }
  
  public SelectThrowing<T, R> otherwise(Consumer<T> action) {
    return otherwise().then(action);
  }
  
  public SelectThrowing<T, R> otherwiseReturn(Function<T, R> action) {
    return otherwise().thenReturn(action);
  }
  
  public ThenThrowing<T, T, R> otherwise() {
    return when(alwaysTrue());
  }
}
