package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

import com.obsidiandynamics.indigo.util.*;

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
  
  public SelectThrowing<T, R> otherwise(ThrowingConsumer<T> action) throws Exception {
    return otherwise().then(action);
  }
  
  public SelectThrowing<T, R> otherwise(ThrowingFunction<T, R> action) throws Exception {
    return otherwise().then(action);
  }
  
  public ThenThrowing<T, T, R> otherwise() {
    return when(alwaysTrue());
  }
}
