package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

import com.obsidiandynamics.indigo.util.*;

public final class SelectThrowing<T> extends Select<T> {
  SelectThrowing(T value) {
    super(value);
  }
  
  public ThenThrowing<T, T> when(Predicate<? super T> predicate) {
    return new ThenThrowing<>(this, value, test(predicate));
  }
  
  public NullThenThrowing<T> whenNull() {
    return new NullThenThrowing<>(this, test(isNull()));
  }
  
  public <E> ThenThrowing<T, E> whenInstanceOf(Class<E> type) {
    return new ThenThrowing<>(this, cast(value), test(instanceOf(type)));
  }
  
  public SelectThrowing<T> otherwise(ThrowingConsumer<T> action) throws Exception {
    return otherwise().then(action);
  }
  
  public ThenThrowing<T, T> otherwise() {
    return when(alwaysTrue());
  }
}
