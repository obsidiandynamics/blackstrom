package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

public final class SelectThrowing<T> extends Select<T> {
  SelectThrowing(T value) {
    super(value);
  }
  
  public ThenThrowing<T> when(Predicate<? super T> predicate) {
    return new ThenThrowing<>(value, test(predicate));
  }
  
  public NullThenThrowing<T> whenNull() {
    return new NullThenThrowing<>(test(isNull()));
  }
  
  public <X> ThenThrowing<X> whenInstanceOf(Class<X> type) {
    return new ThenThrowing<>(cast(value), test(instanceOf(type)));
  }
  
  public ThenThrowing<T> otherwise() {
    return when(alwaysTrue());
  }
}
