package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

public abstract class Select<T> {
  protected final T value;
  
  protected boolean consumed;
  
  protected Select(T value) {
    this.value = value;
  }
  
  protected final boolean test(Predicate<? super T> predicate) {
    return ! consumed && predicate.test(value);
  }
  
  @SuppressWarnings("unchecked")
  protected static <T> T cast(Object obj) {
    return (T) obj;
  }
  
  public static <T> Predicate<T> isNull() {
    return v -> v == null;
  }
  
  public static <T> Predicate<T> isNotNull() {
    return not(isNull());
  }
  
  public static <T> Predicate<T> not(Predicate<T> positive) {
    return v -> ! positive.test(v);
  }
  
  public static <T> Predicate<T> instanceOf(Class<?> type) {
    return v -> type.isInstance(v);
  }
  
  public static <T> Predicate<T> alwaysTrue() {
    return v -> true;
  }
  
  public static <T> SelectThrowing<T> fromThrowing(T value) {
    return new SelectThrowing<>(value);
  }
}
