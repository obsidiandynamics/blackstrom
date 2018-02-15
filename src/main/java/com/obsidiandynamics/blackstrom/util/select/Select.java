package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

public abstract class Select<T, R> {
  protected final T value;
  
  protected boolean consumed;
  
  private R returnValue;
  
  protected Select(T value) {
    this.value = value;
  }
  
  protected final boolean test(Predicate<? super T> predicate) {
    if (consumed) {
      return false;
    } else {
      consumed = predicate.test(value);
      return consumed;
    }
  }
  
  protected final void setReturn(R returnValue) {
    this.returnValue = returnValue;
  }
  
  public final R getReturn() {
    return returnValue;
  }
  
  @SuppressWarnings("unchecked")
  protected static final <T> T cast(Object obj) {
    return (T) obj;
  }
  
  public static final <T> Predicate<T> isNull() {
    return v -> v == null;
  }
  
  public static final <T> Predicate<T> isNotNull() {
    return not(isNull());
  }
  
  public static final <T> Predicate<T> not(Predicate<T> positive) {
    return v -> ! positive.test(v);
  }
  
  public static final <T> Predicate<T> instanceOf(Class<?> type) {
    return v -> type.isInstance(v);
  }
  
  public static final <T> Predicate<T> alwaysTrue() {
    return v -> true;
  }
  
  public static final class WithReturnBuilder<R> {
    public <T> SelectThrowing<T, R> fromThrowing(T value) {
      return new SelectThrowing<>(value);
    }
  }
  
  public static final <R> WithReturnBuilder<R> withReturn() {
    return withReturn(null);
  }
  
  public static final <R> WithReturnBuilder<R> withReturn(Class<R> type) {
    return new WithReturnBuilder<>();
  }
  
  public static final <T, R> SelectThrowing<T, R> fromThrowing(T value) {
    return new SelectThrowing<>(value);
  }
}
