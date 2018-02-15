package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

public final class Select<T, R> {
  private final T value;
  
  private boolean consumed;
  
  private R returnValue;
  
  private Select(T value) {
    this.value = value;
  }
  
  public ValueThen<T, T, R> when(Predicate<? super T> predicate) {
    return new ValueThen<>(this, value, test(predicate));
  }
  
  public NullThen<T, R> whenNull() {
    return new NullThen<>(this, test(isNull()));
  }
  
  public <E> ValueThen<T, E, R> whenInstanceOf(Class<E> type) {
    return when(instanceOf(type)).transform(obj -> type.cast(obj));
  }
  
  public Select<T, R> otherwise(Consumer<T> action) {
    return otherwise().then(action);
  }
  
  public Select<T, R> otherwiseReturn(Function<T, R> action) {
    return otherwise().thenReturn(action);
  }
  
  public ValueThen<T, T, R> otherwise() {
    return when(alwaysTrue());
  }
  
  private final boolean test(Predicate<? super T> predicate) {
    if (consumed) {
      return false;
    } else {
      consumed = predicate.test(value);
      return consumed;
    }
  }
  
  final void setReturn(R returnValue) {
    this.returnValue = returnValue;
  }
  
  public final R getReturn() {
    return returnValue;
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
  
  public static final class WithReturn<R> {
    public <T> Select<T, R> from(T value) {
      return new Select<>(value);
    }
  }
  
  public static final <R> WithReturn<R> withReturn() {
    return withReturn(null);
  }
  
  public static final <R> WithReturn<R> withReturn(Class<R> type) {
    return new WithReturn<>();
  }
  
  public static final <T, R> Select<T, R> from(T value) {
    return new Select<>(value);
  }
}
