package com.obsidiandynamics.blackstrom.util.select;

import java.util.function.*;

import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class Select<V, R> implements SelectRoot<R> {
  private final V value;
  
  private boolean consumed;
  
  private R returnValue;
  
  private Select(V value) {
    this.value = value;
  }
  
  public ValueThen<Select<V, R>, V, R> when(Predicate<? super V> predicate) {
    return new ValueThen<>(this, value, test(predicate));
  }
  
  public NullThen<Select<V, R>, R> whenNull() {
    return new NullThen<>(this, test(isNull()));
  }
  
  public <C> ValueThen<Select<V, R>, C, R> whenInstanceOf(Class<C> type) {
    return when(instanceOf(type)).transform(obj -> type.cast(obj));
  }
  
  public Select<V, R> otherwise(Consumer<V> action) {
    return otherwise().then(action);
  }
  
  public Select<V, R> otherwiseReturn(Function<V, R> action) {
    return otherwise().thenReturn(action);
  }
  
  public ValueThen<Select<V, R>, V, R> otherwise() {
    return when(alwaysTrue());
  }
  
  public final class Checked implements SelectRoot<R> {
    Checked() {}
    
    public ValueThen<Select<V, R>.Checked, V, R>.Checked when(Predicate<? super V> predicate) {
      return new ValueThen<>(this, value, test(predicate)).checked();
    }
    
    public NullThen<Select<V, R>.Checked, R>.Checked whenNull() {
      return new NullThen<>(this, test(isNull())).checked();
    }
    
    public <C> ValueThen<Select<V, R>.Checked, C, R>.Checked whenInstanceOf(Class<C> type) {
      return when(instanceOf(type)).transform(obj -> type.cast(obj));
    }
    
    public <X extends Exception> Select<V, R>.Checked otherwise(CheckedConsumer<V, X> action) throws X {
      return otherwise().then(action);
    }
    
    public <X extends Exception> Select<V, R>.Checked otherwiseReturn(CheckedFunction<V, R, X> action) throws X {
      return otherwise().thenReturn(action);
    }
    
    public ValueThen<Select<V, R>.Checked, V, R>.Checked otherwise() {
      return when(alwaysTrue());
    }

    @Override
    public void setReturn(R returnValue) {
      Select.this.setReturn(returnValue);
    }
    
    public R getReturn() {
      return Select.this.getReturn();
    }
  }
  
  public Checked checked() {
    return new Checked();
  }
  
  private final boolean test(Predicate<? super V> predicate) {
    if (consumed) {
      return false;
    } else {
      consumed = predicate.test(value);
      return consumed;
    }
  }
  
  @Override
  public final void setReturn(R returnValue) {
    this.returnValue = returnValue;
  }
  
  public final R getReturn() {
    return returnValue;
  }
  
  public static final <V> Predicate<V> isNull() {
    return v -> v == null;
  }
  
  public static final <V> Predicate<V> isNotNull() {
    return not(isNull());
  }
  
  public static final <V> Predicate<V> not(Predicate<V> positive) {
    return v -> ! positive.test(v);
  }
  
  public static final <V> Predicate<V> instanceOf(Class<?> type) {
    return v -> type.isInstance(v);
  }
  
  public static final <V> Predicate<V> alwaysTrue() {
    return v -> true;
  }
  
  public static final class Returning<R> {
    public <V> Select<V, R> from(V value) {
      return new Select<>(value);
    }
  }
  
  public static final <R> Returning<R> returning() {
    return returning(null);
  }
  
  public static final <R> Returning<R> returning(Class<R> type) {
    return new Returning<>();
  }
  
  public static final <V, R> Select<V, R> from(V value) {
    return new Select<>(value);
  }
}
