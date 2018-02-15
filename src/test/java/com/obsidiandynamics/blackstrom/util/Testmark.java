package com.obsidiandynamics.blackstrom.util;

import java.util.function.*;

import com.obsidiandynamics.blackstrom.resolver.*;
import com.obsidiandynamics.blackstrom.util.throwing.*;

public final class Testmark {
  private static class TestmarkConfig {
    boolean enabled;
  }
  
  private Testmark() {}
  
  public final static class FluentTestmark {
    FluentTestmark() {}
    
    public <T> FluentTestmark withOptions(T options) {
      setOptions(options);
      return this;
    }
    
    public <T> FluentTestmark withOptions(Class<? super T> optionsType, T options) {
      setOptions(optionsType, options);
      return this;
    }
  }
  
  public static FluentTestmark enable() {
    Resolver.assign(TestmarkConfig.class, Singleton.of(new TestmarkConfig() {{
      enabled=true;
    }}));
    return new FluentTestmark();
  }
  
  public static boolean isEnabled() {
    return Resolver.lookup(TestmarkConfig.class, TestmarkConfig::new).get().enabled;
  }
  
  public static <T> T getOptions(Class<? super T> optionsType, Supplier<T> optionsSupplier) {
    return Resolver.lookup(optionsType, optionsSupplier).get();
  }
  
  public static <T> void setOptions(T options) {
    @SuppressWarnings("unchecked")
    final Class<T> optionsType = (Class<T>) options.getClass();
    setOptions(optionsType, options);
  }
  
  public static <T> void setOptions(Class<? super T> optionsType, T options) {
    Resolver.assign(optionsType, Singleton.of(options));
  }
  
  public static void ifEnabled(ThrowingRunnable r) {
    if (isEnabled()) {
      System.out.println("Starting benchmark...");
      try {
        r.run();
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }
}
