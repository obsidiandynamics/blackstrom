package com.obsidiandynamics.blackstrom.kafka;

@FunctionalInterface
public interface ExceptionGenerator<T, X> {
  X get(T obj);
  
  static <T, X> ExceptionGenerator<T, X> never() {
    return record -> null;
  }
  
  static <T, X> ExceptionGenerator<T, X> once(X exception) {
    return new ExceptionGenerator<T, X>() {
      private X ex = exception;
      @Override public X get(T obj) {
        try {
          return ex;
        } finally {
          ex = null;
        }
      }
    };
  }
}
