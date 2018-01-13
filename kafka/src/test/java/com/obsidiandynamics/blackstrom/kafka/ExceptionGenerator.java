package com.obsidiandynamics.blackstrom.kafka;

@FunctionalInterface
public interface ExceptionGenerator<T> {
  Exception get(T obj);
  
  static <T> ExceptionGenerator<T> never() {
    return record -> null;
  }
  
  static <T> ExceptionGenerator<T> once(Exception exception) {
    return new ExceptionGenerator<T>() {
      private Exception ex = exception;
      @Override public Exception get(T obj) {
        try {
          return ex;
        } finally {
          ex = null;
        }
      }
    };
  }
}
