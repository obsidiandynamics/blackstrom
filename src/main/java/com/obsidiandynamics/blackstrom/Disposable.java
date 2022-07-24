package com.obsidiandynamics.blackstrom;

public interface Disposable {
  void dispose();
  
  interface FailsafeAutoCloseable extends AutoCloseable {
    @Override
    void close();
  }
  
  default FailsafeAutoCloseable closeable() {
    return this::dispose;
  }
  
  interface Nop extends Disposable {
    @Override
    default void dispose() {}
  }
}
