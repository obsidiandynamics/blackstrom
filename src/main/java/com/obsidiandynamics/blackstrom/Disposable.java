package com.obsidiandynamics.blackstrom;

public interface Disposable {
  void dispose();
  
  interface Nop extends Disposable {
    @Override
    default void dispose() {}
  }
}
