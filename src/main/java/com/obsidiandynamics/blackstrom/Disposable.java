package com.obsidiandynamics.blackstrom;

public interface Disposable {
  void dispose();
  
  interface Default extends Disposable {
    @Override
    default void dispose() {}
  }
}
