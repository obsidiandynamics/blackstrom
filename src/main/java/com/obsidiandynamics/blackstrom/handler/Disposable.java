package com.obsidiandynamics.blackstrom.handler;

public interface Disposable {
  void dispose();
  
  interface Default extends Disposable {
    @Override
    default void dispose() {}
  }
}
