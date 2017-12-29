package com.obsidiandynamics.blackstrom.handler;

public interface Initable {
  void init(InitContext context);
  
  interface Default extends Initable {
    @Override
    default void init(InitContext context) {}
  }
}
