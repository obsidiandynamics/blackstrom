package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;

public interface Initable {
  void init(InitContext context);
  
  interface Nop extends Initable {
    @Override
    default void init(InitContext context) {}
  }
}
