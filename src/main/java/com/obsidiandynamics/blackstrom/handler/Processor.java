package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.*;

public interface Processor extends Disposable.Default {
  default void init(InitContext context) {}
}
