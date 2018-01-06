package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.*;

public interface Factor extends ElementalProcessor, Disposable.Default, Groupable {
  default void init(InitContext context) {}
}
