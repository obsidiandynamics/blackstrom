package com.obsidiandynamics.blackstrom.machine;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;

public interface Factor extends ElementalProcessor, Disposable.Default {
  default void init(InitContext context) {}
}
