package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;

public interface Factor extends ElementalProcessor, Disposable.Nop, Groupable {
  default void init(InitContext context) {}
}
