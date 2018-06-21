package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface QueryProcessor extends ElementalProcessor {
  void onQuery(MessageContext context, Query query);
  
  interface Nop extends QueryProcessor {
    @Override default void onQuery(MessageContext context, Query query) {}
  }
}
