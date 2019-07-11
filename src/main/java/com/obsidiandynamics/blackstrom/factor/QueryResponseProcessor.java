package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface QueryResponseProcessor extends ElementalProcessor {
  void onQueryResponse(MessageContext context, QueryResponse queryResponse);
  
  interface Nop extends QueryResponseProcessor {
    @Override 
    default void onQueryResponse(MessageContext context, QueryResponse queryResponse) {}
  }
  
  interface BeginAndConfirm extends QueryResponseProcessor {
    @Override 
    default void onQueryResponse(MessageContext context, QueryResponse queryResponse) {
      context.beginAndConfirm(queryResponse);
    }
  }
}
