package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface CommandResponseProcessor extends ElementalProcessor {
  void onCommandResponse(MessageContext context, CommandResponse commandResponse);
  
  interface Nop extends CommandResponseProcessor {
    @Override 
    default void onCommandResponse(MessageContext context, CommandResponse commandReponse) {}
  }
  
  interface BeginAndConfirm extends CommandResponseProcessor {
    @Override 
    default void onCommandResponse(MessageContext context, CommandResponse commandResponse) {
      context.beginAndConfirm(commandResponse);
    }
  }
}
