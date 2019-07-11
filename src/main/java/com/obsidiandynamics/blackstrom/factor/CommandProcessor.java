package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface CommandProcessor extends ElementalProcessor {
  void onCommand(MessageContext context, Command command);
  
  interface Nop extends CommandProcessor {
    @Override 
    default void onCommand(MessageContext context, Command command) {}
  }
  
  interface BeginAndConfirm extends CommandProcessor {
    @Override 
    default void onCommand(MessageContext context, Command command) {
      context.beginAndConfirm(command);
    }
  }
}
