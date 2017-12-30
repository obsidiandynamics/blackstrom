package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface NominationHandler extends TypedMessageHandler {
  void onNomination(MessageContext context, Nomination nomination);
}
