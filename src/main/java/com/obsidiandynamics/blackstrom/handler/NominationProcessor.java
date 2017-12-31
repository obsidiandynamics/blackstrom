package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface NominationProcessor extends Processor {
  void onNomination(MessageContext context, Nomination nomination);
}
