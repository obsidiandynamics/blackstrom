package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface ProposalProcessor extends ElementalProcessor {
  void onProposal(MessageContext context, Proposal proposal);
  
  interface Nop extends ProposalProcessor {
    @Override default void onProposal(MessageContext context, Proposal proposal) {}
  }
}
