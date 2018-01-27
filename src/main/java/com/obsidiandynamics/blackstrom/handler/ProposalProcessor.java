package com.obsidiandynamics.blackstrom.handler;

import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface ProposalProcessor extends ElementalProcessor {
  void onProposal(MessageContext context, Proposal proposal);
}
