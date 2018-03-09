package com.obsidiandynamics.blackstrom.monitor;

import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.retention.*;

public final class InlineMonitor implements Factor, ProposalProcessor, VoteProcessor {
  private final MonitorEngine engine;
  
  private final MessageHandler downstreamHandler;
  
  private final Factor downstreamFactor;
  
  private MessageContext defaultContext;
  
  public InlineMonitor(MonitorEngineConfig engineConfig, Factor downstreamFactor) {
    engine = new MonitorEngine(new MonitorAction() {
      @Override public void appendVote(Vote vote, AppendCallback callback) {
        defaultContext.getLedger().append(vote, callback);
      }

      @Override public void appendOutcome(Outcome outcome, AppendCallback callback) {
        callback.onAppend(null, null);
        downstreamHandler.onMessage(defaultContext, outcome);
      }
    }, downstreamFactor.getGroupId(), engineConfig);
    
    downstreamHandler = new MessageHandlerAdapter(downstreamFactor);
    this.downstreamFactor = downstreamFactor;
  }
  
  public MonitorEngine getEngine() {
    return engine;
  }

  @Override
  public String getGroupId() {
    return downstreamFactor.getGroupId();
  }

  @Override
  public void onProposal(MessageContext context, Proposal proposal) {
    engine.onProposal(context, proposal);
    downstreamHandler.onMessage(context, proposal);
  }
  
  @Override
  public void onVote(MessageContext context, Vote vote) {
    engine.onVote(context, vote);
    downstreamHandler.onMessage(context, vote);
  }
  
  @Override
  public void init(InitContext context) {
    defaultContext = new DefaultMessageContext(context.getLedger(), null, NopRetention.getInstance());
    downstreamFactor.init(context);
  }
  
  @Override
  public void dispose() {
    downstreamFactor.dispose();
    engine.dispose();
  }
}
