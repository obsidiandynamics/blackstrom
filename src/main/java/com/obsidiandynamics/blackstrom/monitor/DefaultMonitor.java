package com.obsidiandynamics.blackstrom.monitor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class DefaultMonitor implements Monitor {
  private static final String GROUP_ID = "monitor";
  
  private final MonitorEngine engine;
  
  private Ledger ledger;
  
  public DefaultMonitor() {
    this(new MonitorEngineConfig());
  }
  
  public DefaultMonitor(MonitorEngineConfig engineConfig) {
    engine = new MonitorEngine(new MonitorAction() {
      @Override public void appendVote(Vote vote, AppendCallback callback) {
        ledger.append(vote, callback);
      }

      @Override public void appendOutcome(Outcome outcome, AppendCallback callback) {
        ledger.append(outcome, callback);
      }
    }, GROUP_ID, engineConfig);
  }
  
  public MonitorEngine getEngine() {
    return engine;
  }

  @Override
  public String getGroupId() {
    return GROUP_ID;
  }

  @Override
  public void onProposal(MessageContext context, Proposal proposal) {
    engine.onProposal(context, proposal);
  }

  @Override
  public void onVote(MessageContext context, Vote vote) {
    engine.onVote(context, vote);
  }
  
  @Override
  public void init(InitContext context) {
    ledger = context.getLedger();
  }
  
  @Override
  public void dispose() {
    engine.dispose();
  }
}
