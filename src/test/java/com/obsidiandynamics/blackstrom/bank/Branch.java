package com.obsidiandynamics.blackstrom.bank;

import java.util.*;

import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.indigo.util.*;

public final class Branch implements Cohort {
  private final String branchId;
  
  private final Map<Object, Nomination> nominations = new HashMap<>();
  
  private final Map<Object, Outcome> decided = new HashMap<>();
  
  private final int reapIntervalMillis = 1_000;
  
  private final int outcomeLifetimeMillis = 1_000;
  
  private long balance;
  
  private long lastReapTime;
  
  public Branch(String branchId, long initialBalance) {
    this.branchId = branchId;
    balance = initialBalance;
  }
  
  public long getBalance() {
    return balance;
  }

  public String getBranchId() {
    return branchId;
  }

  @Override
  public void onNomination(MessageContext context, Nomination nomination) {
    final BankSettlement settlement = nomination.getProposal();
    final BalanceTransfer xfer = settlement.getTransfers().get(branchId);
    if (xfer == null) return; // settlement doesn't apply to this branch
    
    if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: %s\n", branchId, nomination);
    if (decided.containsKey(nomination.getBallotId())) {
      if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: ignoring, already decided\n", branchId);
      return;
    }
    
    final Pledge pledge;
    final long newBalance = balance + xfer.getAmount();
    if (newBalance >= 0) {
      final boolean inserted = nominations.put(nomination.getBallotId(), nomination) == null;
      if (inserted) {
        pledge = Pledge.ACCEPT;
        balance = newBalance;
        if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: accepting\n", branchId);
      } else {
        if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: ignoring, ballot in progress\n", branchId);
        return;
      }
    } else {
      if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: rejecting, balance: %,d\n", branchId, balance);
      pledge = Pledge.REJECT;
    }
    
    try {
      context.vote(nomination.getBallotId(), branchId, pledge, null);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onOutcome(MessageContext context, Outcome outcome) {
    final Nomination nomination = nominations.remove(outcome.getBallotId());
    if (nomination == null) return; // outcome doesn't apply to this branch
    
    decided.put(outcome.getBallotId(), outcome);
    if (outcome.getVerdict() == Verdict.ABORT) {
      if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: rolling back %s\n", branchId, outcome);
      final BankSettlement settlement = nomination.getProposal();
      final BalanceTransfer xfer = settlement.getTransfers().get(branchId);
      balance -= xfer.getAmount();
    }
    
    final long now = outcome.getTimestamp();
    if (now - lastReapTime > reapIntervalMillis) {
      reapLapsedOutcomes(now);
      lastReapTime = outcome.getTimestamp();
    }
  }
  
  private void reapLapsedOutcomes(long now) {
    final long collectThreshold = now - outcomeLifetimeMillis;
    List<Object> deathRow = null;
    
    for (Outcome oucome : decided.values()) {
      if (oucome.getTimestamp() < collectThreshold) {
        if (deathRow == null) deathRow = new ArrayList<>(decided.size() / 2);
        deathRow.add(oucome.getBallotId());
      }
    }
    
    if (deathRow != null) {
      for (Object ballotId : deathRow) {
        decided.remove(ballotId);
      }
      if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: reaped %,d lapsed outcomes\n", branchId, deathRow.size());
      System.out.format("%s: reaped %,d lapsed outcomes\n", branchId, deathRow.size());
    }
  }
}
