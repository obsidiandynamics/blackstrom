package com.obsidiandynamics.blackstrom.bank;

import java.util.*;

import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.indigo.util.*;

public final class Branch implements Cohort {
  private final String branchId;
  
  private final Map<Object, Nomination> nominations = new HashMap<>();
  
  private long balance;
  
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
  public void onNomination(VotingContext context, Nomination nomination) {
    final BankSettlement settlement = nomination.getProposal();
    final BalanceTransfer xfer = settlement.getTransfers().get(branchId);
    if (xfer == null) return; // settlement doesn't apply to this branch
    
    if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: %s\n", branchId, nomination);
    final Plea plea;
    final long newBalance = balance + xfer.getAmount();
    if (newBalance > 0) {
      nominations.put(nomination.getBallotId(), nomination);
      plea = Plea.ACCEPT;
      balance = newBalance;
      if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: accepting\n", branchId);
    } else {
      if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: rejecting, balance: %,d\n", branchId, balance);
      plea = Plea.REJECT;
    }
    try {
      context.vote(nomination.getBallotId(), branchId, branchId, plea, null);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onDecision(VotingContext context, Decision decision) {
    final Nomination nomination = nominations.remove(decision.getBallotId());
    if (nomination == null) return; // decision doesn't apply to this branch
    
    if (decision.getOutcome() == Outcome.ABORT) {
      if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: rolling back %s", branchId, decision);
      final BankSettlement settlement = nomination.getProposal();
      final BalanceTransfer xfer = settlement.getTransfers().get(branchId);
      balance -= xfer.getAmount();
    }
  }
}
