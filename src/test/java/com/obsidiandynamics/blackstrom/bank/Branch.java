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
  public void onNomination(MessageContext context, Nomination nomination) {
    final BankSettlement settlement = nomination.getProposal();
    final BalanceTransfer xfer = settlement.getTransfers().get(branchId);
    if (xfer == null) return; // settlement doesn't apply to this branch
    
    if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: %s\n", branchId, nomination);
    final Pledge pledge;
    final long newBalance = balance + xfer.getAmount();
    if (newBalance >= 0) {
      nominations.put(nomination.getBallotId(), nomination);
      pledge = Pledge.ACCEPT;
      balance = newBalance;
      if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: accepting\n", branchId);
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
    
    if (outcome.getVerdict() == Verdict.ABORT) {
      if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: rolling back %s", branchId, outcome);
      final BankSettlement settlement = nomination.getProposal();
      final BalanceTransfer xfer = settlement.getTransfers().get(branchId);
      balance -= xfer.getAmount();
    }
  }
}
