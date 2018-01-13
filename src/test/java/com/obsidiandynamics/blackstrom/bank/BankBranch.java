package com.obsidiandynamics.blackstrom.bank;

import java.util.*;

import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.worker.*;
import com.obsidiandynamics.indigo.util.*;

public final class BankBranch implements Cohort {
  private final String branchId;
  
  private final Map<Object, Nomination> nominations = new HashMap<>();
  
  private final Map<Object, Outcome> decided = new HashMap<>();
  
  private final Object lock = new Object();
  
  private final boolean idempotencyEnabled;

  private final WorkerThread gcThread;
  
  private final int gcIntervalMillis = 1_000;
  
  private final int outcomeLifetimeMillis = 1_000;
  
  private long balance;
  
  private long escrow;
  
  public BankBranch(String branchId, long initialBalance, boolean idempotencyEnabled) {
    this.branchId = branchId;
    this.idempotencyEnabled = idempotencyEnabled;
    balance = initialBalance;
    
    gcThread = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withName(BankBranch.class.getSimpleName() + "-gc-" + branchId)
                     .withDaemon(true))
        .onCycle(this::gcCycle)
        .build();
    if (idempotencyEnabled) gcThread.start();
  }
  
  private void gcCycle(WorkerThread thread) throws InterruptedException {
    Thread.sleep(gcIntervalMillis);
    
    final long collectThreshold = System.currentTimeMillis() - outcomeLifetimeMillis;
    final List<Outcome> deathRow = new ArrayList<>();
    
    final List<Outcome> decidedCopy;
    synchronized (lock) {
      decidedCopy = new ArrayList<>(decided.values());
    }
    
    for (Outcome oucome : decidedCopy) {
      if (oucome.getTimestamp() < collectThreshold) {
        deathRow.add(oucome);
      }
    }
    
    if (! deathRow.isEmpty()) {
      for (Outcome outcome : deathRow) {
        synchronized (lock) {
          decided.remove(outcome.getBallotId());
        }
      }

      if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: reaped %,d lapsed outcomes\n", branchId, deathRow.size());
    }
  }
  
  public long getBalance() {
    return balance;
  }
  
  public long getEscrow() {
    return escrow;
  }

  public String getBranchId() {
    return branchId;
  }

  @Override
  public void onNomination(MessageContext context, Nomination nomination) {
    final BankSettlement settlement = nomination.getProposal();
    final BalanceTransfer xfer = settlement.getTransfers().get(branchId);
    if (xfer == null) return; // settlement doesn't apply to this branch
    
    try {
      if (idempotencyEnabled) {
        if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: %s\n", branchId, nomination);
        synchronized (lock) {
          if (decided.containsKey(nomination.getBallotId())) {
            if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: ignoring, already decided\n", branchId);
            return;
          }
        }
      }
      
      final Pledge pledge;
      final long xferAmount = xfer.getAmount();
      final long newBalance = balance + escrow + xferAmount;
      if (newBalance >= 0) {
        final boolean inserted = nominations.put(nomination.getBallotId(), nomination) == null;
        if (inserted) {
          pledge = Pledge.ACCEPT;
          if (xferAmount < 0) {
            escrow += xferAmount;
          }
          if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: accepting\n", branchId);
        } else {
          pledge = Pledge.ACCEPT;
          if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: retransmitting previous acceptance\n", branchId);
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
    } finally {
      context.confirm(nomination.getMessageId());
    }
  }

  @Override
  public void onOutcome(MessageContext context, Outcome outcome) {
    final Nomination nomination = nominations.remove(outcome.getBallotId());
    if (nomination == null) return; // outcome doesn't apply to this branch
    
    try {
      if (idempotencyEnabled) {
        synchronized (lock) {
          decided.put(outcome.getBallotId(), outcome);
        }
      }
  
      final BankSettlement settlement = nomination.getProposal();
      final BalanceTransfer xfer = settlement.getTransfers().get(branchId);
      final long xferAmount = xfer.getAmount();
      if (xferAmount < 0) {
        escrow -= xferAmount;
      }
      
      if (outcome.getVerdict() == Verdict.COMMIT) {
        balance += xferAmount;
      }
      
      if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: finalising %s\n", branchId, outcome);
    } finally {
      context.confirm(outcome.getMessageId());
    }
  }
  
  @Override
  public void dispose() {
    gcThread.terminate().joinQuietly();
  }

  @Override
  public String getGroupId() {
    return branchId;
  }
}