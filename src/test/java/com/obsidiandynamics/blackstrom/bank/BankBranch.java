package com.obsidiandynamics.blackstrom.bank;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.blackstrom.worker.*;
import com.obsidiandynamics.indigo.util.*;

public final class BankBranch implements Cohort {
  private final String branchId;
  
  private final Map<Object, Proposal> proposals = new HashMap<>();
  
  private final Map<Object, Outcome> decided = new HashMap<>();
  
  private final Object lock = new Object();
  
  private final boolean idempotencyEnabled;

  private final WorkerThread gcThread;
  
  private final int gcIntervalMillis = 1_000;
  
  private final int outcomeLifetimeMillis = 1_000;
  
  private final Predicate<Message> messageFilter;
  
  private long balance;
  
  private long escrow;
  
  public BankBranch(String branchId, long initialBalance, boolean idempotencyEnabled, Predicate<Message> messageFilter) {
    this.branchId = branchId;
    this.idempotencyEnabled = idempotencyEnabled;
    this.messageFilter = messageFilter;
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
  public void onProposal(MessageContext context, Proposal proposal) {
    try {
      if (! messageFilter.test(proposal)) return;
      
      final BankSettlement settlement = proposal.getObjective();
      final BalanceTransfer xfer = settlement.getTransfers().get(branchId);
      if (xfer == null) return; // settlement doesn't apply to this branch
    
      if (idempotencyEnabled) {
        if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: %s\n", branchId, proposal);
        synchronized (lock) {
          if (decided.containsKey(proposal.getBallotId())) {
            if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: ignoring, already decided\n", branchId);
            return;
          }
        }
      }
      
      final Intent intent;
      final long xferAmount = xfer.getAmount();
      final long newBalance = balance + escrow + xferAmount;
      if (newBalance >= 0) {
        final boolean inserted = proposals.put(proposal.getBallotId(), proposal) == null;
        if (inserted) {
          intent = Intent.ACCEPT;
          if (xferAmount < 0) {
            escrow += xferAmount;
          }
          if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: accepting %s\n", branchId, proposal.getBallotId());
        } else {
          intent = Intent.ACCEPT;
          if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: retransmitting previous acceptance\n", branchId);
        }
      } else {
        if (TestSupport.LOG) TestSupport.LOG_STREAM.format("%s: rejecting %s, balance: %,d\n", branchId, proposal.getBallotId(), balance);
        intent = Intent.REJECT;
      }
      
      try {
        context.publish(new Vote(proposal.getBallotId(), new Response(branchId, intent, null)).inResponseTo(proposal));
      } catch (Exception e) {
        e.printStackTrace();
      }
    } finally {
      context.confirm(proposal.getMessageId());
    }
  }

  @Override
  public void onOutcome(MessageContext context, Outcome outcome) {
    try {
      if (! messageFilter.test(outcome)) return;
      
      final Proposal proposal = proposals.remove(outcome.getBallotId());
      if (proposal == null) return; // outcome doesn't apply to this branch
      
      if (idempotencyEnabled) {
        synchronized (lock) {
          decided.put(outcome.getBallotId(), outcome);
        }
      }
  
      final BankSettlement settlement = proposal.getObjective();
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

  @Override
  public String toString() {
    return BankBranch.class.getSimpleName() + " [branchId=" + branchId + ", balance=" + balance + ", escrow=" + escrow + "]";
  }

  public static final String getId(int branchIdx) {
    return "branch-" + branchIdx;
  }

  public static final String[] generateIds(int numBranches) {
    return IntStream.range(0, numBranches).boxed()
        .map(BankBranch::getId).collect(Collectors.toList()).toArray(new String[numBranches]);
  }

  public static final BankBranch[] create(int numBranches, long initialBalance, boolean idempotencyEnabled, Sandbox sandbox) {
    return IntStream.range(0, numBranches).boxed()
        .map(i -> new BankBranch(BankBranch.getId(i), initialBalance, idempotencyEnabled, sandbox::contains))
        .collect(Collectors.toList()).toArray(new BankBranch[numBranches]);
  }
}
