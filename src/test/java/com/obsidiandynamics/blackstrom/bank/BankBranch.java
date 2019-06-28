package com.obsidiandynamics.blackstrom.bank;

import static com.obsidiandynamics.zerolog.LogLevel.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.nanoclock.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.zerolog.*;

public final class BankBranch implements Cohort.Base {
  private Zlg zlg = Zlg.forDeclaringClass().get();
  
  private final String branchId;
  
  private final Map<Object, Proposal> proposals = new HashMap<>();
  
  private final Map<Object, Outcome> decided = new HashMap<>();
  
  private final Object lock = new Object();
  
  private final boolean idempotencyEnabled;

  private final WorkerThread gcThread;
  
  private final int gcIntervalMillis = 1_000;
  
  private final int outcomeLifetimeMillis = 60_000;
  
  private final Predicate<Message> messageFilter;
  
  private final AtomicLong outcomes = new AtomicLong();
  
  private long balance;
  
  private long escrow;
  
  private boolean sendMetadata;
  
  public BankBranch(String branchId, long initialBalance, boolean idempotencyEnabled, Predicate<Message> messageFilter) {
    this.branchId = branchId;
    this.idempotencyEnabled = idempotencyEnabled;
    this.messageFilter = messageFilter;
    balance = initialBalance;
    
    gcThread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(BankBranch.class, "gc", branchId))
        .onCycle(this::gcCycle)
        .build();
    if (idempotencyEnabled) gcThread.start();
  }
  
  public BankBranch withZlg(Zlg zlg) {
    this.zlg = zlg;
    return this;
  }
  
  public BankBranch withSendMetadata(boolean sendMetadata) {
    this.sendMetadata = sendMetadata;
    return this;
  }
  
  private void gcCycle(WorkerThread thread) throws InterruptedException {
    Thread.sleep(gcIntervalMillis);
    
    final long collectThreshold = NanoClock.now() - outcomeLifetimeMillis * 1_000_000L;
    final List<Outcome> deathRow = new ArrayList<>();
    
    final List<Outcome> decidedCopy;
    synchronized (lock) {
      decidedCopy = new ArrayList<>(decided.values());
    }
    
    for (Outcome outcome : decidedCopy) {
      if (outcome.getTimestamp() < collectThreshold) {
        deathRow.add(outcome);
      }
    }
    
    if (! deathRow.isEmpty()) {
      for (Outcome outcome : deathRow) {
        synchronized (lock) {
          decided.remove(outcome.getXid());
        }
      }

      assert zlg.level(TRACE).format("%s: reaped %,d lapsed outcomes").arg(branchId).arg(deathRow::size).log();
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
        assert zlg.level(TRACE).format("%s: %s").arg(branchId).arg(proposal).log();
        synchronized (lock) {
          if (decided.containsKey(proposal.getXid())) {
            assert zlg.level(TRACE).format("%s: ignoring, already decided").arg(branchId).log();
            return;
          }
        }
      }
      
      final Intent intent;
      final long xferAmount = xfer.getAmount();
      final long newBalance = balance + escrow + xferAmount;
      final boolean duplicate;
      if (newBalance >= 0) {
        final boolean inserted = proposals.put(proposal.getXid(), proposal) == null;
        if (inserted) {
          intent = Intent.ACCEPT;
          if (xferAmount < 0) {
            escrow += xferAmount;
          }
          duplicate = false;
          assert zlg.level(TRACE).format("%s: accepting %s").arg(branchId).arg(proposal::getXid).log();
        } else {
          intent = Intent.ACCEPT;
          duplicate = true;
          assert zlg.level(TRACE).format("%s: retransmitting previous acceptance").arg(branchId).log();
        }
      } else {
        duplicate = false;
        assert zlg.level(TRACE).format("%s: rejecting %s, balance: %,d").arg(branchId).arg(proposal::getXid).arg(balance).log();
        intent = Intent.REJECT;
      }
      
      final Object metadata;
      if (sendMetadata) {
        metadata = Collections.singletonMap("duplicate", duplicate);
      } else {
        metadata = null;
      }
      
      try {
        context.getLedger().append(new Vote(proposal.getXid(), new Response(branchId, intent, metadata))
                                   .inResponseTo(proposal).withSource(branchId));
      } catch (Exception e) {
        e.printStackTrace();
      }
    } finally {
      context.beginAndConfirm(proposal);
    }
  }

  @Override
  public void onOutcome(MessageContext context, Outcome outcome) {
    try {
      if (! messageFilter.test(outcome)) return;
      
      outcomes.incrementAndGet();
      
      final Proposal proposal = proposals.remove(outcome.getXid());
      if (proposal == null) {
        assert zlg.level(TRACE).format("%s: no applicable outcome for ballot %s").arg(branchId).arg(outcome::getXid).log();
        return; // outcome doesn't apply to this branch
      }
      
      if (idempotencyEnabled) {
        synchronized (lock) {
          decided.put(outcome.getXid(), outcome);
        }
      }
  
      final BankSettlement settlement = proposal.getObjective();
      final BalanceTransfer xfer = settlement.getTransfers().get(branchId);
      final long xferAmount = xfer.getAmount();
      if (xferAmount < 0) {
        escrow -= xferAmount;
      }
      
      if (outcome.getResolution() == Resolution.COMMIT) {
        balance += xferAmount;
      }
      
      assert zlg.level(TRACE).format("%s: finalising %s").arg(branchId).arg(outcome).log();
    } finally {
      context.beginAndConfirm(outcome);
    }
  }
  
  public long getNumOutcomes() {
    return outcomes.get();
  }
  
  @Override
  public void dispose() {
    gcThread.terminate().joinSilently();
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
