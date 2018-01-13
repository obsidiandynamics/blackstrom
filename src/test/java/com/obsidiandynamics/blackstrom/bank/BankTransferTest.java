package com.obsidiandynamics.blackstrom.bank;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.junit.*;
import org.junit.Test;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.machine.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.monitor.basic.*;
import com.obsidiandynamics.indigo.util.*;

import junit.framework.*;

public final class BankTransferTest {
  private static final String[] TWO_BRANCH_IDS = new String[] { getBranchId(0), getBranchId(1) };
  private static final int TWO_BRANCHES = TWO_BRANCH_IDS.length;
  private static final int MAX_WAIT = 10_000;

  private Ledger ledger;

  private VotingMachine machine;

  @After
  public void after() {
    machine.dispose();
  }
  
  private Ledger createLedger() {
    return new MultiNodeQueueLedger();
  }
  
  private void buildStandardMachine(Factor first, Factor second, Factor... thirdAndOthers) {
    ledger = createLedger();
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(first)
        .withFactors(second)
        .withFactors(thirdAndOthers)
        .build();
  }

  @Test
  public void testCommit() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new BasicMonitor();
    final BankBranch[] branches = createBranches(2, initialBalance, true);
    buildStandardMachine(initiator, monitor, branches);

    final Outcome o = initiator.initiate(UUID.randomUUID(), 
                                         TWO_BRANCH_IDS,
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         Integer.MAX_VALUE).get();
    assertEquals(Verdict.COMMIT, o.getVerdict());
    assertEquals(2, o.getResponses().length);
    assertEquals(Pledge.ACCEPT, o.getResponse(getBranchId(0)).getPledge());
    assertEquals(Pledge.ACCEPT, o.getResponse(getBranchId(1)).getPledge());
    Timesert.wait(MAX_WAIT).until(() -> {
      assertEquals(initialBalance - transferAmount, branches[0].getBalance());
      assertEquals(initialBalance + transferAmount, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
    });
  }

  @Test
  public void testAbort() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance + 1;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new BasicMonitor();
    final BankBranch[] branches = createBranches(2, initialBalance, true);
    buildStandardMachine(initiator, monitor, branches);

    final Outcome o = initiator.initiate(UUID.randomUUID(), 
                                         TWO_BRANCH_IDS, 
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         Integer.MAX_VALUE).get();
    assertEquals(Verdict.ABORT, o.getVerdict());
    assertTrue(o.getResponses().length >= 1); // the accept status doesn't need to have been considered
    assertEquals(Pledge.REJECT, o.getResponse(getBranchId(0)).getPledge());
    final Response acceptResponse = o.getResponse(getBranchId(1));
    if (acceptResponse != null) {
      assertEquals(Pledge.ACCEPT, acceptResponse.getPledge());  
    }
    Timesert.wait(MAX_WAIT).until(() -> {
      assertEquals(initialBalance, branches[0].getBalance());
      assertEquals(initialBalance, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
    });
  }
  
  @Test
  public void testFactorFailures() {
    final RxTxFailureModes[] presetFailureModesArray = new RxTxFailureModes[] {
      new RxTxFailureModes() {},
      new RxTxFailureModes() {{
        rxFailureMode = new DuplicateDelivery(1);
      }},
      new RxTxFailureModes() {{
        rxFailureMode = new DelayedDelivery(1, 10);
      }},
      new RxTxFailureModes() {{
        rxFailureMode = new DelayedDuplicateDelivery(1, 10);
      }},
      new RxTxFailureModes() {{
        txFailureMode = new DuplicateDelivery(1);
      }},
      new RxTxFailureModes() {{
        txFailureMode = new DelayedDelivery(1, 10);
      }},
      new RxTxFailureModes() {{
        txFailureMode = new DelayedDuplicateDelivery(1, 10);
      }}
    };
    
    for (TargetFactor target : TargetFactor.values()) {
      for (RxTxFailureModes failureModes : presetFailureModesArray) {
        try {
          testFactorFailure(new FailureModes().set(target, failureModes));
        } catch (Exception e) {
          throw new AssertionError(String.format("target=%s, failureModes=%s", target, failureModes), e);
        }
        machine.dispose();
      }
    }
  }
  
  private enum TargetFactor {
    INITIATOR,
    COHORT,
    MONITOR
  }
  
  private abstract static class RxTxFailureModes {
    FailureMode rxFailureMode; 
    FailureMode txFailureMode;
    
    @Override
    public String toString() {
      return "RxTxFailureModes [rxFailureMode=" + rxFailureMode + ", txFailureMode=" + txFailureMode + "]";
    }
  }
  
  private static class FailureModes extends EnumMap<TargetFactor, RxTxFailureModes> {
    private static final long serialVersionUID = 1L;
    
    FailureModes() {
      super(TargetFactor.class);
      for (TargetFactor target : TargetFactor.values()) {
        set(target, new RxTxFailureModes() {});
      }
    }
    
    FailureModes set(TargetFactor target, RxTxFailureModes rxtxFailureModes) {
      put(target, rxtxFailureModes);
      return this;
    }
  }

  private void testFactorFailure(Map<TargetFactor, RxTxFailureModes> failureModes) throws InterruptedException, ExecutionException, Exception {
    final int initialBalance = 1_000;
    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new BasicMonitor();
    final BankBranch[] branches = createBranches(2, initialBalance, true);

    ledger = createLedger();
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(new FallibleFactor(initiator)
                     .withRxFailureMode(failureModes.get(TargetFactor.INITIATOR).rxFailureMode)
                     .withTxFailureMode(failureModes.get(TargetFactor.INITIATOR).txFailureMode),
                     new FallibleFactor(monitor)
                     .withRxFailureMode(failureModes.get(TargetFactor.MONITOR).rxFailureMode)
                     .withTxFailureMode(failureModes.get(TargetFactor.MONITOR).txFailureMode),
                     new FallibleFactor(branches[0])
                     .withRxFailureMode(failureModes.get(TargetFactor.COHORT).rxFailureMode)
                     .withTxFailureMode(failureModes.get(TargetFactor.COHORT).txFailureMode),
                     branches[1])
        .build();

    testSingleTransfer(initialBalance + 1, Verdict.ABORT, initiator);
    testSingleTransfer(initialBalance, Verdict.COMMIT, initiator);

    Timesert.wait(MAX_WAIT).until(() -> {
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
    });
  }

  private void testSingleTransfer(int transferAmount, Verdict expectedVerdict,
                                  AsyncInitiator initiator) throws InterruptedException, ExecutionException, Exception {
    final Outcome o = initiator.initiate(UUID.randomUUID(), 
                                         TWO_BRANCH_IDS, 
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         Integer.MAX_VALUE)
        .get(MAX_WAIT, TimeUnit.MILLISECONDS);
    assertEquals(expectedVerdict, o.getVerdict());
  }

  @Test
  public void testRandomTransfersBenchmark() throws Exception {
    final int numBranches = 10;
    final long initialBalance = 1_000_000;
    final long transferAmount = 1_000;
    final int runs = 10_000;
    final int backlogTarget = 10_000;
    final boolean idempotencyEnabled = false;

    final AtomicInteger commits = new AtomicInteger();
    final AtomicInteger aborts = new AtomicInteger();
    final Initiator initiator = (NullGroupInitiator) (c, o) -> {
      if (o.getVerdict() == Verdict.COMMIT) {
        commits.incrementAndGet();
      } else {
        aborts.incrementAndGet();
      }
    };
    final BankBranch[] branches = createBranches(numBranches, initialBalance, idempotencyEnabled);
    final Monitor monitor = new BasicMonitor();
    buildStandardMachine(initiator, monitor, branches);

    final long took = TestSupport.tookThrowing(() -> {
      for (int run = 0; run < runs; run++) {
        final String[] branchIds = numBranches != TWO_BRANCHES ? generateRandomBranches(2 + (int) (Math.random() * (numBranches - 1))) : TWO_BRANCH_IDS;
        final BankSettlement settlement = generateRandomSettlement(branchIds, transferAmount);
        ledger.append(new Nomination(run, branchIds, settlement, Integer.MAX_VALUE));

        if (run % backlogTarget == 0) {
          long lastLogTime = 0;
          for (;;) {
            final int backlog = run - commits.get() - aborts.get();
            if (backlog > backlogTarget) {
              TestSupport.sleep(1);
              if (System.currentTimeMillis() - lastLogTime > 5_000) {
                TestSupport.LOG_STREAM.format("throttling... backlog @ %,d (%,d txns)\n", backlog, run);
                lastLogTime = System.currentTimeMillis();
              }
            } else {
              break;
            }
          }
        }
      }

      Timesert.wait(MAX_WAIT).until(() -> {
        TestCase.assertEquals(runs, commits.get() + aborts.get());
        final long expectedBalance = numBranches * initialBalance;
        assertEquals(expectedBalance, getTotalBalance(branches));
      });
    });
    System.out.format("%,d took %,d ms, %,.0f txns/sec (%,d commits | %,d aborts)\n", 
                      runs, took, (double) runs / took * 1000, commits.get(), aborts.get());
  }

  private long getTotalBalance(BankBranch[] branches) {
    return Arrays.stream(branches).collect(Collectors.summarizingLong(b -> b.getBalance())).getSum();
  }

  private static BankSettlement generateRandomSettlement(String[] branchIds, long amount) {
    final Map<String, BalanceTransfer> transfers = new HashMap<>(branchIds.length);
    long sum = 0;
    for (int i = 0; i < branchIds.length - 1; i++) {
      final long randomAmount = amount - (long) (Math.random() * amount * 2);
      sum += randomAmount;
      final String branchId = branchIds[i];
      transfers.put(branchId, new BalanceTransfer(branchId, randomAmount));
    }
    final String lastBranchId = branchIds[branchIds.length - 1];
    transfers.put(lastBranchId, new BalanceTransfer(lastBranchId, -sum));
    if (TestSupport.LOG) TestSupport.LOG_STREAM.format("xfers %s\n", transfers);
    return new BankSettlement(transfers);
  }

  private static String[] generateRandomBranches(int numBranches) {
    final Set<String> branches = new HashSet<>(numBranches);
    for (int i = 0; i < numBranches; i++) {
      while (! branches.add(getRandomBranchId(numBranches)));
    }
    return branches.toArray(new String[numBranches]);
  }

  private static String getRandomBranchId(int numBranches) {
    return getBranchId((int) (Math.random() * numBranches));
  }

  private static String getBranchId(int branchIdx) {
    return "branch-" + branchIdx;
  }

  private static BankBranch[] createBranches(int numBranches, long initialBalance, boolean idempotencyEnabled) {
    final BankBranch[] branches = new BankBranch[numBranches];
    for (int branchIdx = 0; branchIdx < numBranches; branchIdx++) {
      branches[branchIdx] = new BankBranch(getBranchId(branchIdx), initialBalance, idempotencyEnabled);
    }
    return branches;
  }
}
