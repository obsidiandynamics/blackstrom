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

  private final Ledger ledger = new MultiNodeQueueLedger();

  private VotingMachine machine;

  @After
  public void after() {
    machine.dispose();
  }

  private void buildStandardMachine(Factor first, Factor second, Factor... thirdAndOthers) {
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
    final Branch[] branches = createBranches(2, initialBalance, true);
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
    final Branch[] branches = createBranches(2, initialBalance, true);
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
  public void testInitiatorFailure_rxDuplicate() throws InterruptedException, ExecutionException, Exception {
    testInitiatorFailure(new DuplicateDelivery(1), null);
  }

  @Test
  public void testInitiatorFailure_rxDelayed() throws InterruptedException, ExecutionException, Exception {
    testInitiatorFailure(new DelayedDelivery(1, 10), null);
  }

  @Test
  public void testInitiatorFailure_rxDelayedDuplicate() throws InterruptedException, ExecutionException, Exception {
    testInitiatorFailure(new DelayedDuplicateDelivery(1, 10), null);
  }

  @Test
  public void testInitiatorFailure_txDuplicate() throws InterruptedException, ExecutionException, Exception {
    testInitiatorFailure(null, new DuplicateDelivery(1));
  }

  @Test
  public void testInitiatorFailure_txDelayed() throws InterruptedException, ExecutionException, Exception {
    testInitiatorFailure(null, new DelayedDelivery(1, 10));
  }

  @Test
  public void testInitiatorFailure_txDelayedDuplicate() throws InterruptedException, ExecutionException, Exception {
    testInitiatorFailure(null, new DelayedDuplicateDelivery(1, 10));
  }

  private void testInitiatorFailure(FailureMode rxFailureMode, FailureMode txFailureMode) throws InterruptedException, ExecutionException, Exception {
    final int initialBalance = 1_000;
    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new BasicMonitor();
    final Branch[] branches = createBranches(2, initialBalance, true);

    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withFactors(new FailureProneFactor(initiator)
                     .withRxFailureMode(rxFailureMode)
                     .withTxFailureMode(txFailureMode),
                     monitor,
                     branches[0],
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
                                         Integer.MAX_VALUE).get();
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
    final Initiator initiator = (c, o) -> {
      if (o.getVerdict() == Verdict.COMMIT) {
        commits.incrementAndGet();
      } else {
        aborts.incrementAndGet();
      }
    };
    final Branch[] branches = createBranches(numBranches, initialBalance, idempotencyEnabled);
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

  private long getTotalBalance(Branch[] branches) {
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

  private static Branch[] createBranches(int numBranches, long initialBalance, boolean idempotencyEnabled) {
    final Branch[] branches = new Branch[numBranches];
    for (int branchIdx = 0; branchIdx < numBranches; branchIdx++) {
      branches[branchIdx] = new Branch(getBranchId(branchIdx), initialBalance, idempotencyEnabled);
    }
    return branches;
  }
}
