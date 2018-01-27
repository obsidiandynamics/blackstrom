package com.obsidiandynamics.blackstrom.bank;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.factor.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.util.*;
import com.obsidiandynamics.indigo.util.*;
import com.obsidiandynamics.junit.*;

@RunWith(Parameterized.class)
public final class BankTransferTest {  
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private static final String[] TWO_BRANCH_IDS = new String[] { getBranchId(0), getBranchId(1) };
  private static final int TWO_BRANCHES = TWO_BRANCH_IDS.length;
  private static final int FUTURE_GET_TIMEOUT = 10_000;
  
  private final Timesert wait = Wait.SHORT;

  private Ledger ledger;

  private Manifold manifold;

  @After
  public void after() {
    manifold.dispose();
  }
  
  private Ledger createLedger() {
    return new MultiNodeQueueLedger();
  }
  
  private void buildStandardManifold(Factor initiator, Factor monitor, Factor... branches) {
    ledger = createLedger();
    manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactors(initiator, monitor)
        .withFactors(branches)
        .build();
  }

  @Test
  public void testCommit() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor();
    final BankBranch[] branches = createBranches(2, initialBalance, true);
    buildStandardManifold(initiator, monitor, branches);

    final Outcome o = initiator.initiate(UUID.randomUUID(), 
                                         TWO_BRANCH_IDS,
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         Integer.MAX_VALUE)
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(Verdict.COMMIT, o.getVerdict());
    assertNull(o.getAbortReason());
    assertEquals(2, o.getResponses().length);
    assertEquals(Intent.ACCEPT, o.getResponse(getBranchId(0)).getIntent());
    assertEquals(Intent.ACCEPT, o.getResponse(getBranchId(1)).getIntent());
    wait.until(() -> {
      assertEquals(initialBalance - transferAmount, branches[0].getBalance());
      assertEquals(initialBalance + transferAmount, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
    });
  }

  @Test
  public void testAbort() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance + 1;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor();
    final BankBranch[] branches = createBranches(2, initialBalance, true);
    buildStandardManifold(initiator, monitor, branches);

    final Outcome o = initiator.initiate(UUID.randomUUID(), 
                                         TWO_BRANCH_IDS, 
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         Integer.MAX_VALUE)
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(Verdict.ABORT, o.getVerdict());
    assertEquals(AbortReason.REJECT, o.getAbortReason());
    assertTrue("responses.length=" + o.getResponses().length, o.getResponses().length >= 1); // the accept status doesn't need to have been considered
    assertEquals(Intent.REJECT, o.getResponse(getBranchId(0)).getIntent());
    final Response acceptResponse = o.getResponse(getBranchId(1));
    if (acceptResponse != null) {
      assertEquals(Intent.ACCEPT, acceptResponse.getIntent());  
    }
    wait.until(() -> {
      assertEquals(initialBalance, branches[0].getBalance());
      assertEquals(initialBalance, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
    });
  }

  @Test
  public void testImplicitTimeout() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor(new DefaultMonitorOptions().withTimeoutInterval(60_000));
    final BankBranch[] branches = createBranches(2, initialBalance, true);
    // we delay the receive rather than the send, so that the send timestamp appears recent — triggering implicit timeout
    buildStandardManifold(initiator, 
                         monitor, 
                         branches[0], 
                         new FallibleFactor(branches[1]).withRxFailureMode(new DelayedDelivery(1, 10)));

    final Outcome o = initiator.initiate(UUID.randomUUID(),
                                         TWO_BRANCH_IDS, 
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         1)
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(Verdict.ABORT, o.getVerdict());
    assertEquals(AbortReason.IMPLICIT_TIMEOUT, o.getAbortReason());
    assertTrue("responses.length=" + o.getResponses().length, o.getResponses().length >= 1);
    wait.until(() -> {
      assertEquals(initialBalance, branches[0].getBalance());
      assertEquals(initialBalance, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
    });
  }

  @Test
  public void testExplicitTimeout() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance;

    final AsyncInitiator initiator = new AsyncInitiator();
    final Monitor monitor = new DefaultMonitor(new DefaultMonitorOptions().withTimeoutInterval(1));
    final BankBranch[] branches = createBranches(2, initialBalance, true);
    // it doesn't matter whether we delay receive or send, since the messages are sufficiently delayed, such
    // that they won't get there within the test's running time — either failure mode will trigger an explicit timeout
    buildStandardManifold(initiator, 
                         monitor, 
                         new FallibleFactor(branches[0]).withRxFailureMode(new DelayedDelivery(1, 60_000)),
                         new FallibleFactor(branches[1]).withRxFailureMode(new DelayedDelivery(1, 60_000)));

    final Outcome o = initiator.initiate(UUID.randomUUID(),
                                         TWO_BRANCH_IDS, 
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         1)
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(Verdict.ABORT, o.getVerdict());
    assertEquals(AbortReason.EXPLICIT_TIMEOUT, o.getAbortReason());
    wait.until(() -> {
      assertEquals(initialBalance, branches[0].getBalance());
      assertEquals(initialBalance, branches[1].getBalance());
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
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
        manifold.dispose();
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
      return RxTxFailureModes.class.getSimpleName() + " [rxFailureMode=" + rxFailureMode + 
          ", txFailureMode=" + txFailureMode + "]";
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
    final Monitor monitor = new DefaultMonitor();
    final BankBranch[] branches = createBranches(2, initialBalance, true);

    ledger = createLedger();
    manifold = Manifold.builder()
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

    testSingleTransfer(initialBalance + 1, Verdict.ABORT, AbortReason.REJECT, initiator);
    testSingleTransfer(initialBalance, Verdict.COMMIT, null, initiator);

    wait.until(() -> {
      assertEquals(initialBalance * branches.length, getTotalBalance(branches));
      assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
      assertTrue("branches=" + Arrays.asList(branches), nonZeroBalances(branches));
    });
  }

  private void testSingleTransfer(int transferAmount, Verdict expectedVerdict, AbortReason expectedAbortReason,
                                  AsyncInitiator initiator) throws InterruptedException, ExecutionException, Exception {
    assert expectedVerdict == Verdict.COMMIT ^ expectedAbortReason != null;
    final Outcome o = initiator.initiate(UUID.randomUUID(), 
                                         TWO_BRANCH_IDS, 
                                         BankSettlement.builder()
                                         .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                        new BalanceTransfer(getBranchId(1), transferAmount))
                                         .build(),
                                         Integer.MAX_VALUE)
        .get(FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS);
    assertEquals(expectedVerdict, o.getVerdict());
    assertEquals(expectedAbortReason, o.getAbortReason());
  }

  /**
   *  For benchmarking, run with 
   *  -DBankTransferTest.numBranches=2 -DBankTransferTest.runs=4000000 -DBankTransferTest.randomiseRuns=false -DBankTransferTest.enableLogging=false
   *  
   *  @throws Exception
   */
  @Test
  public void testRandomTransfersBenchmark() {
    final int numBranches = PropertyUtils.get("BankTransferTest.numBranches", Integer::valueOf, 10);
    final int runs = PropertyUtils.get("BankTransferTest.runs", Integer::valueOf, 10_000);
    final long transferAmount = 1_000;
    final long initialBalance = runs * transferAmount / (numBranches * numBranches);
    final int backlogTarget = 10_000;
    final boolean idempotencyEnabled = false;
    final boolean randomiseRuns = PropertyUtils.get("BankTransferTest.randomiseRuns", Boolean::valueOf, true);
    final boolean enableLogging = PropertyUtils.get("BankTransferTest.enableLogging", Boolean::valueOf, true);

    final AtomicInteger commits = new AtomicInteger();
    final AtomicInteger aborts = new AtomicInteger();
    final Initiator initiator = (NullGroupInitiator) (c, o) -> {
      (o.getVerdict() == Verdict.COMMIT ? commits : aborts).incrementAndGet();
    };
    final BankBranch[] branches = createBranches(numBranches, initialBalance, idempotencyEnabled);
    final Monitor monitor = new DefaultMonitor();
    buildStandardManifold(initiator, monitor, branches);

    final long took = TestSupport.took(() -> {
      String[] branchIds = null;
      BankSettlement settlement = null;
      if (! randomiseRuns) {
        branchIds = numBranches != TWO_BRANCHES ? generateBranches(numBranches) : TWO_BRANCH_IDS;
        settlement = generateRandomSettlement(branchIds, transferAmount);
      }
      
      for (int run = 0; run < runs; run++) {
        if (randomiseRuns) {
          branchIds = numBranches != TWO_BRANCHES ? generateBranches(2 + (int) (Math.random() * (numBranches - 1))) : TWO_BRANCH_IDS;
          settlement = generateRandomSettlement(branchIds, transferAmount);
        }
        ledger.append(new Proposal(run, branchIds, settlement, Integer.MAX_VALUE));

        if (run % backlogTarget == 0) {
          long lastLogTime = 0;
          for (;;) {
            final int backlog = run - commits.get() - aborts.get();
            if (backlog > backlogTarget) {
              TestSupport.sleep(1);
              if (enableLogging && System.currentTimeMillis() - lastLogTime > 5_000) {
                TestSupport.LOG_STREAM.format("throttling... backlog @ %,d (%,d txns)\n", backlog, run);
                lastLogTime = System.currentTimeMillis();
              }
            } else {
              break;
            }
          }
        }
      }

      wait.until(() -> {
        assertEquals(runs, commits.get() + aborts.get());
        final long expectedBalance = numBranches * initialBalance;
        assertEquals(expectedBalance, getTotalBalance(branches));
        assertTrue("branches=" + Arrays.asList(branches), allZeroEscrow(branches));
        assertTrue("branches=" + Arrays.asList(branches), nonZeroBalances(branches));
      });
    });
    System.out.format("%,d took %,d ms, %,.0f txns/sec (%,d commits | %,d aborts)\n", 
                      runs, took, (double) runs / took * 1000, commits.get(), aborts.get());
  }

  private static long getTotalBalance(BankBranch[] branches) {
    return Arrays.stream(branches).collect(Collectors.summarizingLong(b -> b.getBalance())).getSum();
  }

  private static boolean allZeroEscrow(BankBranch[] branches) {
    return Arrays.stream(branches).allMatch(b -> b.getEscrow() == 0);
  }

  private static boolean nonZeroBalances(BankBranch[] branches) {
    return Arrays.stream(branches).allMatch(b -> b.getBalance() >= 0);
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

  private static String[] generateBranches(int numBranches) {
    final String[] branches = new String[numBranches];
    for (int i = 0; i < numBranches; i++) {
      branches[i] = getBranchId(i);
    }
    return branches;
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
