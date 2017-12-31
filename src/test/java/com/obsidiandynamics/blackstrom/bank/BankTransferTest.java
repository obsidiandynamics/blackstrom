package com.obsidiandynamics.blackstrom.bank;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import org.junit.*;
import org.junit.Test;

import com.obsidiandynamics.await.*;
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
  private static final int MAX_WAIT = 60_000;

  private final Ledger ledger = new MultiNodeQueueLedger();

  private final Monitor monitor = new BasicMonitor();

  private VotingMachine machine;

  @After
  public void after() {
    machine.dispose();
  }
  
  private void buildStandardMachine(Initiator initiator, List<Branch> branches) {
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withProcessors(initiator, monitor)
        .withProcessors(branches)
        .build();
  }

  @Test
  public void testCommit() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance;

    final AsyncInitiator initiator = new AsyncInitiator();
    final List<Branch> branches = createBranches(2, initialBalance);
    buildStandardMachine(initiator, branches);

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
      assertEquals(initialBalance - transferAmount, branches.get(0).getBalance());
      assertEquals(initialBalance + transferAmount, branches.get(1).getBalance());
    });
  }

  @Test
  public void testAbort() throws Exception {
    final int initialBalance = 1_000;
    final int transferAmount = initialBalance + 1;

    final AsyncInitiator initiator = new AsyncInitiator();
    final List<Branch> branches = createBranches(2, initialBalance);
    buildStandardMachine(initiator, branches);

    final Outcome d = initiator.initiate(UUID.randomUUID(), 
                                          TWO_BRANCH_IDS, 
                                          BankSettlement.builder()
                                          .withTransfers(new BalanceTransfer(getBranchId(0), -transferAmount),
                                                         new BalanceTransfer(getBranchId(1), transferAmount))
                                          .build(),
                                          Integer.MAX_VALUE).get();
    assertEquals(Verdict.ABORT, d.getVerdict());
    assertTrue(d.getResponses().length >= 1); // the accept status doesn't need to have been considered
    assertEquals(Pledge.REJECT, d.getResponse(getBranchId(0)).getPledge());
    final Response acceptResponse = d.getResponse(getBranchId(1));
    if (acceptResponse != null) {
      assertEquals(Pledge.ACCEPT, acceptResponse.getPledge());  
    }
    Timesert.wait(MAX_WAIT).until(() -> {
      assertEquals(initialBalance, branches.get(0).getBalance());
      assertEquals(initialBalance, branches.get(1).getBalance());
    });
  }

  @Test
  public void testRandomTransfers() throws Exception {
    final int numBranches = 10;
    final long initialBalance = 1_000_000;
    final long transferAmount = 1_000;
    final int runs = 10_000;
    final int backlogTarget = 20_000;

    final AtomicInteger commits = new AtomicInteger();
    final AtomicInteger aborts = new AtomicInteger();
    final Initiator initiator = (c, o) -> {
      if (o.getVerdict() == Verdict.COMMIT) {
        commits.incrementAndGet();
      } else {
        aborts.incrementAndGet();
      }
    };
    final List<Branch> branches = createBranches(numBranches, initialBalance);
    buildStandardMachine(initiator, branches);

    final long took = TestSupport.tookThrowing(() -> {
      for (int run = 0; run < runs; run++) {
        final String[] branchIds = numBranches != TWO_BRANCHES ? generateRandomBranches(2 + (int) (Math.random() * (numBranches - 1))) : TWO_BRANCH_IDS;
        final BankSettlement settlement = generateRandomSettlement(branchIds, transferAmount);
        ledger.append(new Nomination(run, branchIds, settlement, Integer.MAX_VALUE));

        if (run % backlogTarget == 0) {
          long lastLogTime = 0;
          for (;;) {
            final int backlog = run - commits.get() - aborts.get();
            if (backlog  > backlogTarget) {
              TestSupport.sleep(10);
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

  private long getTotalBalance(List<Branch> branches) {
    return branches.stream().collect(Collectors.summarizingLong(b -> b.getBalance())).getSum();
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

  private static List<Branch> createBranches(int numBranches, long initialBalance) {
    final List<Branch> branches = new ArrayList<>(); 
    for (int branchIdx = 0; branchIdx < numBranches; branchIdx++) {
      branches.add(new Branch(getBranchId(branchIdx), initialBalance));
    }
    return branches;
  }
}
