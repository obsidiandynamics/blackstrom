package com.obsidiandynamics.blackstrom.bank;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
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
  
  private final Ledger ledger = new SingleQueueLedger();
  
  private final List<Branch> branches = new ArrayList<>();
  
  private final Monitor monitor = new BasicMonitor();
  
  private VotingMachine machine;
  
  @After
  public void after() {
    machine.dispose();
  }

  @Test
  public void testRandomTransfers() throws Exception {
    final int numBranches = 10;
    final long initialBalance = 1_000_000;
    final long transferAmount = 1_000;
    final int runs = 100_000;
    final int maxWait = 60_000;
    final int backlogTarget = 100_000;
    
    final AsyncInitiator initiator = new AsyncInitiator("settler");
    
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withInitiator(initiator)
        .withCohorts(createBranches(numBranches, initialBalance))
        .withMonitor(monitor)
        .build();

    final AtomicInteger commits = new AtomicInteger();
    final AtomicInteger aborts = new AtomicInteger();
    final Consumer<Decision> decisionCounter = d -> {
      if (d.getOutcome() == Outcome.COMMIT) {
        commits.incrementAndGet();
      } else {
        aborts.incrementAndGet();
      }
    };

    final long took = TestSupport.tookThrowing(() -> {
      for (int run = 0; run < runs; run++) {
        final String[] branchIds = numBranches != TWO_BRANCHES ? generateRandomBranches(2 + (int) (Math.random() * (numBranches - 1))) : TWO_BRANCH_IDS;
        final BankSettlement settlement = generateRandomSettlement(branchIds, transferAmount);
        initiator.initiate(run, branchIds, settlement, 0, decisionCounter);
        
        if (run % backlogTarget == 0) {
          boolean logged = false;
          for (;;) {
            final int backlog = run - commits.get() - aborts.get();
            if (backlog  > backlogTarget) {
              TestSupport.sleep(10);
              if (! logged) {
                TestSupport.LOG_STREAM.format("throttling... backlog @ %,d (%,d runs)\n", backlog, run);
                logged = true;
              }
            } else {
              break;
            }
          }
        }
      }
      
      Timesert.wait(maxWait).until(() -> {
        TestCase.assertEquals(runs, commits.get() + aborts.get());
        final long expectedBalance = numBranches * initialBalance;
        assertEquals(expectedBalance, getTotalBalance());
      });
    });
    System.out.format("%,d took %,d ms, %,d txns/sec (%,d commits | %,d aborts)\n", 
                      runs, took, runs / took * 1000, commits.get(), aborts.get());
  }
  
  private long getTotalBalance() {
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

  private List<Branch> createBranches(int numBranches, long initialBalance) {
    for (int branchIdx = 0; branchIdx < numBranches; branchIdx++) {
      branches.add(new Branch(getBranchId(branchIdx), initialBalance));
    }
    return branches;
  }
}
