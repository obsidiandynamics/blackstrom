package com.obsidiandynamics.blackstrom.bank;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import org.junit.*;
import org.junit.Test;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.ledger.multiqueue.*;
import com.obsidiandynamics.blackstrom.machine.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.monitor.basic.*;

import junit.framework.*;

public final class BankTransferTest {
  private final Ledger ledger = new MultiQueueLedger(Integer.MAX_VALUE);
  
  private final List<Branch> branches = new ArrayList<>();
  
  private final Monitor monitor = new BasicMonitor();
  
  private VotingMachine machine;
  
  @After
  public void after() {
    machine.dispose();
  }

  @Test
  public void testRandomTransfers() throws Exception {
    final int numBranches = 3;
    final long initialBalance = 1000;
    final int runs = 1000;
    final int maxWait = 10_000;
    
    final AsyncInitiator initiator = new AsyncInitiator("settler");
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withInitiator(initiator)
        .withCohorts(createBranches(numBranches, initialBalance))
        .withMonitor(monitor)
        .build();
    
    final List<Decision> decisions = new CopyOnWriteArrayList<>();
    
    for (int run = 0; run < runs; run++) {
      final String[] branchIds = generateRandomBranches(numBranches);
      final BankSettlement settlement = generateRandomSettlement(branchIds, initialBalance / 2);
      initiator.initiate(branchIds, settlement, 0, decisions::add);
    }
    
    Timesert.wait(maxWait).until(() -> {
      TestCase.assertEquals(runs, decisions.size());
      final long expectedBalance = numBranches * initialBalance;
      assertEquals(expectedBalance, getTotalBalance());
    });
  }
  
  private long getTotalBalance() {
    return branches.stream().collect(Collectors.summarizingLong(b -> b.getBalance())).getSum();
  }
  
  private BankSettlement generateRandomSettlement(String[] branchIds, long amount) {
    final Map<String, BalanceTransfer> transfers = new HashMap<>(branchIds.length);
    long sum = 0;
    for (int i = 0; i < branchIds.length - 1; i++) {
      final long randomAmount = amount = (long) (Math.random() * amount * 20);
      sum += randomAmount;
      final String branchId = branchIds[i];
      transfers.put(branchId, new BalanceTransfer(branchId, randomAmount));
    }
    final String lastBranchId = branchIds[branchIds.length - 1];
    transfers.put(lastBranchId, new BalanceTransfer(lastBranchId, -sum));
    return new BankSettlement(transfers);
  }
  
  private String[] generateRandomBranches(int numBranches) {
    final Set<String> branches = new HashSet<>(numBranches);
    for (int i = 0; i < numBranches; i++) {
      while (! branches.add(getRandomBranchId(numBranches)));
    }
    return branches.toArray(new String[numBranches]);
  }
  
  private String getBranchId(int branchIdx) {
    return "branch-" + branchIdx;
  }
  
  private String getRandomBranchId(int numBranches) {
    return getBranchId((int) (Math.random() * numBranches));
  }

  private List<Branch> createBranches(int numBranches, long initialBalance) {
    for (int branchIdx = 0; branchIdx < numBranches; branchIdx++) {
      branches.add(new Branch(getBranchId(branchIdx), initialBalance));
    }
    return branches;
  }
}
