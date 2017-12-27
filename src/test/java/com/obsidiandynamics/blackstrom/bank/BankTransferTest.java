package com.obsidiandynamics.blackstrom.bank;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.ledger.multiqueue.*;
import com.obsidiandynamics.blackstrom.machine.*;
import com.obsidiandynamics.blackstrom.monitor.*;
import com.obsidiandynamics.blackstrom.monitor.basic.*;

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
  public void testBasicTransfer() {
    final int numBranches = 3;
    final int initialBalance = 1000;
    
    machine = VotingMachine.builder()
        .withLedger(ledger)
        .withCohorts(createBranches(numBranches, initialBalance))
        .withMonitor(monitor)
        .build();
    
    //fail("Not yet implemented");
  }

  private List<Branch> createBranches(int numBranches, int initialBalance) {
    for (int i = 0; i < numBranches; i++) {
      branches.add(new Branch("branch-" + i, initialBalance));
    }
    return branches;
  }
}
