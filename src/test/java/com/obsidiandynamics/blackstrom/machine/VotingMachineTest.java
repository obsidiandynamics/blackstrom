package com.obsidiandynamics.blackstrom.machine;

import static junit.framework.TestCase.*;
import static org.mockito.Mockito.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class VotingMachineTest {
  @Test
  public void test() {
    final Ledger ledger = mock(Ledger.class);
    final Initiator initiator = mock(Initiator.class);
    final Cohort cohort = mock(Cohort.class);
    final Monitor monitor = mock(Monitor.class);
    
    final VotingMachine machine = VotingMachine.builder()
        .withLedger(ledger)
        .withInitiator(initiator)
        .withCohort(cohort)
        .withMonitor(monitor)
        .build();
    
    verify(initiator).init(notNull());
    verify(cohort).init(notNull());
    verify(monitor).init(notNull());
    verify(ledger).init();
    
    assertEquals(ledger, machine.getLedger());
    assertEquals(1, machine.getInitiators().size());
    assertEquals(initiator, machine.getInitiators().iterator().next());
    assertEquals(1, machine.getCohorts().size());
    assertEquals(cohort, machine.getCohorts().iterator().next());
    assertEquals(1, machine.getMonitors().size());
    assertEquals(monitor, machine.getMonitors().iterator().next());
    
    machine.dispose();
    
    verify(ledger).dispose();
    verify(initiator).dispose();
    verify(cohort).dispose();
    verify(monitor).dispose();
  }
}
