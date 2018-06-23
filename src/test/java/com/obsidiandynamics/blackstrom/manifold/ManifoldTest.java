package com.obsidiandynamics.blackstrom.manifold;

import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.cohort.*;
import com.obsidiandynamics.blackstrom.initiator.*;
import com.obsidiandynamics.blackstrom.ledger.*;
import com.obsidiandynamics.blackstrom.monitor.*;

public final class ManifoldTest {
  @Test
  public void testBuilder() {
    final Ledger ledger = mock(Ledger.class);
    final Initiator initiator = mock(Initiator.class);
    final Cohort cohort = mock(Cohort.class);
    final Monitor monitor = mock(Monitor.class);
    
    final Manifold manifold = Manifold.builder()
        .withLedger(ledger)
        .withFactor(initiator)
        .withFactors(cohort, monitor)
        .build();
    
    verify(initiator).init(notNull());
    verify(cohort).init(notNull());
    verify(monitor).init(notNull());
    verify(ledger).init();
    
    assertEquals(ledger, manifold.getLedger());
    assertTrue(manifold.getFactors().contains(initiator));
    assertTrue(manifold.getFactors().contains(cohort));
    assertTrue(manifold.getFactors().contains(monitor));
    
    manifold.dispose();
    
    verify(ledger).dispose();
    verify(initiator).dispose();
    verify(cohort).dispose();
    verify(monitor).dispose();
  }
}
