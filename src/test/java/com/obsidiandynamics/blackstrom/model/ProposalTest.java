package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class ProposalTest {
  @Test
  public void testFields() {
    final String[] cohorts = new String[] {"a", "b"};
    final Proposal p = new Proposal(1, cohorts, "objective", 1000);
    assertArrayEquals(cohorts, p.getCohorts());
    assertEquals("objective", p.getObjective());
    assertEquals(1000, p.getTtl());
    
    Assertions.assertToStringOverride(p);
  }
  
  @Test
  public void testEqualsHashCode() {
    final Proposal p1 = new Proposal(1, 1000, new String[] {"a", "b"}, "objective", 1000);
    final Proposal p2 = new Proposal(1, 1000, new String[] {"a", "b"}, "something else", 1000);
    final Proposal p3 = new Proposal(1, 1000, new String[] {"a", "b"}, "objective", 1000);
    final Proposal p4 = p1;

    assertNotEquals(p1, p2);
    assertEquals(p1, p3);
    assertEquals(p1, p4);
    assertNotEquals(p1, new Object());

    assertNotEquals(p1.hashCode(), p2.hashCode());
    assertEquals(p1.hashCode(), p3.hashCode());
  }
}
