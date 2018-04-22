package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

import nl.jqno.equalsverifier.*;

public final class ProposalTest {
  @Test
  public void testFields() {
    final String[] cohorts = new String[] {"a", "b"};
    final Proposal p = new Proposal("B1", cohorts, "objective", 1000);
    assertArrayEquals(cohorts, p.getCohorts());
    assertEquals("objective", p.getObjective());
    assertEquals(1000, p.getTtl());
    
    Assertions.assertToStringOverride(p);
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(Proposal.class).suppress(Warning.NONFINAL_FIELDS).verify();
  }
}
