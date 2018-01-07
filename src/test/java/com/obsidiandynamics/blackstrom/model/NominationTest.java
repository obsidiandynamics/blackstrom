package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class NominationTest {
  @Test
  public void testFields() {
    final String[] cohorts = new String[] {"a", "b"};
    final Nomination n = new Nomination(1, cohorts, "proposal", 1000);
    assertArrayEquals(cohorts, n.getCohorts());
    assertEquals("proposal", n.getProposal());
    assertEquals(1000, n.getTtl());
    
    Assertions.assertToStringOverride(n);
  }
  
  @Test
  public void testEqualsHashCode() {
    final Nomination n1 = new Nomination(1, 1000, new String[] {"a", "b"}, "proposal", 1000);
    final Nomination n2 = new Nomination(2, 1000, new String[] {"a", "b"}, "proposal", 1000);
    final Nomination n3 = new Nomination(1, 1000, new String[] {"a", "b"}, "proposal", 1000);
    final Nomination n4 = n1;

    assertNotEquals(n1, n2);
    assertEquals(n1, n3);
    assertEquals(n1, n4);
    assertNotEquals(n1, new Object());

    assertNotEquals(n1.hashCode(), n2.hashCode());
    assertEquals(n1.hashCode(), n3.hashCode());
  }
}
