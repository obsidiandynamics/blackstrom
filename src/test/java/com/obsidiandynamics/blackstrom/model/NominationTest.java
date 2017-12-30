package com.obsidiandynamics.blackstrom.model;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class NominationTest {
  @Test
  public void test() {
    final String[] cohorts = new String[] {"a", "b"};
    final Nomination n = new Nomination(1, cohorts, "proposal", 1000);
    assertArrayEquals(cohorts, cohorts);
    assertEquals("proposal", n.getProposal());
    assertEquals(1000, n.getTtl());
    
    Assertions.assertToStringOverride(n);
  }
}
