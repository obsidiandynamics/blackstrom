package com.obsidiandynamics.blackstrom.monitor;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class OutcomeMetadataTest {
  @Test
  public void testFields() {
    final OutcomeMetadata meta = new OutcomeMetadata(100);
    assertEquals(100L, meta.getProposalTimestamp());
  }
  
  @Test
  public void testEqualsHashCode() {
    final OutcomeMetadata m1 = new OutcomeMetadata(100);
    final OutcomeMetadata m2 = new OutcomeMetadata(200);
    final OutcomeMetadata m3 = new OutcomeMetadata(100);
    final OutcomeMetadata m4 = m1;

    assertNotEquals(m1, m2);
    assertEquals(m1, m3);
    assertEquals(m1, m4);
    assertNotEquals(m1, new Object());

    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertEquals(m1.hashCode(), m3.hashCode());
  }
  
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new OutcomeMetadata(100));
  }
}
