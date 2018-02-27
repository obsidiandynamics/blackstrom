package com.obsidiandynamics.blackstrom.monitor;

import static org.junit.Assert.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class DefaultOutcomeMetadataTest {
  @Test
  public void testFields() {
    final DefaultOutcomeMetadata meta = new DefaultOutcomeMetadata(100);
    assertEquals(100L, meta.getProposalTimestamp());
  }
  
  @Test
  public void testEqualsHashCode() {
    final DefaultOutcomeMetadata m1 = new DefaultOutcomeMetadata(100);
    final DefaultOutcomeMetadata m2 = new DefaultOutcomeMetadata(200);
    final DefaultOutcomeMetadata m3 = new DefaultOutcomeMetadata(100);
    final DefaultOutcomeMetadata m4 = m1;

    assertNotEquals(m1, m2);
    assertEquals(m1, m3);
    assertEquals(m1, m4);
    assertNotEquals(m1, new Object());

    assertNotEquals(m1.hashCode(), m2.hashCode());
    assertEquals(m1.hashCode(), m3.hashCode());
  }
  
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new DefaultOutcomeMetadata(100));
  }
}
